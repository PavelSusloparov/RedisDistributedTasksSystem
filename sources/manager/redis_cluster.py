__all__ = ['ClusterAttributes', 'ClusterConnectionPool']
__revision__ = '$Id'

from functools import wraps
import json
import os
import subprocess
import socket
import time

from ..redis import RawRedis
from ..redis.exceptions import ConnectionError, ResponseError, InvalidResponse
from .log import redis_config, manager_logger, ERROR, WARNING, INFO, DEBUG
from ..utils import cover_except

# Cluster configuration condition.
EMPTY, OLD, NEW = (0, 1, 2)

#Interval for resubscribe on channel.(unit in seconds)
RESUBSCRIBE_INTERVAL = 3

#Interval after redis-server is up.
UP_INTERVAL = 5

#Waiting interval for WatchProcess
# if config is not changed.(unit in seconds)
CONFIG_INTERVAL = 5

#Interval between debug notification for nodes counter section.
COUNTER_INTERVAL = 10

#Limit for redis manager system ready conditional for nodes counter section
#and for checking redis server up status.
INIT_WAIT_LIMIT = 3 * COUNTER_INTERVAL

#Redis server pid and pidfile should be removed in limit.
EXPECTANT_LIMIT = 10


def similarity(list1, list2):
    """
    Function compares 2 lists, contained dictionaries.
    It will return true, if lists are equal.
    """
    for elem_number in range(len(list1)):
        if list1[elem_number] != list2[elem_number]:
            return False
    return True

class ConfigError(Exception):
    pass

class RedisManagerError(Exception):
    pass

class RedisServerError(Exception):
    pass

class ClusterAttributes(object):
    """
    Class works with redis.conf.
    It gets all initialization parametrs for redis cluster.
    It collects parameters for redis-server starting.
    """

    def __init__(self, node_attr=None, db='default', collection='default'):
        self.name = 'Cluster Attr'
        self.collection = collection
        self._set_db(db)
        self._set_repl()
        self._set_node_attr(node_attr)
        self._set_node_attributes()
        self._set_node_str()

    def _set_db(self, db):
        if isinstance(db, int):
            self.db = db
        else:
            db_section = redis_config['db']
            self.db = db_section.get(db, db_section['default'])
        DEBUG("{}: db = {}".format(self.name, self.db))

    def _node_update(self, node_attr):
        """
        If value is not configured by user,
        value from default_node section will be used.
        """
        def_node = redis_config['default_node']
        new_node_attr = {'db': self.db}
        new_node_attr['host'] = node_attr.get('host', def_node['host'])
        new_node_attr['port'] = node_attr.get('port', def_node['port'])
        return new_node_attr

    def _set_repl(self):
        def update_repl(repl):
            col_repl = []
            for cluster_idx, repl_cluster in enumerate(repl):
                cluster_repl = []
                for node_attr in repl_cluster:
                    cluster_repl.append(self._node_update(node_attr))
                col_repl.append(cluster_repl)
            return col_repl

        self.repl = {}
        self.shard_list = []
        sh_coll = redis_config['sharded_collections']
        DEBUG("Sharded_collections: {}".format(sh_coll))
        for col, v in sh_coll.items():
            DEBUG("Current collection: {}".format(col))
            repl_col = sh_coll.get(col, sh_coll['default'])
            self.repl.setdefault(col, update_repl(repl_col))
            for db_attr in v:
                if db_attr not in self.shard_list:
                    self.shard_list.append(db_attr)
        DEBUG("{}: Current replication: {}".format(self.name, self.repl))

    def _set_node_attr(self, node_attr):
        DEBUG("Node attr: {}".format(node_attr))
        if not node_attr:
            node_attr = self.repl[self.collection][0][0]
        self.node_attr = self._node_update(node_attr)
        DEBUG("{}: Node attr: {}".format(self.name, self.node_attr))

    def _set_node_attributes(self):
        """
        Cluster attributes:
        1) Self redis index in cluster.
        2) Cluster connection attributes.
        3) Cluster connection pool.
        """
        for cur_col, cur_repl in self.repl.items():
            for cluster_idx, repl_cluster in enumerate(cur_repl):
                for pos_in_cluster, conn_param in enumerate(repl_cluster):
                    if self.node_attr == conn_param:
                        self.cluster_idx = cluster_idx
                        self.node_cluster_idx = pos_in_cluster
                        self.attributes = repl_cluster
                        self.master_attr = self.attributes[0]
                        self.nodes_quantity = len(repl_cluster)
                        DEBUG("{}: Cluster idx: {}, Node cluster idx: {}, attr: {}"
                            .format(self.name, self.cluster_idx,
                                self.node_cluster_idx, self.attributes))
                        DEBUG("{}: Master attr: {}".format(self.name,
                            self.master_attr))
                        return
        raise ConfigError("Can not find {}; in current configuration: {}."
            "Please check redis.conf.".format(self.node_attr, self.repl))

    def _set_node_str(self):
        self.repl_set_name = "rs_{}".format(self.cluster_idx + 1)
        node_str = "{}_{}_{}".format(self.node_attr['host'],
            self.node_attr['port'], self.node_attr['db'])
        self.node_repr = "{}_{}".format(self.repl_set_name, node_str)
        DEBUG("{}: Replica name: {}, node repr: {}".format(self.name,
            self.repl_set_name, self.node_repr))


class FailureAwareConnection(RawRedis):
    """
    Class is wrapper for original py-redis version 2.4.13.
    It is for ClusterConnectionPool usage purposes.
    """
    RESPONSE_CALLBACKS = {}
    name = "Failure aware connection"
    commands = ['keys', 'flushdb', 'rpop', 'lpush', 'lindex', 'publish',
               'sadd', 'smembers', 'sismember', 'llen', 'lrange', 'ltrim',
               'brpop', 'pubsub', 'slaveof', 'ping', 'pipeline']
    empty_commands = ['pubsub', 'slaveof', 'ping']

    def __init__(self, node_attr, db='redis_manager', **kwargs):
        self.node_attr = dict(node_attr)
        self.channel = "connection_error"
        self.ca = ClusterAttributes(node_attr=self.node_attr, db=db)
        host = self.ca.node_attr['host']
        port = self.ca.node_attr['port']
        db = self.ca.node_attr['db']
        super(FailureAwareConnection, self).__init__(host=host, port=port,
            db=db, **kwargs)
        self.attr = self.ca.attributes
        self.node_cluster_idx = self.ca.node_cluster_idx
        self.connections = self._get_connections()
        self.current_connection = self.connections[self.node_cluster_idx]

    @cover_except(manager_logger)
    def __getattribute__(self, attr):
        if attr not in FailureAwareConnection.commands:
            return super(FailureAwareConnection, self).__getattribute__(attr)
        @wraps(self.current_connection.__getattribute__(attr))
        def wrap(*args, **kwargs):
            DEBUG("{}::Inside __getattribute__. Attr: {}"
               .format(FailureAwareConnection.name, attr))
            try:
                f = self.current_connection.__getattribute__(attr)
                result = f(*args, **kwargs)
                DEBUG("{}::{}, Result::{}"
                    .format(FailureAwareConnection.name, attr, result))
            except InvalidResponse as detail:
                ERROR("{}::Invalid responce: {}"
                    .format(FailureAwareConnection.name, detail))
                raise
            except ResponseError as detail:
                ERROR("{}::Responce Error: {}"
                    .format(FailureAwareConnection.name, detail))
                raise
            except ConnectionError:
                if attr not in FailureAwareConnection.empty_commands:
                    self._connection_error_publish()
                result = None
            return result
        return wrap

    def _get_connections(self):
        cluster_conn = []
        for attributes in self.attr:
            db = RawRedis(host=attributes['host'], port=attributes['port'],
                db=attributes['db'])
            cluster_conn.append(db)
        return cluster_conn

    def _publish(self, connection, channel, message):
        try:
            connection.publish(channel, message)
        except ConnectionError:
            pass

    def _connection_error_publish(self):
        """
        Publish, that redis-server is down,
        to connection_error channel, which listens ha_manager.
        """
        for connection in self.connections:
            self._publish(connection, self.channel,
                json.dumps(self.node_attr))

    def handled_ping(self):
        try:
            self.current_connection.ping()
        except ConnectionError:
            self._connection_error_publish()


class ClusterConnectionPool(object):
    """
    Class includes all information about FailureAwareConnection pool,
    methods and metadata for RedisManager,
    methods and metadata for Redis wrapper.
    Class know all information about one replica set.
        Functionality:
        1) It has information about replica set configuration in run time.
        2) It cans set ang get node counter in run time.
    """

    def __init__(self, repl_set_name, attributes,
                 node_cluster_idx, db='default'):
        self.name = "Cluster connection pool"
        self.repl_set_name = repl_set_name
        self.attr = [{'host': x['host'], 'port': x['port']}
            for x in attributes]
        node_attr = attributes[node_cluster_idx]
        ca = ClusterAttributes(node_attr=node_attr, db=db)
        self.host_port = ca.node_attr
        del(self.host_port['db'])
        self.db = ca.db
        self.nodes_quantity = ca.nodes_quantity
        DEBUG("{}:: Attributes: {}, node attributes: {}".format(self.name,
            self.attr, self.host_port))
        self.cluster_fa_conn = self._get_FailureAwareConnection_pool()
        self._set_names()
        self._set_current_node_parameters()
        self.master_index = 0
        self._set_fa_master_conn()
        self._set_master_conn()
        if len(self.attr) != 1:
            self.single_node_mode = 0
            self.main_slave_index = 1
            self._set_main_slave_parameters()
        else:
            self.single_node_mode = 1

    def _get_FailureAwareConnection_pool(self):
        """
        Get connection pool for WatchProcess.
        """
        cluster_conn = []
        for host_port in self.attr:
            fac = FailureAwareConnection(host_port)
            cluster_conn.append(fac)
        return cluster_conn

    def _set_names(self):
        self.process_name = "redis-server"
        self.nodes_counter_name = ("redis_manager:{}_nodes_counter"
            .format(self.repl_set_name))
        self.node_counter_name_lock = ("{}_lock"
            .format(self.nodes_counter_name))
        self.marker_name = ("redis_manager:reconfiguration_marker_{}"
            .format(self.repl_set_name))
        self.cluster_config_name = ("redis_manager:cluster_config_{}"
            .format(self.repl_set_name))

    def _set_current_node_parameters(self):
        self.node_cluster_idx = self._get_index(self.host_port)
        self.node_conn = self._get_fa_connection(self.host_port)
        self.node_attr_name = self.get_string_attr(self.host_port)
        self.node_name = ("{}_{}".format(self.repl_set_name,
            self.node_attr_name))

    def _set_main_slave_parameters(self):
        self.main_slave_attr = self._get_attributes(self.main_slave_index)
        self.main_slave_name = self.get_string_attr(self.main_slave_attr)
        self.main_slave_db = self._get_fa_connection(self.main_slave_attr)

    def _set_fa_master_conn(self):
        self.master_fa_attr = self._get_attributes(self.master_index)
        self.master_fa_attr_name = self.get_string_attr(self.master_fa_attr)
        self.master_fa_db = self._get_fa_connection(self.master_fa_attr)

    def _set_master_conn(self):
        self.master_attr = self._get_attributes(self.master_index)
        self.master_attr_name = self.get_string_attr(self.master_attr)
        self.master_db = RawRedis(host=self.master_attr['host'],
            port=self.master_attr['port'], db=self.db)

    def _get_fa_connection(self, attr):
        index = self._get_index(attr)
        return self.cluster_fa_conn[index]

    def _get_index(self, attr):
        return self.attr.index(attr)

    def _get_attributes(self, index):
        return self.attr[index]

    def _update_attr(self, down_node_index):
        down_node_attr = self.attr.pop(down_node_index)
        self.attr.append(down_node_attr)
        self.update_class_param()

    def _get_down_node_index(self, down_node_attr):
        if self.update_cluster_config() != EMPTY:
            return self._get_index(down_node_attr)

    def update_self_cluster_info(self):
        while True:
            if self.update_cluster_config() == NEW:
                break
            else:
                time.sleep(CONFIG_INTERVAL)

    def set_down_node(self, host, port):
        self.down_attr = {'host':host, 'port':port}
        self.down_attr_name = self.get_string_attr(self.down_attr)
        self.down_node_index = self._get_down_node_index(self.down_attr)

    def get_string_attr(self, attr):
        return "{}_{}".format(attr['host'], attr['port'])

    def update_class_param(self):
        self.cluster_fa_conn = self._get_FailureAwareConnection_pool()
        self._set_fa_master_conn()
        self._set_master_conn()
        self._set_main_slave_parameters()
        self._set_current_node_parameters()

    def set_reconfiguration_marker(self):
        """
        Reconfiguration marker is needed by Listen Process.
        When redis node is down,
        Listen Process gets reconfiguration marker and does reconfiguration.
        Only one Listen Process can get reconfiguration marker in one time.
        """
        self.cluster_fa_conn[self.master_index].lpush(self.marker_name, 1)

    def get_reconfiguration_marker(self):
        """
        When redis node is down,
        Listen Process gets reconfiguration marker and does reconfiguration.
        Only one Listen Process can get reconfiguration marker in one time.
        """
        raw_value = (self.cluster_fa_conn[self.main_slave_index]
            .rpop(self.marker_name))
        return int(raw_value) if raw_value else 0

    #=================Set replication========================
    def _dependence(self, master_attr, slave_attr=None):
        """
        Replica set dependence.
        2 modes:
        1) Set master mode. Do not set slave_attr.
        2) Set slave mode.
        """
        if slave_attr:
            slave_idx = self._get_index(slave_attr)
            self.cluster_fa_conn[slave_idx].slaveof(master_attr['host'],
                master_attr['port'])
            INFO("Set dependence. Master: {}, Slave: {}"
                .format(self.get_string_attr(master_attr),
                self.get_string_attr(slave_attr)))
        else:
            master_idx = self._get_index(master_attr)
            self.cluster_fa_conn[master_idx].slaveof()
            INFO("Set master node: {}"
                .format(self.get_string_attr(master_attr)))

    def set_master(self, host, port):
        node_attr = {'host':host, 'port':port}
        node_idx = self._get_index(node_attr)
        self.cluster_fa_conn[node_idx].slaveof()
        INFO("Set master node: {}"
            .format(self.get_string_attr(node_attr)))

    def set_dependence(self, host, port, slave=False):
        node_attr = {'host':host, 'port':port}
        INFO("Dependence:: Current attributes: {}".format(self.attr))
        if slave:
            self.update_cluster_config(other_node=True)
        node_idx = self._get_index(node_attr)
        INFO("Dependence:: Node index in cluster: {}".format(node_idx))
        if node_idx == self.master_index:
            self._dependence(node_attr)
        else:
            main_node_attr = self.attr[node_idx-1]
            self._dependence(main_node_attr, node_attr)

    #=========================================================
    #=================Cluster configuration===================
    def set_cluster_config(self):
        """
        Show cluster condition in current time.
        Cluster config is distributed on all redis in cluster,
        through redis replication.
        """
        INFO("Set {} cluster configuration.".format(self.attr))
        self.node_conn.rpop(self.cluster_config_name)
        try:
            write_attr = json.dumps(self.attr)
            self.node_conn.lpush(self.cluster_config_name, write_attr)
        except TypeError:
            raise TypeError("Can not dump cluster attributes.")

    def get_cluster_config(self, other_node=False):
        """
        Get cluster config in run time.
        """
        conn = self.main_slave_db if other_node else self.node_conn
        raw_cluster_config = conn.lindex(self.cluster_config_name, 0)
        DEBUG("{}:Raw cluster configuration: {}"
            .format(self.name, raw_cluster_config))
        if raw_cluster_config is not None:
            try:
                cluster_config = json.loads(raw_cluster_config)
                return cluster_config
            except TypeError:
                return EMPTY
        else:
            return EMPTY

    def update_cluster_config(self, other_node=False):
        """
        Cluster configuration from self redis.
        Reconfigure WatchProcess attributes.
        """
        cluster_config = self.get_cluster_config(other_node=other_node)
        DEBUG("{}:: Cluster configuration: {}".format(self.name, cluster_config))
        if cluster_config:
            if not similarity(self.attr, cluster_config):
                self.node_cluster_idx = cluster_config.index(self.host_port)
                self.attr = cluster_config
                self.update_class_param()
                return NEW
            return OLD
        else:
            if self.single_node_mode:
                try:
                    if not self.master_db.ping():
                        self._set_master_conn()
                except ConnectionError:
                    self._set_master_conn()
            return EMPTY

    def set_new_cluster_configuration(self, down_node_index):
        """
        Method configures WatchProcess attributes.
        """
        self._update_attr(down_node_index)
        self.master_fa_db.rpop(self.cluster_config_name)
        self.master_fa_db.lpush(self.cluster_config_name,
            json.dumps(self.attr))
    #=========================================================
    #==============Run time node condition check==============
    def is_current_a_master(self):
        if self.node_cluster_idx == self.master_index:
            return True
        else:
            return False

    def is_down_a_master(self):
        return self.down_node_index == self.master_index

    def is_down_a_current(self):
        return self.down_node_index == self.node_cluster_idx
    #=========================================================


class NodeCounter(object):
    """
    Node counter is used for redis_manager initialization.
    """
    def __init__(self, node_attr):
        self.nodes_counter_name = "node_counter"
        node_attr = node_attr
        ca = ClusterAttributes(node_attr=node_attr, db='redis_manager')
        self.nodes_quantity = ca.nodes_quantity
        self.master_attr = ca.master_attr
        self.name = ca.node_repr
        self.conn = FailureAwareConnection(self.master_attr)

    def refresh_connection(self):
        self.conn = FailureAwareConnection(self.master_attr)

    def get_node_counter(self):
        raw_value = (self.conn
            .lrange(self.nodes_counter_name, 0, 1))
        DEBUG("Raw value(Ready nodes): {}".format(raw_value))
        return int(raw_value[0]) if raw_value else 0

    def clean_node_counter(self):
        self.conn.ltrim(self.nodes_counter_name, 1, 0)

    def set_node_counter(self):
        counter = self.get_node_counter() + 1
        DEBUG("Incremented counter from db: {}".format(counter))
        pipe = self.conn.pipeline()
        (pipe.rpop(self.nodes_counter_name)
            .lpush(self.nodes_counter_name, counter).execute())
        return counter

    def wait_cluster_setting(self, debug=False):
        """
        Method is used for checking cluster condition in run time.
        """
        counter = 0
        while True:
            ready_nodes = self.get_node_counter()
            if not counter % COUNTER_INTERVAL:
                if debug:
                    INFO("{}:: Ready nodes: {}. Real quantity: {}"
                        .format(self.name, ready_nodes, self.nodes_quantity))
                    INFO("{}:: Cluster is not ready.".format(self.name))
                if counter == INIT_WAIT_LIMIT:
                    raise RedisManagerError("Can not set up cluster.")
            if ready_nodes != self.nodes_quantity:
                time.sleep(1)
                counter += 1
            else:
                break

class RedisServer(object):
    """
    Class includes os operation with redis-server.
    """
    def __init__(self, host, port, params=None):
        self.host = host
        self.port = port
        self.node_attr = {'host':self.host, 'port':self.port}
        self.node_name = self._get_string_attr()
        self.name = "Redis server {}".format(self.node_name)
        self.daemonize = "yes"
        self.dbfilename = "{}.rdb".format(self.node_name)
        if not params:
            self.params = {}
        else:
            self.params = params
        self.rs_attr = redis_config['redis_server']
        self.conn = FailureAwareConnection(self.node_attr)
        self._set_start_attr(params)

    def _get_string_attr(self):
        return "{}_{}".format(self.host, self.port)

    def _set_params(self):
        self.params['dbfilename'] = "{}.rdb".format(self.node_name)
        self.params['pidfile'] = ("{}/{}.pid"
            .format(self.rs_attr['pidfile_dir'], self.node_name))
        self.params['logfile'] = ("{}/{}.log".format(self.rs_attr['log_dir'],
            self.node_name))
        self.params['loglevel'] = self.rs_attr['loglevel']
        self.params['repl_ping_slave_period'] = self.rs_attr['repl_ping_slave_period']
        self.params['repl_timeout'] = self.rs_attr['repl_timeout']
        self.params['appendfsync'] = self.rs_attr['appendfsync']
        self.params['dir'] = self.rs_attr['dump_dir']

    def _add_param(self, param, values):
        if self.first_param:
            line_break = ''
            self.first_param = False
        else:
            line_break = '\n'
        if isinstance(values, list):
            self.redis_server_attr = ("{}{}{}".format(self.redis_server_attr,
                line_break, param))
            for value in values:
                self.redis_server_attr = ("{} {}"
                    .format(self.redis_server_attr, value))
        elif values:
            self.redis_server_attr = ("{}{}{} {}"
                .format(self.redis_server_attr, line_break,
                param, values))
        else:
            self.redis_server_attr = "{}\"".format(self.redis_server_attr)
        DEBUG("{}:: Param: {}, value: {}".format(self.name, param, values))
        DEBUG("{}:: Server attributes: {}"
            .format(self.name, self.redis_server_attr))

    def _set_start_attr(self, params):
        self._set_params()
        self.redis_server_attr = "echo -e \""
        self.first_param = True

        self._add_param('daemonize', self.daemonize)
        self._add_param('pidfile', self.params['pidfile'])
        self._add_param('dir', self.params['dir'])
        self._add_param('dbfilename', self.params['dbfilename'])
        self._add_param('logfile', self.params['logfile'])
        self._add_param('loglevel', self.params['loglevel'])
        self._add_param('appendfsync', self.params['appendfsync'])
        self._add_param('port', self.port)
        self._add_param('repl-ping-slave-period',
            self.params['repl_ping_slave_period'])
        self._add_param('repl-timeout', self.params['repl_timeout'])
        self._add_param(None, None)

    def _is_file_exist(self, location):
        try:
            os.stat(location)
            DEBUG("{}::Directory {} is exist.".format(self.name, location))
            return True
        except OSError:
            DEBUG("{}::Directory {} is not exist.".format(self.name, location))
            return False

    def _delete_old_info(self):
        expectant = 0
        #Check pid file
        if not self._is_file_exist(self.params['pidfile']):
            return
        try:
            stream = open(self.params['pidfile'], 'r')
            pid = stream.readline().replace('\n', '')
            DEBUG("{}::PID IS {}".format(self.name, pid))
            pid_loc = os.path.join('/proc', pid)
        except IOError:
            DEBUG("Pid file was removed.")
            return
        #Check pid
        if not self._is_file_exist(pid_loc):
            return
        #wait, while pid file will be terminated
        while True:
            if not self._is_file_exist(pid_loc):
                break
            INFO("{}:: Pid {} is not terminated yet. "
                "Wait 1 sec.".format(self.name, pid))
            expectant += 1
            if expectant == EXPECTANT_LIMIT:
                raise RedisServerError("Redis server pid {} is not removed."
                    .format(pid_loc))
            time.sleep(1)
        #delete pid file
        try:
            command = "rm  {}".format(self.params['pidfile'])
            subprocess.check_output(command, shell=True)
        except subprocess.CalledProcessError:
            pass
        DEBUG("{}::Pid file {} was removed.".format(self.name,
            self.params['pidfile']))

    def up(self, params=None):
        """
        Method starts redis server.
        """
        self._delete_old_info()
        INFO("{}:: Start redis.".format(self.name))
        command = ("{} | {} -".format(self.redis_server_attr,
            self.rs_attr['bin_path']))
        INFO("{}:: Redis start command:\n{}".format(self.name, command))
        try:
            exit_status = subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError:
            raise RedisServerError("Can not start {} redis-server. "
                "Check /nb/redis/redis_log/{}.log".format(self.node_name,
                self.node_name))
        time.sleep(UP_INTERVAL)
        #Check pid file
        if self._is_file_exist(self.params['pidfile']):
            stream = open(self.params['pidfile'], 'r')
            pid = stream.readline().replace('\n', '')
            pid_loc = os.path.join('/proc', pid)
            if self._is_file_exist(pid_loc):
                INFO("{} is ready for use. Pid: {}"
                    .format(self.name, pid))
                return True
        raise RedisServerError("Can not create pid file for {} "
                "redis-server.".format(self.node_name))

    def clean_dump(self):
        """
        Method clean redis server dump.
        """
        if self.rs_attr['clean_dump'] and self.rs_attr['clean_dump'] != '-':
            dump_path = ("{}/{}".format(self.rs_attr['dump_dir'],
                self.params['dbfilename']))
            command = "rm -f {}".format(dump_path)
            INFO("{}:: Clean dump {}.".format(self.name, dump_path))
            try:
                subprocess.check_call(command, shell=True)
            except subprocess.CalledProcessError:
                raise RedisServerError("{}:: Can not clean {}."
                    .format(self.name, dump_path))
        else:
            INFO("{}:: Dump cleaning is disabled.".format(self.name))
