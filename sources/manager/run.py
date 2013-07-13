__all__ = ['Managers']
__revision__ = '$Id'

from datetime import datetime
import json
import multiprocessing
import os
import socket
import sys
import time
from Queue import Empty

from ..manager.redis_cluster import (RESUBSCRIBE_INTERVAL, INIT_WAIT_LIMIT,
    EMPTY, ClusterAttributes, ClusterConnectionPool, RedisServer, NodeCounter)
from ..redis.exceptions import ConnectionError
from ..utils import ChildKiller, kill_processes, cover_except
from .log import redis_config, manager_logger, MONDB, WARNING, INFO, DEBUG
from ..mondb.events import RedisManagerEvent


class RedisManager(multiprocessing.Process):
    """
    Redis Manager tracks on cluster connection,
    automatically restores down connection and reconfigure cluster.
    Each manager was started for each redis, for tracking changes in cluster.
    Redis Manager is used for one replica set.
    """

    def __init__(self, node_attr, done_q):
        multiprocessing.Process.__init__(self)
        self.node_attr = node_attr
        self.done_q = done_q
        ca = ClusterAttributes(node_attr=self.node_attr, db='redis_manager')
        self.repl_set_name = ca.repl_set_name
        self.cluster_attr = ca.attributes
        self.node_cluster_idx = ca.node_cluster_idx
        self.nodes_quantity = ca.nodes_quantity
        self.node_attr = ca.node_attr
        self.ccp = ClusterConnectionPool(self.repl_set_name,
            self.cluster_attr, self.node_cluster_idx, db='redis_manager')
        self.name = "{}_RedisManager".format(self.ccp.node_name)
        DEBUG("{}:: Attributes: {}".format(self.name, self.ccp.attr))
        self.set_sequence_time = self.node_cluster_idx
        self.nc = NodeCounter(self.node_attr)

    @cover_except(manager_logger)
    def run(self):
        """
        Sleep self.set_sequence_time time for
        initialization in right order, follow redis.conf.
        Class creates WatchProcess and ListenProcess.
        Class created MonDb event, if tesla is available.
        """
        ChildKiller(self.name, manager_logger).start()
        rm_event = RedisManagerEvent() if MONDB else None
        INFO("{} starting with pid: {}".format(self.name, self.pid))
        INFO("{}:: Current index in cluster: {}".format(self.name,
            self.node_cluster_idx))
        time.sleep(self.set_sequence_time)
        RedisServer(self.node_attr['host'], self.node_attr['port']).up()
        if self.ccp.single_node_mode:
            WARNING("{}:: Replication is disabled.".format(self.name))
            INFO("{}:: Start master watch process.".format(self.name))
            self.nc.clean_node_counter()
            self.nc.set_node_counter()
        else:
            self.ccp.set_dependence(self.node_attr['host'],
                self.node_attr['port'])
            INFO("{}:: Initialization wait. Sleep {} sec.".format(self.name,
                self.set_sequence_time))
            #  Master node in replica set is first node.
            #  Chain is configured by waiting self.set_sequence_time
            #  (unit in seconds)
            time.sleep(self.set_sequence_time)
            if not self.node_cluster_idx:
                self.ccp.set_reconfiguration_marker()
                self.nc.clean_node_counter()
            self.nc.set_node_counter()
            INFO("{}:: Node counter: {}".format(self.name, self.nc.get_node_counter()))
            INFO("{}:: Waiting cluster initialization.".format(self.name))
            self.nc.wait_cluster_setting(debug=True)
            INFO("{}:: Cluster is ready.".format(self.name))
            self.ccp.set_cluster_config()
            cc = self.ccp.get_cluster_config()
            if not cc:
                time.sleep(1)
                cc = self.ccp.get_cluster_config()
            INFO("{}:: Baseline cluster configuration: {}".format(self.name, cc))
            INFO("{}:: Start watch and listen processes.".format(self.name))
            lp = ListenProcess(self.repl_set_name, self.cluster_attr,
                self.node_cluster_idx, rm_event=rm_event)
            lp.daemon = True
            lp.start()
        wp = WatchProcess(self.repl_set_name, self.cluster_attr,
        self.node_cluster_idx, rm_event=rm_event)
        wp.daemon = True
        wp.start()

        self.done_q.put(self.node_attr)
        wp.join()
        if not self.ccp.single_node_mode:
            lp.join()


class WatchProcess(multiprocessing.Process):
    """
    - Pings redis master node
    - Pings redis node on local box
    """

    def __init__(self, repl_set_name, cluster_attr,
                 node_cluster_idx, rm_event=None):
        multiprocessing.Process.__init__(self)
        self.ccp = ClusterConnectionPool(repl_set_name,
            cluster_attr, node_cluster_idx, db='redis_manager')
        self.host = self.ccp.host_port['host']
        self.port = self.ccp.host_port['port']
        self.node_cluster_idx = self.ccp.node_cluster_idx
        self.name = "{}_wp".format(self.ccp.node_name)
        self.start_time = datetime.now().second
        self.between_ping_time = redis_config['between_pings_time']
        self.is_alive_time = redis_config['cluster_state_time']
        self.nc = NodeCounter(self.ccp.host_port)
        self.rm_event = rm_event

    def _write_to_mondb(self, process_type, event_type):
        if self.rm_event:
            cluster_config = self.ccp.get_cluster_config()
            self.rm_event.replication_chain = cluster_config
            self.rm_event.process_affected = int(self.pid)
            self.rm_event.event_type = event_type
            self.rm_event.process_type = process_type
            self.rm_event.record()

    @cover_except(manager_logger)
    def run(self):
        INFO("{} starting with pid: {}".format(self.name, self.pid))
        while True:
            if self.ccp.single_node_mode:
                self.one_node_ping()
            else:
                if (not (self.start_time - datetime.now().second) %
                    self.is_alive_time):
                    cluster_config = self.ccp.get_cluster_config()
                    INFO("{}:: Current state: {}".format(self.name,
                        cluster_config))
                if self.ccp.update_cluster_config() == EMPTY:
                    self.ccp.update_cluster_config(other_node=True)
                self.master_ping()
                self.watched_redis_ping()
            time.sleep(self.between_ping_time)

    def one_node_ping(self):
        is_alive = self.ccp.node_conn.ping()
        DEBUG("{}:: Self ping: {}".format(self.name, is_alive))
        if is_alive:
            return
        WARNING("{}:: Master {} is down.".format(self.name,
            self.ccp.node_attr_name))
        self._write_to_mondb('master', 'down')
        rs = RedisServer(self.host, self.port)
        rs.clean_dump()
        rs.up()
        self.nc.refresh_connection()
        self.nc.set_node_counter()
        self._write_to_mondb('master', 'up')
        WARNING("{}:: Master {} was restored.".format(self.name,
            self.ccp.node_attr_name))

    def master_ping(self):
        """
        If ping is unsuccessful, message with master attributes
        will be sent to connection_error channel.
        """
        if not self.ccp.is_current_a_master():
            DEBUG("{}:: Ping master {}".format(self.name,
                self.ccp.master_fa_attr))
            self.ccp.master_fa_db.handled_ping()

    def watched_redis_ping(self):
        """
        If self redis is down, WatchProcess performs the following steps:
        1) Reloads redis server.
        2) Writes event to MonDB, if tesla is available.
        3) Reconfigures cluster configuration.
        """
        is_master = self.ccp.is_current_a_master()
        DEBUG("{}:: Is master: {}".format(self.name, is_master))
        if is_master:
            return
        is_alive = self.ccp.node_conn.ping()
        DEBUG("{}:: Self ping: {}".format(self.name, is_alive))
        if is_alive:
            return
        WARNING("{}:: Slave {} is down.".format(self.name,
            self.ccp.node_attr_name))
        self._write_to_mondb('slave', 'down')
        self.ccp.set_new_cluster_configuration(self.node_cluster_idx)
        rs = RedisServer(self.host, self.port)
        rs.clean_dump()
        rs.up()
        self.ccp.set_dependence(self.host, self.port, slave=True)
        self._write_to_mondb('slave', 'up')
        WARNING("{}:: Slave {} was restored.".format(self.name,
            self.ccp.node_attr_name))
        WARNING("{}:: New cluster attributes: {}".format(self.name,
            self.ccp.attr))


class ListenProcess(multiprocessing.Process):
    """
    Class functionality:
    1) Subscribe and listen error channel.
    2) If master redis in replication chain was down,
       ListenProcess performs the following steps:
        1) Reloads redis server.
        2) Writes event to MonDB, if tesla is available.
        3) Reconfigures cluster configuration.
    """

    def __init__(self, repl_set_name, cluster_attr,
                 node_cluster_idx, rm_event=None):
        multiprocessing.Process.__init__(self)
        self.ccp = ClusterConnectionPool(repl_set_name,
            cluster_attr, node_cluster_idx, db='redis_manager')
        self.host = self.ccp.host_port['host']
        self.port = self.ccp.host_port['port']
        self.name = "{}_lp".format(self.ccp.node_name)
        self.error_channel = "connection_error"
        self.start_time = datetime.now()
        self.rm_event = rm_event

    def _write_to_mondb(self, event_type):
        if self.rm_event:
            cluster_config = self.ccp.get_cluster_config()
            self.rm_event.replication_chain = cluster_config
            self.rm_event.process_affected = int(self.pid)
            self.rm_event.event_type = event_type
            self.rm_event.process_type = 'master'
            self.rm_event.record()

    @cover_except(manager_logger)
    def run(self):
        INFO("{} starting with pid: {}".format(self.name, self.pid))
        while True:
            is_alive = self.ccp.node_conn.ping()
            if not is_alive:
                WARNING("{}:: Connection error".format(self.name))
                time.sleep(RESUBSCRIBE_INTERVAL)
                self.ccp.update_class_param()
                continue
            subscription = self.ccp.node_conn.pubsub()
            subscription.subscribe(self.error_channel)
            INFO("{}:: Subscribed on {} channel.".format(self.name,
                self.error_channel))
            try:
                for message in subscription.listen():
                    DEBUG("{}:: Message: {}".format(self.name, message))
                    subscription.unsubscribe(self.error_channel)
                    if message['channel'] == self.error_channel:
                        try:
                            down_attr = json.loads(message['data'])
                            self.ccp.set_down_node(down_attr['host'],
                                down_attr['port'])
                        except TypeError:
                            continue
                        self.down_node_handle()
                    subscription.subscribe(self.error_channel)
            except ConnectionError:
                continue

    def down_node_handle(self):
        if self.ccp.is_down_a_master() and not self.ccp.is_down_a_current():
            WARNING("{}:: Master {} in cluster is down.".format(self.name,
                self.ccp.down_attr_name))
            WARNING("{}:: Try to get marker from {}.".format(self.name,
                self.ccp.main_slave_name))
            new_master_attr = self.ccp.main_slave_attr
            self.ccp.set_master(new_master_attr['host'], new_master_attr['port'])
            INFO("{}:: New master attr: {}".format(self.name, new_master_attr))
            if self.ccp.get_reconfiguration_marker():
                WARNING("{}:: {} got marker.".format(self.name,
                    self.ccp.node_attr_name))
                self._write_to_mondb('down')
                self.cluster_reconfiguration()
                self._write_to_mondb('up')
                WARNING("{}:: Node {} is restored.".format(self.name,
                    self.ccp.down_attr_name))
                WARNING("{}:: New cluster attributes: {}".format(self.name,
                    self.ccp.attr))
            else:
                INFO("{}::{} was not restored.".format(self.name,
                    self.ccp.node_attr_name))
                self.ccp.update_self_cluster_info()
        else:
            DEBUG("{}::Slave node {} is down. Wp will handle it."
                .format(self.name, self.ccp.down_attr_name))

    def cluster_reconfiguration(self):
        down_node_idx = self.ccp.down_node_index
        self.ccp.set_new_cluster_configuration(down_node_idx)
        host = self.ccp.down_attr['host']
        port = self.ccp.down_attr['port']
        rs = RedisServer(host, port)
        rs.clean_dump()
        rs.up()
        self.ccp.set_dependence(host, port)
        self.ccp.set_reconfiguration_marker()


class Managers(object):

    def __init__(self):
        self.shard_list = ClusterAttributes().shard_list
        self.name = "Managers process"
        self.pid = os.getpid()
        self.done_q = multiprocessing.Queue()
        self.srvs = []

    @cover_except(manager_logger)
    def start(self):
        INFO("{} starting with pid: {}".format(self.name, self.pid))
        kill_processes(self.name, manager_logger)
        ck = ChildKiller(self.name, manager_logger, redis=True)
        ck.daemon = True
        ck.start()
        loc_addr = {addr[4][0] for addr in
                    socket.getaddrinfo('localhost', 80) +
                    socket.getaddrinfo(socket.getfqdn(), 80)}
        for single_repl in self.shard_list:
            for db_attr in single_repl:
                cfg_addr = {addr[4][0] for addr in
                    socket.getaddrinfo(db_attr['host'], 80)}
                if loc_addr & cfg_addr:
                    srv = RedisManager(db_attr, self.done_q)
                    srv.daemon = True
                    srv.start()
                    self.srvs.append(srv)
                    INFO('Redis Manager {} spawned'.format(db_attr))
        for _ in self.srvs:
            try:
                db_attr = self.done_q.get(timeout=INIT_WAIT_LIMIT)
                db_attr.pop('db')
                INFO('Redis Manager {} started'.format(db_attr))
            except Empty:
                ERROR("{}:: Unable to start all Redis Managers."
                    .format(self.name))
                sys.exit(1)
        for srv in self.srvs:
            srv.join()
        ck.join()


def main():
    Managers().start()


if __name__ == "__main__":
    main()
