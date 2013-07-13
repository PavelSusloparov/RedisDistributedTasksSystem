__all__ = ['ReliableRedis', 'ReliableShard']
__revision__ = '$Id'

from functools import wraps
import os
import re
import time

from ..manager.redis_cluster import (ClusterAttributes, ClusterConnectionPool,
    NodeCounter)
from ..redis.exceptions import ConnectionError, ResponseError, InvalidResponse
from ..redis import RawRedis
from ..manager.log import (redis_config, conn_logger, MONDB,
    DEBUG_MONITOR as DEBUG, INFO_MONITOR as INFO, WARNING_MONITOR as WARNING,
    ERROR_MONITOR as ERROR)
from ..utils import cover_except
from ..mondb.events import RedisManagerEvent


class ReliableRedis(RawRedis):
    """
    Class is wrapper for original py-redis version 2.4.13.
    It is for client usage purposes.
    """
    RESPONSE_CALLBACKS = {}
    name = "Redis reliable connection"
    commands = ['pipeline', 'pubsub', 'publish',
                #### SERVER INFORMATION ####
                'bgrewriteaof', 'bgsave', 'config_get', 'config_set',
                'dbsize', 'debug_object', 'delete', 'echo', 'flushall',
                'flushdb', 'info', 'lastsave', 'object', 'ping', 'save',
                'shutdown', 'slaveof',
                #### BASIC KEY COMMANDS ####
                'append', 'decr', 'exists', 'expire', 'expireat', 'get',
                'getbit', 'getset', 'incr', 'keys', 'mget', 'mset', 'msetnx',
                'move', 'persist', 'randomkey', 'rename', 'renamenx', 'set',
                'setbit', 'setex', 'setnx', 'setrange', 'strlen', 'substr',
                'ttl', 'type', 'watch', 'unwatch',
                #### LIST COMMANDS ####
                'blpop', 'brpop', 'brpoplpush', 'lindex', 'linsert', 'llen',
                'lpop', 'lpush', 'lpushx', 'lrange', 'lrem', 'lset', 'ltrim',
                'rpop', 'rpoplpush', 'rpush', 'rpushx', 'sort',
                #### SET COMMANDS ####
                'sadd', 'scard', 'sdiff', 'sdiffstore', 'sinter',
                'sinterstore', 'sismember', 'smembers', 'smove', 'spop',
                'srandmember', 'srem', 'sunion', 'sunionstore',
                #### SORTED SET COMMANDS ####
                'zadd', 'zcard', 'zcount', 'zincrby', 'zinterstore', 'zrange',
                'zrangebyscore', 'zrank', 'zrem', 'zremrangebyrank',
                'zremrangebyscore', 'zrevrange', 'zrevrangebyscore',
                'zrevrank', 'zscore', 'zunionstore',
                #### HASH COMMANDS ####
                'hdel', 'hexists', 'hget', 'hgetall', 'hincrby', 'hkeys',
                'hlen', 'hset', 'hsetnx', 'hmset', 'hmget', 'hvals']

    def __init__(self, host=None, port=None, db='default', **kwargs):
        node_attributes = {}
        if host:
            node_attributes['host'] = host
        if port:
            node_attributes['port'] = port
        ca = ClusterAttributes(node_attr=node_attributes, db=db)
        node_attr = ca.node_attr
        self.repl_set_name = ca.repl_set_name
        self.cluster_attr = ca.attributes
        self.node_cluster_idx = ca.node_cluster_idx
        super(ReliableRedis, self).__init__(host=node_attr['host'], port=node_attr['port'],
            db=node_attr['db'], **kwargs)
        self.ccp = ClusterConnectionPool(self.repl_set_name,
            self.cluster_attr, self.node_cluster_idx, db=node_attr['db'])
        try:
            self.ccp.master_db.ping()
        except ConnectionError:
            NodeCounter(node_attr).wait_cluster_setting()
        self.node_attr = self.ccp.master_attr
        DEBUG("Master attributes: {}".format(self.node_attr))
        self.rm_event = RedisManagerEvent() if MONDB else None

    def _write_to_mondb(self, event_type):
        if self.rm_event:
            cluster_config = self.ccp.get_cluster_config()
            self.rm_event.replication_chain = cluster_config
            self.rm_event.process_affected = int(os.getpid())
            self.rm_event.event_type = event_type
            self.rm_event.process_type = 'client'
            self.rm_event.record()

    @cover_except(conn_logger)
    def __getattribute__(self, attr):
        if attr not in ReliableRedis.commands:
            return super(ReliableRedis, self).__getattribute__(attr)
        @wraps(self.ccp.master_db.__getattribute__(attr))
        def wrap(*args, **kwargs):
            self.ccp.update_cluster_config()
            while True:
                try:
                    DEBUG("{}::Inside __getattribute__. Attr: {}. Args: {}"
                        .format(ReliableRedis.name, attr, args))
                    f = self.ccp.master_db.__getattribute__(attr)
                    result = f(*args, **kwargs)
                    DEBUG("{}::{}, Result::{}".format(ReliableRedis.name, attr, result))
                except InvalidResponse as detail:
                    ERROR("{}::Invalid responce: {}".format(ReliableRedis.name, detail))
                    raise
                except ResponseError as detail:
                    ERROR("{}::Responce Error: {}".format(ReliableRedis.name, detail))
                    raise
                except ConnectionError:
                    self._write_to_mondb('down')
                    WARNING("{}::Attributes in function:: {},"
                        "old master attributes: {}".format(ReliableRedis.name, attr,
                        self.ccp.master_attr))
                    self.ccp.update_cluster_config()
                    self._write_to_mondb('up')
                    WARNING("{}::New master is {}".format(ReliableRedis.name,
                        self.ccp.master_attr))
                    continue
                return result
        return wrap


class ReliableShard(object):
    """
    Class shards tasks on several nodes.
    It uses Redis wrapper for connection to redis-server.
    """
    name = "Reliable shard"

    def __init__(self, collection='default', db='default'):
        self.collection = collection
        self.db = db
        self._set_attributes_pool()
        self._set_connection_pool()

    @cover_except(conn_logger)
    def __getattribute__(self, attr):
        DEBUG("{}::Inside get attr: {}".format(ReliableShard.name, attr))
        if attr not in ReliableRedis.commands:
            return super(ReliableShard, self).__getattribute__(attr)
        @wraps(self.registry_node.__getattribute__(attr))
        def wrapper(*args, **kwargs):
            DEBUG("Command {} is supported".format(attr))
            if not self.patterns:
                WARNING("{}::Patterns are not defined.".format(ReliableShard.name))
                DEBUG("{}::Sharding is disabled.".format(ReliableShard.name))
                return self._execute_on_main(attr, *args, **kwargs)
            DEBUG("{}::Sharding is enabled.".format(ReliableShard.name))
            matched_str = self._get_match_string(args)
            (user_func, matched_args) = self._match(attr, matched_str)
            if matched_args:
                DEBUG("{}::{} matched. Args: {}".format(ReliableShard.name, attr, matched_args))
                execute_res = self._execute_on_shard(attr, user_func, matched_args, **kwargs)
                if not execute_res:
                    DEBUG("User function is None. Use main node.")
                    return self._execute_on_main(attr, *args, **kwargs)
                return execute_res
            else:
                WARNING("{}:No matches. Use default scenario."
                    .format(ReliableShard.name))
                return self._execute_on_main(attr, *args, **kwargs)
        return wrapper

    def _set_attributes_pool(self):
        def add_node(node_attr):
            ca = ClusterAttributes(node_attr=node_attr, db=self.db)
            self.attributes_pool.append(ca.node_attr)

        self.attributes_pool = []
        shards = redis_config['sharded_collections']
        task_shard = shards.get(self.collection, shards['default'])
        self.shard_number = len(task_shard)
        DEBUG("Task shard: {}".format(task_shard))
        DEBUG("Shard_number: {}".format(self.shard_number))
        for index, repl_set in enumerate(task_shard):
            DEBUG("Replication set: {}".format(repl_set))
            if len(repl_set) == 1:
                # Replication set has only one node(single node mode)
                add_node(repl_set[0])
            else:
                # Get cluster configuration from redis
                ccp = ClusterConnectionPool('rs_{}'.format(index + 1), repl_set,
                    0, db=self.db)
                # Detect master from the configuration stored in Redis.
                # If not available, fallback to config settings.
                cluster_config = ccp.get_cluster_config()
                add_node(cluster_config[0] if cluster_config else repl_set[0])
        DEBUG("Collection {} shard on {} nodes.".format(self.collection, self.attributes_pool))

    def _set_connection_pool(self):
        """
        Set connection pool for one collection, using attributes_pool.
        """
        self.connection_pool = []
        for index, attr in enumerate(self.attributes_pool):
            conn = ReliableRedis(host=attr['host'], port=attr['port'], db=attr['db'])
            self.connection_pool.append(conn)
        self.registry_node = self.connection_pool[0]

    def _execute_on_main(self, attr, *args, **kwargs):
        """
        If user has not defined rule for method,
        default node will be used.
        """
        try:
            f = self.connection_pool[0].__getattribute__(attr)
            result = f(*args, **kwargs)
            DEBUG("To registry node::{}, Result::{}".format(attr, result))
            return result
        except KeyError as detail:
            ERROR("Cmd: {}. KeyError: {}.".format(attr, detail))
            raise

    def _get_match_string(self, args):
        """
        Make a string for pattern matching,
        using attr and args.
        """
        DEBUG("Args: {}".format(args))
        return ' '.join(list(args))

    def _match(self, cmd, matched_str):
        """
        Match input string with user string templeates.
        """
        cmd_attr = self.patterns.get(cmd, None)
        if not cmd_attr:
            WARNING("Pattern dict {} is not included {}"
                .format(self.patterns, cmd))
            return (None, None)
        DEBUG("Matched string: {}".format(matched_str))
        (reg_pat, user_func) = cmd_attr
        m = re.match(reg_pat, matched_str)
        if m:
            result = m.groups()
            DEBUG("Found match: {}".format(result))
            return (user_func, list(result))

    def _execute_on_shard(self, cmd, user_func, args, **kwargs):
        """
        Shard matched method on redis-server,
        using user shard key.
        """
        result_list = []
        data = user_func(*args[::-1])
        DEBUG("Data from user: {}".format(data))
        for key_params in data:
            key = key_params[0]
            params = key_params[1]
            DEBUG("Shard key: {}".format(key))
            DEBUG("Params: {}".format(params))
            conn_idx = hash(key) % self.shard_number
            conn = self.connection_pool[conn_idx]
            DEBUG("Cmd: {}, params: {}, kwargs: {}".format(cmd, params, kwargs))
            res = conn.__getattribute__(cmd)(*params, **kwargs)
            DEBUG("Result {} command: {}".format(cmd, res))
            result_list.append(res)
        return result_list

    def set_patterns(self, pattern_dict):
        self.patterns = {cmd: (re.compile(cmd_attr[0]), cmd_attr[1])
                            for cmd, cmd_attr in pattern_dict.items()}
        DEBUG("Patterns: {}".format(self.patterns))