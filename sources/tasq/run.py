__all__ = ['Managers']
__revision__ = '$Id'

import collections
import importlib
import json
import multiprocessing
import os
import re
import socket
import time
import traceback

from ..manager.redis_cluster import RESUBSCRIBE_INTERVAL, ClusterAttributes, NodeCounter
from ..connection import ReliableRedis, ReliableShard
from ..redis.exceptions import ConnectionError
from ..utils import ChildKiller, Lock, cover_except
from .log import (tasq_config, monitoring_logger, tasq_logger,
    ERROR, WARNING, INFO, DEBUG, MONITOR)
from ..manager.log import monitor_config, MONDB

if MONDB:
    from ..mondb.events import RedisTasqEvent
    from tesla.event.exception import exception

PATTERNS = {'lpush':( r"(tasq:(.*)_input) (.*)", lambda value, shard_key, key: [(shard_key, (key, value))] ),
            'brpop': ( r"(tasq:(.*)_input)", lambda shard_key, *params: [(shard_key, params)] ) }

class Monitoring(multiprocessing.Process):
    """
    Class functionality:
    1) Monitor tasq system parametrs in run time.
    2) Write tasq system condition to MonDb, if tesla is available.
    """

    def __init__(self, qr_attributes, task_type_workers):
        multiprocessing.Process.__init__(self)
        self.task_types_name = 'tasq:task_types'
        self.statistic_period = monitor_config['statistic_period']
        self.file_log = (monitor_config['file_log'] and
            monitor_config['file_log'] != '-')
        main_attr = qr_attributes
        self.task_type_workers = task_type_workers
        self.queues_registry = ReliableRedis(host=main_attr['host'],
            port=main_attr['port'], db='tasq')
        MONITOR("Monitoring process mode: {}".format(self.file_log))
        self.rs_pool = {}
        self.formats = {'sp': "{:>30}".format(''),
                       'eq': "{:=^30}".format('=')}

    def _get_task_instance(self, task_type):
        mod, klass = task_type.rsplit('.', 1)
        current_task_class = getattr(importlib.import_module(mod), klass)()
        return current_task_class

    def _add_db_info(self, connection, msg):
        db_size = connection.info()['used_memory']
        msg += ("{sp}NODE:{}\n{sp}DB_SIZE:{}\n{sp}{eq}{eq}\n"
                .format(connection.node_attr, db_size, **self.formats))
        return msg

    def _add_queues_info(self, task_inst, task_type, connection,
                        task_queues, msg):
        input_queue_name = task_inst.input_list
        result_queue_name = task_inst.result_list
        backup_set_name = task_inst.backup_set
        pipe = connection.pipeline()
        data = (pipe.llen(input_queue_name)
            .llen(result_queue_name).scard(backup_set_name)
            .execute())
        task_queues[task_type] = [{'input_queue': data[0]},
            {'result_queue': data[1]}, {'backup_set': data[2]}]
        queues = {'node': connection.node_attr,
            'input_queue': data[0] if data[0] else None,
            'result_queue': data[1] if data[1] else None,
            'backup_set': data[2] if data[2] else None}
        queues.update(**self.formats)
        if queues['input_queue']:
            msg += ("{sp}{node}\n"
                "{sp}{:>5}Warning: Input queue size:{input_queue}\n"
                .format(' ', **queues))
        if queues['result_queue']:
            msg += ("{sp}{node}\n"
                "{sp}{:>5}Warning: Result queue size:{result_queue}\n"
                .format(' ', **queues))
        if queues['backup_set']:
            msg += ("{sp}{node}\n"
                "{sp}{:>5}Warning: Backup set size:{backup_set}\n"
                .format(' ', **queues))
        return msg

    def _add_workers_info(self, task_workers, task_type, msg):
        self.task_worker_group = (self.task_type_workers.get(task_type, None))
        msg += "{sp}Workers: ".format(**self.formats)
        if not self.task_worker_group:
            msg += "None\n".format(**self.formats)
        else:
            workers = self.task_worker_group.workers_inst
            msg += "\n"
            task_workers[task_type] = []
            for index, worker in enumerate(workers):
                workers = {'index': index + 1,
                            'pid': worker.pid,
                            'name': worker.name}
                workers.update(**self.formats)
                (task_workers[task_type]
                    .append({worker.name: worker.pid}))
                msg += ("{sp}{:>5}{index}) Pid: {pid}, {name}\n"
                    .format(' ', **workers))
        return msg

    def _mondb_record(self, task_queues, task_workers, db_size, master_node):
            rte = RedisTasqEvent()
            rte.task_queues = str(task_queues)
            rte.workers = str(task_workers)
            rte.db_size = db_size
            rte.master_node = master_node
            rte.record()

    @cover_except(monitoring_logger)
    def run(self):
        while True:
            time.sleep(self.statistic_period)
            task_types = self.queues_registry.smembers(self.task_types_name)
            msg = "Existent task types: "
            if not task_types:
                msg += "None"
            else:
                task_queues = {}
                task_workers = {}
                master_node = ''
                db_size = None
                msg += "\n"
                for task_index, task in enumerate(task_types):
                    msg += "{sp}{}) {}\n".format(task_index + 1, task,
                        **self.formats)
                msg += "{sp}{eq}{eq}\n".format(**self.formats)
                for task_index, task_type in enumerate(task_types):
                    if task_type not in self.rs_pool.keys():
                        task_inst = self._get_task_instance(task_type)
                        rs = ReliableShard(collection='tasq', db='tasq')
                        self.rs_pool[task_type] = rs
                    msg += "{:>30}Task: {}\n".format(' ', task_type)
                    for conn in self.rs_pool[task_type].connection_pool:
                        if not task_index:
                            msg = self._add_db_info(conn, msg)
                        msg = self._add_queues_info(task_inst, task_type,
                            conn, task_queues, msg)
                    msg = self._add_workers_info(task_workers, task_type, msg)
                    if task_index != len(task_types)-1:
                        msg += "{sp}{eq}{eq}\n".format(**self.formats)
                self._mondb_record(task_queues, task_workers, db_size,
                    master_node)
            if self.file_log:
                MONITOR("{}\n{eq}{eq}{eq}".format(msg, **self.formats))


class ResultTracker(multiprocessing.Process):
    """
    Class tracks results for task from result_queue.
    """

    def __init__(self, task_class):
        multiprocessing.Process.__init__(self)
        self.task_class = task_class
        self.name = "ResultTracker"
        DEBUG("{} starting with pid: {}.".format(self.name, self.pid))

    @cover_except(tasq_logger)
    def run(self):
        self.task_class.solve()


class Task(object):
    """
    Base class for task.
    Next methods should be overloaded in child's classes:
    1) process
    2) solved_one
    3) solved_all
    """

    def __init__(self):
        if self._get_name() == 'Task':
            raise NotImplementedError('Customize your derived task '
                                      'through inheritance.')
        self.callback_logger = tasq_logger
        self.localhost = socket.getfqdn()
        self.rs = ReliableShard(collection='tasq', db='tasq')
        self.rs.set_patterns(PATTERNS)
        self.config = tasq_config['tasks_config']
        self._set_tasks_config()
        self._set_names()
        self.chord_len = 0
        self.num_enqueue_task = 0
        self.compression = 5
        self._is_registered = None
        self._task_register()

    @classmethod
    def _get_name(cls):
        return cls.__name__

    @classmethod
    def _get_type(cls):
        return "{}.{}".format(cls.__module__, cls.__name__)

    def _get_queue_name(self):
        return "tasq:{}".format(self._get_type())

    def _set_names(self):
        self.task_types_name = "tasq:task_types"
        self.input_list = "{}_input".format(self._get_queue_name())
        self.result_list = "{}_results".format(self._get_queue_name())
        self._backup_init()

    def _backup_init(self):
        self.backup_set = ("{}_backup_set".format(self._get_queue_name()))
        self.backup_lock = ("{}_lock".format(self.backup_set))
        self.lock = Lock(monitoring_logger)
        self.lock.set(self.rs.registry_node, self.backup_lock)

    def _set_tasks_config(self):
        self.task_config = self.config.get(self._get_type(), None)
        if not self.task_config:
            INFO("***Configuration for {} task is not specified."
                .format(self._get_name()))

        #Can be specified by user in code
        self.set_workers(None, base=True)

        #Realiable task section
        #Guaranteed can be specified by user in code
        self.set_guaranteed(self._get_config('guaranteed', is_bool=True),
            base=True)
        self.backup_retries = self._get_config('backup_retries')
        self.backup_task_timeout = self._get_config('backup_task_timeout')

        #Result queue section
        self.result_retries = self._get_config('result_retries')
        self.result_polling_interval = self._get_config('result_polling_interval')
        self.results_wait = (self.result_retries * self.result_polling_interval)

        #Input queue section
        self.input_polling_timeout = self._get_config('input_polling_timeout')
        self.downtime_mode = self._get_config('downtime_mode')
        self.downtime_mode = self.downtime_mode and self.downtime_mode != '-'
        self.downtime_limit = self._get_config('downtime_limit')
        self.downtime_wait = self.downtime_limit * self.input_polling_timeout

    def _get_config(self, name, is_bool=False):
        default_name = "default_{}".format(name)
        default_value = ((self.config[default_name] and
            self.config[default_name] != '-')
            if is_bool else self.config[default_name])
        return (self.task_config.get(name, default_value)
                if self.task_config else default_value)

    def _check_registered(self):
        if not self._is_registered:
            self._is_registered = self.rs.registry_node.sismember(self.task_types_name,
            self._get_queue_name())
            DEBUG("Check register:: Task was not registered.")
            return False
        DEBUG("Check register:: Task was already registered.")
        return True

    def _task_register(self):
        """
        Register task_type in master redis into task_types set.
        """
        if not self._check_registered():
            DEBUG("Task register:: {}.".format(self._get_name()))
            self.rs.registry_node.publish(self.task_types_name, self._get_queue_name())

    def _chord_queues_init(self, chord_id):
        """
        Start initialization for processing queues.
        """
        (chord_id_name, chord_id_time_name,
            self.chord_result_lock) = self.chord_queues(chord_id)
        self.lock.set(self.rs.registry_node, self.chord_result_lock)
        current_time = time.time()
        self.rs.registry_node.lpush(chord_id_time_name, current_time)

    def _enqueue_tasks(self, tasks, chord_id):
        """
        Put tasks in input_queue to redis.
        """
        self._chord_queues_init(chord_id)
        for task_num in tasks:
            self._put_task(task_num, chord_id)

    def _put_task(self, task_num, chord_id):
        """
        Put one task in input_queue to redis.
        """
        self.num_enqueue_task += 1
        new_chord_id = "{}/{}".format(chord_id, self.num_enqueue_task)
        task_data = (task_num, new_chord_id)
        attr = ["{} {}".format(self.input_list, json.dumps(task_data))]
        self.rs.lpush(*attr)

    def set_guaranteed(self, guaranteed, base=False):
        if not base:
            INFO("***Reliable task mode for {} is {}.".format(self._get_name(),
                "Enabled" if guaranteed else "Disabled"))
        self.guaranteed = guaranteed

    def set_workers(self, workers, base=False):
        if not base:
            INFO("***WORKERS SECTION WAS CHANGED BY USER ON {}."
                .format(workers))
        self.workers = workers

    def chord_queues(self, chord_id):
        chord_id_name = "tasq:{}_results".format(chord_id)
        chord_id_time_name = "{}_time".format(chord_id_name)
        chord_result_lock = "{}_lock".format(chord_id_name)
        return chord_id_name, chord_id_time_name, chord_result_lock

    def get_chord_info(self, chord_id):
        chord_len, host, chord_id_output, task_number = chord_id.split('/')
        chord_id_handled = chord_id[:-len(task_number)-1]
        return int(chord_len), host, task_number, chord_id_handled, chord_id_output

    #===============================================================
    #TASK BACKUP METHODS
    def _debug_undo(self, rsh_conn, task_chord_set,
                    worker, debug=False, use='BEFORE'):
        if debug:
            all = (rsh_conn.registry_node
                .zrevrangebyscore(self.backup_set, '+inf', '-inf'))
            if all:
                for index, task_chord in enumerate(all):
                    INFO("Backup task {} undo: {}) {}"
                        .format(use, index + 1, json.loads(task_chord)))
            else:
                INFO("UNDO::BACKUP SET {} UNDO IS EMPTY.".format(use))
            INFO("{}::UNDO BACKUP TASK: {}".format(worker,
                task_chord_set))

    def _get_backup_tasks(self, rsh_conn, worker):
        current_handling_task_chord = None
        if not self.guaranteed:
            return
        if not self.lock.get(rsh_conn.registry_node, self.backup_lock):
            DEBUG("{}:: Backup task: Late.".format(worker))
            return
        DEBUG("{}:: Backup task: handle it.".format(worker))
        score = time.time()
        task_chord_coll = (rsh_conn.registry_node
            .zrevrangebyscore(self.backup_set, score, '-inf'))
        DEBUG("{}:: All backup tasks: {}".format(worker, task_chord_coll))
        if task_chord_coll:
            current_item = json.loads(task_chord_coll[0])
            DEBUG("{}:: Get task {} from order set.".format(worker,
                current_item))
            DEBUG("{}:: Get task {} from order set.".format(worker,
                self._get_type()))
            current_handling_task_chord = current_item[1]
        self.lock.set(rsh_conn.registry_node, self.backup_lock)
        return current_handling_task_chord

    def _remove_task_from_order_set(self, rsh_conn,
                                    num_task_chord_set, worker):
        num_task_chord = json.dumps(num_task_chord_set)
        is_removed = rsh_conn.registry_node.zrem(self.backup_set,
            num_task_chord)
        DEBUG("{}:: Remove task {} from order set.".format(worker,
            num_task_chord))
        status = True if is_removed else False
        return status

    def set_backup(self, rsh_conn, task_chord_set, worker):
        DEBUG("{}:: Backup task: {}".format(worker, task_chord_set))
        backup_num_task_chord = []
        backup_task_chord = []
        attempts = 0
        time_now = time.time()
        score = time_now + self.backup_retries
        task_chord_coll = (rsh_conn.registry_node
            .zrevrangebyscore(self.backup_set, '+inf', time_now))
        DEBUG("{}:: All backup tasks: {}".format(worker, task_chord_coll))
        if task_chord_coll:
            for task_chord in task_chord_coll:
                backup_num_task_chord.append(json.loads(task_chord))
            for num_task_chord in backup_num_task_chord:
                backup_task_chord.append(num_task_chord[1])
            if task_chord_set in backup_task_chord:
                index = backup_task_chord.index(task_chord_set)
                attempts = backup_num_task_chord[index][0] + 1
                if attempts > self.backup_task_timeout:
                    ERROR("{}:: Tried more than {} times. "
                          "Removing backup task.".format(worker, attempts))
                    is_removed = self._remove_task_from_order_set(rsh_conn,
                        backup_num_task_chord[index], worker)
                    return is_removed
        else:
            DEBUG("{}::Backup set is empty. Put task with 0 num trying."
                .format(worker))
        new_backup_task = (attempts, task_chord_set)
        DEBUG("{}:: Put task {} in backup order set.".format(worker,
            new_backup_task))
        rsh_conn.registry_node.zadd(self.backup_set,
            json.dumps(new_backup_task), score)
        return True

    def undo_backup(self, rsh_conn, task_chord_set, worker):
        self._debug_undo(rsh_conn, task_chord_set, worker)
        attempts = 0
        num_task_chord_set = (attempts, task_chord_set)
        is_removed = self._remove_task_from_order_set(rsh_conn,
            num_task_chord_set, worker)
        DEBUG("Undo:: is removed: {}".format(is_removed))
        self._debug_undo(rsh_conn, task_chord_set, worker, use='AFTER')
        return is_removed

        #=================================================================

    def enqueue(self, item, chord=False, sync=False):
        """
        Put tasks in input_queue to redis.
        Tasks can be list, dictionary, string or integer.
        Tasks are distributed on shard cluster.
        Modes:
        - Chord. Client can set chord name by himself.
        - Sync. There are 2 modes:
        1. synchronous - Client should call solve method for getting results.
        2. asynchronous - Result are waited after enqueue call immediately.
        """
        self._task_register()
        if (isinstance(item, collections.Sequence)
            and not isinstance(item, str)):
            tasks = list(item)
        else:
            tasks = [item]
        DEBUG("Put task into redis: {}, task type: {}".format(tasks,
            type(tasks)))
        self.chord_len = len(tasks)
        if not self.chord_len: return
        if not chord:
            self.chord_len = 1
            chord_id = ("{}/{}/{}".format(self.chord_len,
                self.localhost, self._get_type()))
            self._chord_queues_init(chord_id)
            self._put_task(tasks, chord_id)
        elif isinstance(chord, str):
            chord_id = "{}/{}/{}".format(self.chord_len,
                self.localhost, chord)
            self._enqueue_tasks(tasks, chord_id)
        else:
            chord_id = ("{}/{}/{}".format(self.chord_len,
                self.localhost, self._get_type()))
            self._enqueue_tasks(tasks, chord_id)
        if not sync:
            p = ResultTracker(self)
            p.start()
            p.join()

    #===============================================================
    #GET TASK METHODS
    def _get_routine_task(self, rsh_conn, worker):
        data = rsh_conn.brpop(self.input_list, timeout=self.input_polling_timeout)[0]
        DEBUG("Routine brpop data: {}".format(data))
        if not data:
            return None
        (task_type, task_chord) = data
        try:
            task_chord_decoded  = json.loads(task_chord)
        except AttributeError:
            ERROR("Data for {} task is not valid. Customize input values."
                .format(self._get_type()))
            return None
        DEBUG("Routine:: task_chord_decoded: {}".format(task_chord_decoded))
        return task_chord_decoded

    def get_value(self, rsh_conn, worker):
        """
        Get tasks from redis.
        """
        task_chord = self._get_backup_tasks(rsh_conn, worker)
        if not task_chord:
            task_chord = self._get_routine_task(rsh_conn, worker)
        return task_chord

        #=================================================================

    def process(self, value):
        """
        Method for handling task, which user is put to enqueue.
        User must define method in overwritten own Task class.
        """
        raise NotImplementedError('Customize your task process '
                                    'through inheritance.')

    def solved_one(self, state, chord, results):
        """
        User can use the same output format, but logger
        should be overwritten.
        Please define self.callback_logger.
        """
        len_equals = int(state) / self.compression
        process_str = "[{}{}%]".format('=' * len_equals, state)
        self.callback_logger.debug("Solved_one:: Chord:{}\n{:>30}Processing {}"
            .format(chord, '', process_str))

    def solved_all(self, chord):
        """
        User can use the same output format, but logger
        should be overwritten.
        Please define self.callback_logger.
        """
        self.callback_logger.debug("Solved_all:{} was processed successfully."
            .format(chord))

    def solve(self):
        """
        Checks result_queue and gets results from it,
        if they are available.
        For synchronous mode, client should run this method
        for getting results.
        """
        chord_result_list = []
        result_len = 0
        for counter in range(self.result_retries):
            time_difference = 0
            while True:
                t_before = time.time()
                value_set = self.rs.registry_node.brpop(self.result_list,
                    timeout=self.result_polling_interval)
                time_difference = time.time() - t_before
                if value_set:
                    (queue_name, raw_value) = value_set
                    if not raw_value:
                        break
                else:
                    break
                value = json.loads(raw_value)
                DEBUG("VALUE: {}".format(value))
                result = value[0]
                chord = value[1].encode()
                DEBUG("RESULT: {}".format(result))
                chord_result_list.append(result)
                result_len = len(chord_result_list)
                state = (float(result_len) / float(self.chord_len)) * 100
                if self.chord_len != 1:
                    self.solved_one(state, chord, chord_result_list)
                DEBUG("Chord result list: {}".format(chord_result_list))
                if result_len == self.chord_len:
                    self.solved_all(chord)
                    DEBUG("Results: {}".format(chord_result_list))
                    return chord_result_list
            if time_difference < self.result_polling_interval:
                time.sleep(self.result_polling_interval - time_difference)
        WARNING("{}:: Can not handle in {} seconds.\n"
            "Results queue length: {}, necessary length: {}"
            .format(self._get_name(), self.results_wait,
                result_len, self.chord_len))


class Worker(multiprocessing.Process):
    """
    Worker is process, for handling tasks from redis.
    Workers quantity for task can be set in tasq.conf or in code
    by setting workers dictionary.
    """

    def __init__(self, klass, task_type):
        multiprocessing.Process.__init__(self)
        self.task_instance = klass
        self.task_type = self.task_instance._get_type()
        self._get_worker_name()
        self.rs = ReliableShard(collection='tasq', db='tasq')
        self.rs.set_patterns(PATTERNS)
        self.downtime_count = 0
        self.lock = Lock(monitoring_logger)

    def _get_worker_name(self):
        m = re.match(r"\w+-\d+\:(?P<num_worker>\d+)", self.name)
        worker_number = m.group('num_worker')
        self.short_task_type = self.task_instance._get_name()
        self.name = ("{}_Worker#{}".format(self.short_task_type,
            worker_number))

    def _process_task(self, task, chord_id, host):
        task_chord_set = (task, chord_id)
        DEBUG("{}:: Task chord set: {}".format(self.name, task_chord_set))
        if self.task_instance.guaranteed:
            self.task_instance.set_backup(self.rs, task_chord_set, self.name)
        try:
            result = self.task_instance.process(task)
            DEBUG("{}:: _process_task: result: {}".format(self.name, result))
            if self.task_instance.guaranteed:
                is_removed = self.task_instance.undo_backup(self.rs,
                    task_chord_set, self.name)
                if not is_removed:
                    return None
        except Exception:
            if MONDB:
                exception('Tasq: Unhandled exception in {} from {}'
                    .format(self.task_type, host))
            ERROR("{}:: Process function is corrupted: {}".format(self.name,
                traceback.format_exc()))
            return None
        return result

    def _clear_old_data(self):
        queue_start_time = (self.rs.registry_node.
            lrange(self.chord_id_time_name, 0, 1))
        if queue_start_time:
            current_time = time.time()
            if (float(queue_start_time[0]) +
                self.task_instance.results_wait < current_time):
                WARNING("Old data will be deleted.")
                pipe = self.rs.registry_node.pipeline()
                (pipe.ltrim(self.chord_id_name, 1, 0)
                 .rpop(self.chord_id_time_name).execute())

    def _handle_task(self, task_chord_set):
        (task, chord_id) = task_chord_set
        (chord_length, host, task_number, chord_id_handled,
            chord_id_output) = self.task_instance.get_chord_info(chord_id)
        result = self._process_task(task, chord_id, host)
        DEBUG("{}, {}:: Result: {}".format(self.task_type, self.name, result))
        if not result:
            return False
        (self.chord_id_name, self.chord_id_time_name,
            self.chord_result_lock) = (self.task_instance.
            chord_queues(chord_id_handled))
        result_set = (task_number, result)
        self.rs.registry_node.lpush(self.chord_id_name, json.dumps(result_set))
        current_len = int(self.rs.registry_node.llen(self.chord_id_name))
        self._clear_old_data()
        all_items = (self.rs.registry_node.lrange(self.chord_id_name, 0,
            current_len))
        DEBUG("Chord length: {}, current length: {} "
            .format(chord_length, current_len))
        DEBUG("{}:: Chord {} now. {}".format(self.name, self.chord_id_name,
            all_items))
        if self.task_instance.guaranteed:
            if current_len == chord_length:
                all_items = (self.rs.registry_node
                .lrange(self.chord_id_name, 0, current_len))
                DEBUG("{}:: Chord {} was handled. {}".format(self.name,
                    self.chord_id_name, all_items))
                self._set_result(current_len, chord_id_output)
        else:
            all_items = self.rs.registry_node.lrange(self.chord_id_name,
                0, current_len)
            DEBUG("{}:: Chord {} was handled. {}".format(self.name,
                self.chord_id_name, all_items))
            self._set_result(current_len, chord_id_output)
        return True

    def _set_result(self, chord_length, chord_id):
        if self.lock.get(self.rs.registry_node, self.chord_result_lock):
            DEBUG("{}:: Handle task's chord.".format(self.name))
            results = []
            json_results = (self.rs.registry_node.lrange(self.chord_id_name,
                0, chord_length))
            if json_results:
                for result_set in json_results:
                    result = json.loads(result_set)
                    results.append(result)
                results.sort()
                DEBUG("{}:: Ordered result: {}".format(self.name, results))
                pipe = self.rs.registry_node.pipeline()
                for result in results:
                    in_queue = (result[1], chord_id)
                    pipe.lpush(self.task_instance.result_list,
                        json.dumps(in_queue))
                pipe.execute()
                (pipe.ltrim(self.chord_id_name, 1, 0)
                 .rpop(self.chord_id_time_name).execute())
            else:
                WARNING("{}:: Json results for {} is empty.".format(self.name,
                    self.chord_id_name))
            self.lock.set(self.rs.registry_node, self.chord_result_lock)
        else:
            DEBUG("{}:: Late.".format(self.name))
            return

    @cover_except(tasq_logger)
    def run(self):
        """
        Start worker process.
        Firstly one task from chord is handled.
        Secondly if chord is more than one task, chord is handled.
        """
        INFO("{} starting with pid: {}".format(self.name, self.pid))
        while True:
            task_chord = self.task_instance.get_value(self.rs, self.name)
            if (self.downtime_count == self.task_instance.downtime_limit
                and self.task_instance.downtime_mode):
                self.downtime_count = 0
                INFO("{}:: Waiting {} already {} sec.".format(self.name,
                    self.task_type, self.task_instance.downtime_wait))
            if task_chord:
                self.downtime_count = 0
                task_status = self._handle_task(task_chord)
                DEBUG("{}::\n{:>30}Type: {}\n{:>30}Chord: {}\n{:>30}Status:{}"
                    .format(self.name, '', self.task_type, '', task_chord[1], '',
                    task_status))
            else:
                self.downtime_count += 1


class WorkerGroup(object):
    """
    Class incapsulates functions for each task type.
    """

    def __init__(self, task_type):
        self.task_type = task_type
        self.rs = ReliableShard(collection='tasq', db='tasq')
        self.rs.set_patterns(PATTERNS)
        self.workers_inst = []
        self.name = "Worker group"

    def _set_configuration(self):
        mod, klass = self.task_type.rsplit('.', 1)
        self.current_task_class = getattr(importlib.import_module(mod), klass)()

    def _get_num_worker(self):
        """
        Get dictionary from workers config section, where
            keys: ip addresses
            values: number of workers.
        """
        loc_addr = {addr[4][0] for addr in
                    socket.getaddrinfo('localhost', 80) +
                    socket.getaddrinfo(socket.getfqdn(), 80)}
        default_workers = [('localhost', 1)]
        DEBUG("{}:: Task type:{}".format(self.name, self.task_type))
        task_section = tasq_config['tasks_config'].get(self.task_type, None)
        workers = (task_section.get('workers', default_workers)
            if task_section else default_workers)
        for host_num_workers in workers:
            cfg_addr = {addr[4][0] for addr in
                socket.getaddrinfo(host_num_workers[0], 80)}
            if cfg_addr & loc_addr:
                self._set_configuration()
                if self.current_task_class.workers:
                    host_num_workers = self.current_task_class.workers
                INFO("{}:: {} is local. Workers can be created."
                    .format(self.name, host_num_workers[0]))
                return host_num_workers[1]
        WARNING("{}:: Workers for {} are not defined."
            .format(self.name, socket.getfqdn('localhost')))
        return 0

    def start_workers(self):
        """
        Start workers, finding local server in tasq configuration.
        """
        for i in range(self._get_num_worker()):
            INFO("{}:: Worker for {} is created.".format(self.name,
                self.task_type))
            w = Worker(self.current_task_class, self.task_type)
            w.start()
            self.workers_inst.append(w)
        else:
            return


class WorkerManager(multiprocessing.Process):
    """
    Class functionality:
    1) Creates workers for tasks from master redis set.
    2) Subscribes on channel for creating workers in future for new tasks.
    3) Starts monitoring process for tasq system.
    """

    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.name = "Worker manager"
        self.task_types_name = 'tasq:task_types'
        self.error_delay = 10
        self.qr_attributes = ClusterAttributes(db='tasq', collection='tns').node_attr
        self.queues_registry = ReliableRedis(host=self.qr_attributes['host'],
            port=self.qr_attributes['port'], db='tasq')
        self.task_type_workers = {}

    def _set_up(self):
        INFO("{}:: Queue registry attributes: {}".format(self.name, self.qr_attributes))
        task_types = self.queues_registry.smembers(self.task_types_name)
        INFO("{}:: Existent task_types: {}".format(self.name, task_types))
        if task_types:
            for task_type in task_types:
                DEBUG("Worker group for {} task is created.".format(task_type))
                wg = WorkerGroup(task_type)
                self.task_type_workers[task_type] = wg
                wg.start_workers()
        DEBUG("{}:: Task type workers: {}".format(self.name,
            self.task_type_workers))

    def get_task_type_workers(self):
        return self.task_type_workers

    @cover_except(tasq_logger)
    def run(self):
        INFO("{} starting with pid: {}".format(self.name, self.pid))
        self._set_up()
        ChildKiller(self.name, tasq_logger).start()
        Monitoring(self.qr_attributes, self.task_type_workers).start()
        while True:
            try:
                INFO("{}:: Subscribe on {} node.".format(self.name,
                    self.queues_registry.node_attr))
                subscription = self.queues_registry.pubsub()
                subscription.subscribe(self.task_types_name)
                for message in subscription.listen():
                    if message['channel'] == self.task_types_name:
                        new_task_type = message['data'].rsplit(':', 1)[1]
                        if new_task_type not in self.task_type_workers:
                            INFO("{}:: New task type: {}".format(self.name,
                                new_task_type))
                            self.queues_registry.sadd(self.task_types_name, new_task_type)
                            wg = WorkerGroup(new_task_type)
                            self.task_type_workers[new_task_type] = wg
                            wg.start_workers()
                            DEBUG("{}:: Task type workers: {}."
                                .format(self.name, self.task_type_workers))
            except ConnectionError:
                WARNING("{}:: Resubscribing..".format(self.name))
                time.sleep(RESUBSCRIBE_INTERVAL)
                continue


class Managers(object):
    def __init__(self):
        self.pid = os.getpid()
        self.name = "Manager Process"

    def _wait_redis_init(self):
        ca = ClusterAttributes(db='redis_manager')
        repl = ca.repl.get('tasq', ca.repl['default'])
        for single_repl in repl:
            master_db = single_repl[0]
            NodeCounter(master_db).wait_cluster_setting()

    @cover_except(tasq_logger)
    def start(self):
        INFO("{} starting with pid: {}".format(self.name, self.pid))
        ChildKiller(self.name, tasq_logger).start()
        self._wait_redis_init()
        WorkerManager().start()


def main():
    Managers().start()


if __name__ == "__main__":
    main()
