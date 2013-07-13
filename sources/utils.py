__all__ = ['ChildKiller', 'load_config']
__revision__ = '$Id'

import multiprocessing
import os
import signal
import subprocess
import time
import traceback
import yaml

BASE_PATH = '/usr/local/nb/redis/config'

def load_config(filename):
    """
    Function gets data from config.
    """
    full_path = os.path.join(BASE_PATH, '{}.conf'.format(filename))
    stream = open(full_path, 'r')
    return yaml.load(stream)

def kill_processes(name, logger):
    """
    Kill all redis-server processes.
    """
    logger.warning("{} send sigterm signal to all redis-servers.".format(name))
    _process = subprocess.Popen(["pgrep", 'redis-server'],
        stdout=subprocess.PIPE)
    for index, pid in enumerate(_process.stdout):
        try:
            os.kill(int(pid), signal.SIGTERM)
        except OSError:
            logger.error("ChildKiller::OSError:Redis-server.")
            pass

class ChildKiller(multiprocessing.Process):
    """
    If process start ChildKiller and die,
    ChildKiller will send 'sigterm' signal to all children processes.
    """
    def __init__(self, parent_name, logger, redis=False):
        multiprocessing.Process.__init__(self)
        self.child = []
        self.tasq_init_time = 3
        self.between_requests_time = 5
        self.name = "{} child killer".format(parent_name)
        self.redis_flag = redis
        self.logger = logger

    def run(self):
        time.sleep(self.tasq_init_time)
        ppid = os.getppid()
        self.logger.info("{} starting with pid: {}, parent pid: {}"
            .format(self.name, self.pid, ppid))
        while True:
            try:
                command = "/usr/bin/pgrep -P {}".format(ppid)
                raw_child = subprocess.check_output(command, shell=True)
            except subprocess.CalledProcessError:
                self.logger.warning("{}::kill {} child's processes."
                    .format(self.name, self.child))
                for child_pid in self.child:
                    if child_pid != str(self.pid):
                        try:
                            os.kill(int(child_pid), signal.SIGTERM)
                        except OSError:
                            self.logger.error("ChildKiller:OSError.")
                            pass
                if self.redis_flag:
                    kill_processes(self.name, self.logger)
                break
            self.child = raw_child.split('\n')[:-1]
            time.sleep(self.between_requests_time)

class Lock(object):
    """
    Lock queue for multiprocessing proposes.
    """
    def __init__(self, logger):
        self.logger = logger

    def get(self, connection, queue_name):
        raw_lock = connection.rpop(queue_name)
        if raw_lock:
            lock = int(raw_lock)
            return lock
        return 0

    def set(self, connection, queue_name):
        attr_dict = connection.connection_pool.connection_kwargs
        while True:
            if connection.ping():
                if not connection.llen(queue_name):
                    connection.lpush(queue_name, 1)
                break
            else:
                self.logger.error("Can not get lock from queue {}.\n"
                      "Redis attributes:: host:{}, port:{}, db: {}"
                    .format(attr_dict['host'], attr_dict['port'],
                    attr_dict['db']))
                time.sleep(1)


def cover_except(logger):
    """
    Wrapper for catching exception and log it, using logger.
    """
    def wrapper(func):
        def catch_except(*args, **kwargs):
            try:
                results = func(*args, **kwargs)
            except Exception:
                logger.error("Covered Exception: {}"
                    .format(traceback.format_exc()))
                raise
            return results
        return catch_except
    return wrapper
