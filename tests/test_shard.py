import argparse
import sys
import time
from redis import Redis
from redis.connection.connection import ReliableShard
from multiprocessing import JoinableQueue as Queue
from multiprocessing import Process

DEF_PORT = 6390

lambda_pattern = lambda key, *params: [(key, (params[0].split(' ')))]

PATTERNS = {'lpush': ( r"(test:(.*):lpush_test .*)", lambda_pattern ),
            'hget': ( r"(test:(.*):hget_test .*)", lambda_pattern ),
            'hset': ( r"(test:(.*):hset_test .*)", lambda_pattern ),
            'rpop': ( r"(test:(.*):rpop_test)", lambda_pattern ),
            'brpop': ( r"(test:(.*):brpop_test)", lambda_pattern ),
            'info': ( r"(test:(.*):info_test)", lambda key, *params: [(key, ([]))] )}

def INFO(message):
    print "INFO: {}".format(message)

class ParamsError(Exception):
    pass

class Worker(Process):

    def __init__(self, task_q, host, port, shard_num, cmd, params, kwargs):
        super(Worker, self).__init__()
        self.daemon = True
        self.task_q = task_q
        self.params = dict(params)
        self.kwargs = dict(kwargs)
        if shard_num:
            self.r = ReliableShard(collection='test{}'.format(shard_num), db='test')
            self.r.set_patterns(PATTERNS)
            self.shard = True
        else:
            self.r = Redis(host=host, port=port, db='test')
            self.shard = False
        self.cmd = cmd

    def run(self):
        param = []
        count = 0
        f = self.r.__getattribute__(self.cmd)
        while True:
            # s = time.time()
            task_idx = self.task_q.get(True)
            count += 1
            if self.cmd not in self.params.keys():
                raise ParamsError("There are no params for '{}' command.".format(self.cmd))
            if self.params.get(self.cmd, None):
                param = self.params[self.cmd][task_idx]
            if self.kwargs.get(self.cmd, None):
                kwargs = self.kwargs[self.cmd]
            else:
                kwargs = {}
            f(*param, **kwargs)
            # INFO("One task: {}".format(time.time() - s))
            # INFO("{}::Count: {}".format(self.name, count))
            self.task_q.task_done()

class Test(object):

    def __init__(self, cmd, cmd_num, shard_num, clear=True):
        self.cmd = cmd
        self.cmd_num = cmd_num
        self.shard_num = shard_num
        self.clear = clear
        self.task_q = Queue()

    def clear_db(self):
        if self.clear:
            for idx in range(1, self.shard_num + 1):
                r = Redis(host='redis{}'.format(idx), port=DEF_PORT + idx, db='test')
                r.flushdb()

    def _set_params(self):
        self.params = {'lpush': [], 'rpop': [], 'brpop': [],
          'hset': [], 'hget': [],
          'info': []}
        self.kwargs = {'brpop': {'timeout': 1}}
        if self.shard_num:
            for task_idx in range(self.cmd_num):
                self.params['lpush'].append(['test:key{}:lpush_test value{}'.format(task_idx, task_idx)])
                self.params['rpop'].append(['test:key{}:rpop_test'.format(task_idx, task_idx)])
                self.params['brpop'].append(['test:key{}:brpop_test'.format(task_idx, task_idx)])
                self.params['hset'].append(['test:key{}:hset_test field{} value{}'.format(task_idx, task_idx, task_idx)])
                self.params['hget'].append(['test:key{}:hget_test field{}'.format(task_idx, task_idx)])
                self.params['info'].append(['test:key{}:info_test None'.format(task_idx)])
        else:
            for task_idx in range(self.cmd_num):
                self.params['lpush'].append(['key{}'.format(task_idx), 'value{}'.format(task_idx)])
                self.params['rpop'].append(['key{}'.format(task_idx)])
                self.params['brpop'].append(['key{}'.format(task_idx)])
                self.params['hset'].append(['key{}'.format(task_idx), 'field{}'.format(task_idx), 'value{}'.format(task_idx)])
                self.params['hget'].append(['key{}'.format(task_idx), 'field{}'.format(task_idx)])
                self.params['info'] = []

    def start(self):
        self.clear_db()
        self._set_params()
        shard_num = self.shard_num
        if not self.shard_num:
            shard_num = 1
        for idx in range(1, shard_num + 1):
            host = 'redis{}'.format(idx)
            port = DEF_PORT + idx
            Worker(self.task_q, host, port, self.shard_num, self.cmd, self.params, self.kwargs).start()

        st = time.time()
        for task_idx in range(self.cmd_num):
            self.task_q.put(task_idx)
        self.task_q.join()

        INFO("{} process time: {} sec".format(self.cmd, time.time() - st))


def main():
    parser = argparse.ArgumentParser(prog='test_shard.py',
        description='Test for ReliableShard method. All parametrs are mandatory.',
        usage='%(prog)s [options]')

    parser.add_argument('-c', '--cmd', type=str, nargs=1,
        help='Command to redis.')
    parser.add_argument('-n', '--cmd_num', type=int, nargs='?',
        default = 1000, help='Number of commands.')
    parser.add_argument('-s', '--shard_num', type=int, nargs='?',
        default = 0, help='Number of shards.(1,2,4,8 for sharding. 0 for disable sharding.)')
    parser.add_argument('-f', '--flush', type=bool, nargs=1,
        default = False, help='Clear all redis-server.')

    args = parser.parse_args()

    if args.flush:
        Test('', 0, 8).clear_db()
        INFO("Clear all redis-server.")
        sys.exit()

    INFO("Input:\n{:>5}Command: {}\n{:>5}Number of commands: {}\n{:>5}Number of shards: {}\n"
        .format(' ', args.cmd, ' ', args.cmd_num, ' ', args.shard_num, ' '))

    if not args.cmd:
        ERROR("Please input command with -c option.\n")
        parser.print_help()
        sys.exit()

    if args.cmd == ['hget']:
        Test('hset', args.cmd_num, args.shard_num).start()
    if args.cmd == ['rpop']:
        Test('lpush', args.cmd_num, args.shard_num).start()
    if args.cmd == ['brpop']:
        Test('lpush', args.cmd_num, args.shard_num).start()
    Test(args.cmd[0], args.cmd_num, args.shard_num, clear=False).start()

if __name__ == "__main__":
    main()