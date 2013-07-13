import re
import subprocess
import signal
import socket
import os
import time
import unittest

from redis.manager.log import redis_config
from redis.manager.redis_cluster import UP_INTERVAL
from redis import RawRedis


class TestUtil(object):

    def _remove_next_line(self, str):
        return str[:-1]

    def _get_pid(self, filename):
        file = open(filename, 'r')
        pid = file.readline()
        return int(self._remove_next_line(pid))

    def _is_local(self, host):
        loc_addr = {addr[4][0] for addr in
                    socket.getaddrinfo('localhost', 80) +
                    socket.getaddrinfo(socket.getfqdn(), 80)}
        cfg_addr = {addr[4][0] for addr in
                    socket.getaddrinfo(host, 80)}
        return loc_addr & cfg_addr

    def get_master_slave_info(self):
        nodes_number = 0
        master_found = False
        slave_found = False
        redis_server_dict = {}
        pid_dir = redis_config['redis_server']['pidfile_dir']
        all_pid_files = subprocess.Popen(["ls", pid_dir],
            stdout=subprocess.PIPE)
        for index, filename in enumerate(all_pid_files.stdout):
            filename = self._remove_next_line(filename)
            pid_path = "{}/{}".format(pid_dir, filename)
            host_port = re.split('_', filename[:-len('.pid')])
            host = host_port[0]
            port = int(host_port[1])
            r = RawRedis(host=host, port=port)
            if not self._is_local(host):
                return False
            role = r.info()['role']
            if role == 'master' and not master_found:
                master_found = True
                pid = self._get_pid(pid_path)
                redis_server_dict['master'] = pid
            if role == 'slave' and not slave_found:
                slave_found = True
                pid = self._get_pid(pid_path)
                redis_server_dict['slave'] = pid
            nodes_number += 1
        redis_server_dict['nodes_number'] = nodes_number
        return redis_server_dict

    def check_dump_files(self):
        not_includes = -1
        check_dump_dir = "/usr/local/nb/redis/bin/redis-check-dump"
        dump_dir = redis_config['redis_server']['dump_dir']
        all_dump_files = subprocess.Popen(["ls", dump_dir],
            stdout=subprocess.PIPE)
        for index, dumpfile in enumerate(all_dump_files.stdout):
            corrupt_dump = True
            dumpfile = os.path.join(dump_dir, dumpfile)
            dumpfile = self._remove_next_line(dumpfile)
            result = subprocess.Popen([check_dump_dir, dumpfile],
                stdout=subprocess.PIPE)
            for res in result.stdout:
                is_includes = res.find('OK')
                if is_includes != not_includes:
                    corrupt_dump = False
            if corrupt_dump:
                return False
        return True


class TestRedisManagerReplication(unittest.TestCase):

    def setUp(self):
        self.util = TestUtil()

    def test_kill_master(self):
        redis_server_dict = self.util.get_master_slave_info()
        self.assertNotEqual(redis_server_dict, False)
        nodes_number_init = redis_server_dict['nodes_number']
        self.assertEqual(redis_server_dict.has_key('master'), True)
        self.assertNotEqual(nodes_number_init, 0)
        os.kill(redis_server_dict['master'], signal.SIGTERM)
        #Time for setting new replication chain
        time.sleep(UP_INTERVAL * 2)
        redis_server_dict = self.util.get_master_slave_info()
        self.assertEqual(redis_server_dict.has_key('master'), True)
        self.assertEqual(nodes_number_init, redis_server_dict['nodes_number'])
        self.assertEqual(self.util.check_dump_files(), True)

    @unittest.skipUnless(TestUtil().get_master_slave_info().has_key('slave'),
        "Replication is disabled.")
    def test_kill_slave(self):
        redis_server_dict = self.util.get_master_slave_info()
        self.assertNotEqual(redis_server_dict, False)
        nodes_number_init = redis_server_dict['nodes_number']
        self.assertNotEqual(nodes_number_init, 0)
        os.kill(redis_server_dict['slave'], signal.SIGTERM)
        #Time for setting new replication chain
        time.sleep(UP_INTERVAL * 2)
        redis_server_dict = self.util.get_master_slave_info()
        self.assertEqual(redis_server_dict.has_key('slave'), True)
        self.assertEqual(nodes_number_init, redis_server_dict['nodes_number'])
        self.assertEqual(self.util.check_dump_files(), True)


if __name__ == "__main__":
    TestUtil().get_master_slave_info()
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRedisManagerReplication)
    unittest.TextTestRunner(verbosity=2).run(suite)