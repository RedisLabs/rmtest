from subprocess import Popen
import unittest
import os.path
import rmtest2.config
from rmtest2 import BaseModuleTestCase, ClusterTestCase


MODULE_PATH = os.path.abspath(os.path.dirname(__file__)) + '/' + 'module.so'


def build_module():
    csrc = MODULE_PATH[0:-3] + '.c'
    po = Popen(['cc', '-o', MODULE_PATH, '-shared', csrc])
    po.communicate()
    if po.returncode != 0:
        raise Exception('Failed to compile module')


rmtest2.config.REDIS_MODULE = MODULE_PATH


class TestTestCase(BaseModuleTestCase):
    @classmethod
    def get_module_args(cls):
        return ['foo', 'bar']

    @classmethod
    def setUpClass(cls):
        super(TestTestCase, cls).setUpClass()
        # Check for the presence of the module
        if not os.path.exists(MODULE_PATH):
            build_module()

    def testBasic(self):
        with self.assertResponseError():
            self.cmd('TEST.ERR')

        for _ in self.reloading_iterator():
            with self.assertResponseError():
                self.cmd('TEST.ERR')
            self.assertCmdOk('TEST.TEST')


class ClusterTestCase(ClusterTestCase):
    @classmethod
    def get_module_args(cls):
        return ['foo', 'bar']

    @classmethod
    def setUpClass(cls):
        super(ClusterTestCase, cls).setUpClass()
        # Check for the presence of the module
        if not os.path.exists(MODULE_PATH):
            build_module()

    def testCluster(self):
        ports = self.get_cluster_ports()
        self.assertEqual(3, len(ports))

        res = self.broadcast('ping')
        self.assertListEqual(['PONG', 'PONG', 'PONG'], res)

    def testModuleOnCluster(self):
        for _ in self.reloading_iterator():
            self.assertCmdOk('TEST.TEST')
            self.cmd('TEST.TEST')
            self.execute_command('TEST.TEST')

            client = self.client_for_key("foobar")
            self.assertIsNotNone(client)
            with self.assertResponseError():
                client.execute_command('TEST.ERR')


if __name__ == '__main__':
    unittest.main()