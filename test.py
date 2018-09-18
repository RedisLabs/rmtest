from subprocess import Popen
import unittest
import os.path
from rmtest import ModuleTestCase
from rmtest.cluster import ClusterModuleTestCase
from rmtest.disposableredis import cluster


MODULE_PATH = os.path.abspath(os.path.dirname(__file__)) + '/' + 'module.so'


def build_module():
    csrc = MODULE_PATH[0:-3] + '.c'
    po = Popen(['cc', '-o', MODULE_PATH, '-shared', '-fPIC', csrc])
    po.communicate()
    po.wait()
    if po.returncode != 0:
        raise Exception('Failed to compile module')


class TestTestCase(ModuleTestCase(MODULE_PATH, module_args=('foo','bar'))):
    @classmethod
    def setUpClass(cls):
        super(TestTestCase, cls).setUpClass()
        # Check for the presence of the module
        if not os.path.exists(MODULE_PATH):
            build_module()

    def testContext(self):
        with self.redis() as r:
            with self.redis() as r:
                for _ in r.retry_with_rdb_reload():
                    self.assertOk(r.execute_command('TEST.TEST'))
                    with self.assertResponseError():
                        r.execute_command('TEST.ERR')

    def testBasic(self):
        self.assertTrue(self.server)
        self.assertTrue(self.client)
        with self.assertResponseError():
            self.cmd('TEST.ERR')


class ClusterTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(ClusterTestCase, cls).setUpClass()
        # Check for the presence of the module
        if not os.path.exists(MODULE_PATH):
            build_module()

    def setUp(self):
        self.cl = cluster.Cluster(num_nodes=3)

    def testCluster(self):
        ports = self.cl.start()

        self.assertEqual(3, len(ports))

        res = self.cl.broadcast('ping')
        self.assertListEqual(['PONG', 'PONG', 'PONG'], res)
    
    def tearDown(self):
        self.cl.stop()
        
class ClusterTestCaseWithModule(ClusterModuleTestCase(MODULE_PATH, num_nodes=5, module_args=('foo','bar'))):

    def testCluster(self):

        client = self.client()
        self.assertIsNotNone(client)

        for _ in self.retry_with_rdb_reload():
            self.assertOk(client.execute_command('TEST.TEST'))
            self.assertOk(self.cmd('TEST.TEST'))

            node = self.client_for_key("foobar")
            self.assertIsNotNone(node)
            with self.assertResponseError():
                client.execute_command('TEST.ERR')

if __name__ == '__main__':
    unittest.main()
