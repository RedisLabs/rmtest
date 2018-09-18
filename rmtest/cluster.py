# pylint: disable=line-too-long, missing-docstring, invalid-name, duplicate-code

import os
import contextlib
import unittest
from redis import Redis, ConnectionPool, ResponseError
from .disposableredis.cluster import Cluster

REDIS_MODULE_PATH_ENVVAR = 'REDIS_MODULE_PATH'
REDIS_PATH_ENVVAR = 'REDIS_PATH'
REDIS_PORT_ENVVAR = 'REDIS_PORT'

def ClusterModuleTestCase(module_path, num_nodes=3, redis_path='redis-server', fixed_port=None, module_args=tuple()):
    """
    Inherit your test class from the class generated by calling this function
    module_path is where your module.so resides, override it with REDIS_MODULE_PATH in env
    redis_path is the executable's path, override it with REDIS_PATH in env
    redis_port is an optional port for an already running redis
    module_args is an optional tuple or list of arguments to pass to the module on loading
    """

    module_path = os.getenv(REDIS_MODULE_PATH_ENVVAR, module_path)
    redis_path = os.getenv(REDIS_PATH_ENVVAR, redis_path)
    fixed_port = os.getenv(REDIS_PORT_ENVVAR, fixed_port)

    # If we have module args, create a list of arguments
    loadmodule_args = module_path if not module_args else [module_path] + list(module_args)

    class _ModuleTestCase(unittest.TestCase):


        @classmethod
        def setUpClass(cls):
            if fixed_port:
                cls._cluster = None
                cls._client = Redis(port=fixed_port, connection_pool=ConnectionPool(port=fixed_port))
            else:
                cls._cluster = Cluster(num_nodes, path=redis_path, loadmodule=loadmodule_args)
                cls._ports = cls._cluster.start()
                cls._client = cls._cluster.nodes[0].client()

        @classmethod
        def tearDownClass(cls):
            if cls._cluster:
                cls._cluster.stop()


        def client(self):
            return self._client


        def client_for_key(self, key):
            if not self._cluster:
                return self._client
            return self._cluster.client_for_key(key)

        def key_cmd(self, cmd, key, *args, **kwargs):
            """
            Execute a command where the key needs to be known
            """
            conn = self.client_for_key(key)
            return conn.execute_command(cmd, key, *args, **kwargs)

        def cmd(self, *args, **kwargs):
            """
            Execute a non-sharded command without selecting the right client
            """
            return self._client.execute_command(*args, **kwargs)

        def assertOk(self, okstr, msg=None):
            if isinstance(okstr, (bytes, bytearray)):
                self.assertEqual(b"OK", okstr, msg)
            else:
                self.assertEqual("OK", okstr, msg)

        def assertCmdOk(self, cmd, *args, **kwargs):
            self.assertOk(self.cmd(cmd, *args, **kwargs))

        def assertExists(self, key, msg=None):
            conn = self.client_for_key(key)
            self.assertTrue(conn.exists(key), msg)

        def assertNotExists(self, key, msg=None):
            conn = self.client_for_key(key)
            self.assertFalse(conn.exists(key), msg)


        def retry_with_rdb_reload(self):
            """
            Send DEBUG RELOAD to all nodes and test the result
            """
            yield 1
            if self._cluster:
                self._cluster.broadcast('DEBUG', 'RELOAD')
            else:
                self._client.execute_command('DEBUG', 'RELOAD')
            yield 2


        @contextlib.contextmanager
        def assertResponseError(self, msg=None):
            """
            Assert that a context block with a redis command triggers a redis error response.

            For Example:

                with self.assertResponseError():
                    r.execute_command('non_existing_command')
            """

            try:
                yield
            except ResponseError:
                pass
            else:
                self.fail("Expected redis ResponseError " + (msg or ''))


    return _ModuleTestCase
