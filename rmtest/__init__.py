import unittest
from .disposableredis import DisposableRedis
import os
import contextlib


REDIS_MODULE_PATH_ENVVAR = 'REDIS_MODULE_PATH'
REDIS_PATH_ENVVAR = 'REDIS_PATH'
REDIS_PORT_ENVVAR = 'REDIS_PORT'


def ModuleTestCase(module_path, redis_path='redis-server', fixed_port=None):

    module_path = os.getenv(REDIS_MODULE_PATH_ENVVAR, module_path)
    redis_path = os.getenv(REDIS_PATH_ENVVAR, redis_path)
    fixed_port = os.getenv(REDIS_PORT_ENVVAR, fixed_port)

    class _ModuleTestCase(unittest.TestCase):

        _module_path = os.path.abspath(os.path.join(os.getcwd(), module_path))
        _redis_path = redis_path

        def redis(self, port=None):
            if fixed_port is not None:
                port = fixed_port
            return DisposableRedis(port=port, path=self._redis_path, loadmodule=self._module_path)

        def assertOk(self, x, msg=None):
            self.assertEquals("OK", x, msg)

        def assertExists(self, r, key, msg=None):
            self.assertTrue(r.exists(key), msg)

        def assertNotExists(self, r, key, msg=None):
            self.assertFalse(r.exists(key), msg)  

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
