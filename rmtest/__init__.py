import unittest
import os
import contextlib
from redis import ResponseError

from .disposableredis import DisposableRedis
from . import config

REDIS_MODULE_PATH_ENVVAR = 'REDIS_MODULE_PATH'
REDIS_PATH_ENVVAR = 'REDIS_PATH'
REDIS_PORT_ENVVAR = 'REDIS_PORT'


class BaseModuleTestCase(unittest.TestCase):
    """
    You can inherit from this base class directly. The server, port, and module
    settings can be defined either directly via the config module (see the
    config.py file), or via the rmtest.config file in the current directoy (i.e.
    of the process, not the file), or via environment variables.
    """

    # Class attributes which control how servers are created/destroyed

    # Create a new process for each test.
    process_per_test = False

    # Class level server. Usually true if process_per_test is false
    class_server = None

    # Module-specific arguments
    @classmethod
    def get_module_args(cls):
        return []

    @classmethod
    def get_server_args(cls):
        return {}

    def tearDown(self):
        cls = type(self)
        if cls.class_server:
            cls.class_server.reset()

        elif hasattr(self, '_server'):
            self._server.stop()
            del self._server

        super(BaseModuleTestCase, self).tearDown()

    def setUp(self):
        super(BaseModuleTestCase, self).setUp()
        self._ensure_server()

    @classmethod
    def tearDownClass(cls):
        if cls.class_server:
            cls.class_server.stop()
            cls.class_server = None
        super(BaseModuleTestCase, cls).tearDownClass()

    @classmethod
    def setup_class_server(cls):
        cls.class_server = cls.create_server()
        cls.class_server.start()

    @property
    def server(self):
        self._ensure_server()
        return self._server

    def redis(self):
        return self.server

    @property
    def client(self):
        self._ensure_server()
        return self._client

    def restart_and_reload(self):
        self._server.dump_and_reload(restart_process=True)
        self._client = self._server.client()

    def reloading_iterator(self):
        """
        You can use this function as a block under which to execute code while
        the server is reloaded.
        e.g.

        for _ in self.reloading_iterator:
            do_stuff
        """
        yield 1
        self._server.dump_and_reload(restart_process=False)
        yield 2

    retry_with_reload = reloading_iterator
    retry_with_rdb_reload = reloading_iterator

    def _ensure_server(self):
        if getattr(self, '_server', None):
            return

        if self.class_server:
            self._server = self.class_server
            self._client = self._server.client()
            return

        self._server = self.create_server()
        self._server.start()
        self._client = self._server.client()

        if not self.process_per_test:
            type(self).class_server = self._server

    @property
    def is_external_server(self):
        """
        :return: True if the connected-to server is already launched
        """
        return config.REDIS_PORT

    @classmethod
    def build_server_args(cls, **redis_args):
        if not config.REDIS_MODULE:
            raise Exception('No module specified. Use config file or environment!')
        redis_args.update(cls.get_server_args())
        redis_args.update(
            {'loadmodule': [config.REDIS_MODULE] + cls.get_module_args()})
        return redis_args

    @classmethod
    def create_server(cls, **redis_args):
        """
        Return a connection to a server, creating one or connecting to an
        existing server.
        """
        return DisposableRedis(path=config.REDIS_BINARY, port=config.REDIS_PORT,
                               **cls.build_server_args(**redis_args))

    #  Redis comand set

    def cmd(self, *args, **kwargs):
        return self.client.execute_command(*args, **kwargs)

    def execute_command(self, *args, **kwargs):
        return self.cmd(*args, **kwargs)

    def exists(self, *args):
        return self.client.exists(*args)

    def hmset(self, *args, **kwargs):
        return self.client.hmset(*args, **kwargs)

    def keys(self, *args, **kwargs):
        return self.client.keys(*args, **kwargs)

    def assertOk(self, x, msg=None):
        if type(x) == type(b""):
            self.assertEqual(b"OK", x, msg)
        else:
            self.assertEqual("OK", x, msg)

    def assertCmdOk(self, cmd, *args, **kwargs):
        self.assertOk(self.cmd(cmd, *args, **kwargs))

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
