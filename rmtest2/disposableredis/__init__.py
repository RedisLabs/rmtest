import subprocess
import socket
import redis
import time
import os
import os.path
import sys
import warnings
import random
import traceback

REDIS_DEBUGGER = os.environ.get('REDIS_DEBUGGER', None)
REDIS_SHOW_OUTPUT = int(os.environ.get(
    'REDIS_VERBOSE', 1 if REDIS_DEBUGGER else 0))


def get_random_port():
    while True:
        port = random.randrange(1025, 10000)
        sock = socket.socket()
        try:
            sock.listen(port)
        except Exception:
            continue
        sock.close()
        return port


class DisposableRedis(object):

    def __init__(self, port=None, path='redis-server', **extra_args):
        """
        :param port: port number to start the redis server on.
            Specify none to automatically generate
        :type port: int|None
        :param extra_args: any extra arguments kwargs will
            be passed to redis server as --key val
        """
        self._port = port

        # this will hold the actual port the redis is listening on.
        # It's equal to `_port` unless `_port` is None
        # in that case `port` is randomly generated
        self.port = None
        self._is_external = True if port else False
        self.use_aof = extra_args.pop('use_aof', False)
        self.extra_args = []
        for k, v in extra_args.items():
            self.extra_args.append('--{}'.format(k))
            if isinstance(v, (list, tuple)):
                self.extra_args += list(v)
            else:
                self.extra_args.append(v)

        self.path = path
        self.errored = False
        self.dumpfile = None
        self.aoffile = None
        self.pollfile = None
        self.process = None

    def _get_output(self):
        if not self.process:
            return ''
        return '' if REDIS_SHOW_OUTPUT else self.process.stdout.read()

    def _start_process(self, *args):
        if self._is_external:
            return

        if REDIS_DEBUGGER:
            debugger = REDIS_DEBUGGER.split()
            args = debugger + list(args)

        stdout = None if REDIS_SHOW_OUTPUT else subprocess.PIPE
        if REDIS_SHOW_OUTPUT:
            sys.stderr.write("Executing: {}".format(repr(args)))
        # print("Launching new process..")
        # traceback.print_stack()
        self.process = subprocess.Popen(
            args,
            stdin=sys.stdin,
            stdout=stdout,
            stderr=sys.stderr,
        )

        begin = time.time()
        while True:
            try:
                self.client().ping()
                break
            except (redis.ConnectionError, redis.ResponseError):
                self.process.poll()
                if self.process.returncode is not None:
                    raise RuntimeError(
                        "Process has exited with code {}\n. Redis output: {}"
                        .format(self.process.returncode, self._get_output()))

                if time.time() - begin > 300:
                    raise RuntimeError(
                        'Cannot initialize client (waited 5mins)')

                time.sleep(0.1)

    def start(self):
        """
        Start the server. To stop the server you should call stop()
        accordingly
        """
        if self._port is None:
            self.port = get_random_port()
        else:
            self.port = self._port

        if not self.dumpfile:
            self.dumpfile = 'dump.{}.rdb'.format(self.port)
        if not self.aoffile:
            self.aoffile = 'appendonly.{}.aof'.format(self.port)

        args = [self.path,
                '--port', str(self.port),
                '--save', '',
                '--dbfilename', self.dumpfile]
        if self.use_aof:
            args += ['--appendonly', 'yes',
                     '--appendfilename', self.aoffile]
        args += self.extra_args

        self._start_process(*args)

    def _cleanup_files(self):
        for f in (self.aoffile, self.dumpfile):
            try:
                os.unlink(f)
            except OSError:
                pass

    def stop(self, for_restart=False):
        if self._is_external:
            return

        self.process.terminate()
        if not for_restart:
            self._cleanup_files()

    def _wait_for_child(self):
        # Wait until file is available
        r = self.client()
        while True:
            info = r.info('persistence')
            if info['aof_rewrite_scheduled'] or info['aof_rewrite_in_progress']:
                time.sleep(0.1)
            else:
                break

    def dump_and_reload(self, restart_process=False):
        """
        Dump the rdb and reload it, to test for serialization errors
        """
        conn = self.client()

        if restart_process:
            if self._is_external:
                warnings.warn('Tied to an external process. Cannot restart')
                return
            import time
            conn.bgrewriteaof()
            self._wait_for_child()

            self.stop(for_restart=True)
            self.start()
        else:
            conn.save()
            try:
                conn.execute_command('DEBUG', 'RELOAD')
            except redis.RedisError as err:
                self.errored = True
                raise err

    def client(self):
        """
        :rtype: redis.StrictRedis
        """
        return redis.StrictRedis(port=self.port)

    def reset(self):
        conn = self.client()
        conn.flushdb()