import os
from ConfigParser import ConfigParser, NoOptionError, NoSectionError

"""
Configuration file for determining port, path, etc. of Redis. This is used
by all loaded tests.

This file exports three variables, REDIS_BINARY which contains the executable
path for Redis; REDIS_MODULE which contains the path to the module to test,
and REDIS_PORT which connects to an already-existent redis server.

The `REDIS_PATH`, `REDIS_MODULE_PATH`, and `REDIS_PORT` environment variables
can all be used to override these settings.
"""


class ConfigVar(object):
    def __init__(self, env, prop, default=None):
        self.env = env
        self.prop = prop
        self.default = default
        self.value = default


cfg = ConfigParser()
cfg.read(['rmtest.config', 'rmtest2.config', 'rmtest.config.default'])

entries = {
    'path': ConfigVar('REDIS_PATH', 'executable', 'redis-server'),
    'module': ConfigVar('REDIS_MODULE_PATH', 'module'),
    'port': ConfigVar('REDIS_PORT', 'existing_port'),
    'process_per_test': ConfigVar('REDIS_PROCESS_PER_TEST', 'process_per_test'),
    'is_cluster': ConfigVar('REDIS_IS_CLUSTER', 'is_cluster'),
    'no_rdb_save': ConfigVar('REDIS_NO_RDBSAVE', 'no_rdb_save'),
    'no_keys_cmd': ConfigVar('REDIS_NO_KEYS_CMD', 'no_keys_cmd'),
}

for _, ent in entries.items():
    try:
        ent.value = cfg.get('server', ent.prop)
    except (NoOptionError, NoSectionError):
        pass

    # Override from environment
    if ent.env in os.environ:
        ent.value = os.environ[ent.env]

PROCESS_PER_TEST = bool(entries['process_per_test'].value)
REDIS_BINARY = entries['path'].value
REDIS_MODULE = entries['module'].value
REDIS_PORT = entries['port'].value
if REDIS_PORT:
    REDIS_PORT = int(REDIS_PORT)

IS_CLUSTER = entries['is_cluster'].value
NO_RDBSAVE = entries['no_rdb_save'].value
NO_KEYS_CMD = entries['no_keys_cmd'].value


def get_param(name, default=None):
    if name in os.environ:
        return os.environ[name]

    try:
        return cfg.get('testvars', name)
    except (NoOptionError, NoSectionError):
        return default
