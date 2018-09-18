# pylint: disable=missing-docstring, invalid-name, too-few-public-methods

"""
Configuration file for determining port, path, etc. of Redis. This is used
by all loaded tests.

This file exports three variables, REDIS_BINARY which contains the executable
path for Redis; REDIS_MODULE which contains the path to the module to test,
and REDIS_PORT which connects to an already-existent redis server.

The `REDIS_PATH`, `REDIS_MODULE_PATH`, and `REDIS_PORT` environment variables
can all be used to override these settings.
"""

import os
try:
    from configparser import ConfigParser, NoOptionError, NoSectionError
except ImportError:
    from ConfigParser import ConfigParser, NoOptionError, NoSectionError


class ConfigVar(object):
    def __init__(self, env, prop, default=None):
        self.env = env
        self.prop = prop
        self.default = default
        self.value = default


cfg = ConfigParser()
cfg.read(['rmtest.config'])

entries = {
    'path': ConfigVar('REDIS_PATH', 'executable', 'redis-server'),
    'module': ConfigVar('REDIS_MODULE_PATH', 'module'),
    'port': ConfigVar('REDIS_PORT', 'existing_port')
}

for _, ent in entries.items():
    try:
        ent.value = cfg.get('server', ent.prop)
    except (NoOptionError, NoSectionError):
        pass

    # Override from environment
    if ent.env in os.environ:
        ent.value = os.environ[ent.env]


REDIS_BINARY = entries['path'].value
REDIS_MODULE = entries['module'].value
REDIS_PORT = entries['port'].value
if REDIS_PORT:
    REDIS_PORT = int(REDIS_PORT)
