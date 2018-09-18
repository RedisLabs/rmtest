# rmtest

A simple nano-framework for testing redis modules using python unit test, and a disposable ephemeral redis subprocess.

[![CircleCI](https://circleci.com/gh/RedisLabs/rmtest.svg?style=svg)](https://circleci.com/gh/RedisLabs/rmtest)

## Example:

```py

import unittest
from rmtest import ModuleTestCase

class MyTestCase(ModuleTestCase('../module.so', module_args = ('foo', 'bar'))):
            
    def testCmd(self):
        self.assertOk(self.cmd('mymodule.dosomething', 'foo', 'bar'))

if __name__ == '__main__':
    unittest.main()               
    
```

## Controlling parameters with Environment Variables

It is possible to control the path to the redis server executable, the path to the module and an optional fixed port, from environment variables.

### REDIS_MODULE_PATH

Controls the path to the module, either absolute, or relative to where the test resides.

### REDIS_PATH

Controls the path to redis-server. By default we assume it's in `$PATH`

### REDIS_PORT

Causes the tests to run against a fixed redis port instead of spawning ephemeral disposable redis instances for each test. 

This is useful for debugging failures with `redis-cli MONITOR`.

## REDIS_DEBUGGER

Causes the tests to be run under a debugger (e.g. `valgrind`). The value to this
environment variable is the path to the debugger. Does not work if there are spaces
in the path.


## Installing from pypi

```sh
$ pip install rmtest
```

## Installing from Git

1. Clone this repo

2. `sudo python setup.py install`
