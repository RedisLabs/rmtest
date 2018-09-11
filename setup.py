#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name='rmtest',
    version='1.0.0',
    description='Redis Module Testing Utility',
    url='http://github.com/RedisLabs/rmtest',
    packages=find_packages(),
    install_requires=['redis'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Database',
        'Topic :: Software Development :: Testing'
    ]
)
