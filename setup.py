#!/usr/bin/env python

from __future__ import with_statement

from setuptools import setup
from os import chmod
import stat

import subprocess

try:
    from lib2to3 import refactor
    fixers = set(refactor.get_fixers_from_package('lib2to3.fixes'))
except ImportError:
    fixers = set()

with open('README.md') as readme:
    documentation = readme.read()

setup(
    name = 'dynamo-fuse',
    version = '0.1.4',

    description = 'Linux FUSE file system implementation with AWS DynamoDB as the storage',
    long_description = documentation,
    author = 'Denis Mikhalkin',
    author_email = 'denismo@yahoo.com',
    maintainer = 'Denis Mikhalkin',
    maintainer_email = 'denismo@yahoo.com',
    license = 'GNU General Public License, version 3',
    url = 'https://github.com/denismo/dynamo-fuse',
    packages=['dynamofuse', 'dynamofuse.records'],
    requires=['fusepy(>2.0)'],
    data_files=[
        ('/sbin', ['data/mount.fuse.dynamo'])
    ],

    use_2to3 = True,
    # only use the following fixers (everything else is already compatible)
    use_2to3_exclude_fixers = fixers - set([
        'lib2to3.fixes.fix_except',
        'lib2to3.fixes.fix_future',
        'lib2to3.fixes.fix_numliterals',
    ]),

    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Filesystems',
    ]
)

try:
    chmod('/sbin/mount.fuse.dynamo', stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IXOTH | stat.S_IROTH)
except:
    pass