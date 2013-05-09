from dynamofuse import BaseRecord

__author__ = 'Denis Mikhalkin'

from errno import  ENOENT, EINVAL
from os.path import realpath, join, dirname, basename
from threading import Lock
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from time import time
from boto.dynamodb.condition import EQ, GT
from boto.dynamodb.types import Binary
import logging
import cStringIO
from stat import S_IFDIR, S_IFLNK, S_IFREG, S_ISREG, S_ISDIR
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import itertools

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Node(BaseRecord):
    pass