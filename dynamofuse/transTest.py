from __future__ import with_statement

__author__ = 'Denis Mikhalkin'
from posix import R_OK, X_OK, W_OK
from errno import *
from os.path import realpath
from sys import argv, exit
from threading import Lock
import boto.dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from boto.exception import BotoServerError, BotoClientError
from boto.exception import DynamoDBResponseError
from stat import *
from boto.dynamodb.types import Binary
from time import time, sleep
from boto.dynamodb.condition import EQ, GT
import os
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context
from io import FileIO
import logging
from logging import StreamHandler, FileHandler
import sys
import cStringIO
import itertools
import traceback
from threading import Thread

if not hasattr(__builtins__, 'bytes'):
    bytes = str

global TestTable

def runTest():
    for i in range(1, 10):
        obj1 = TestTable.get_item("test", "obj1")
        obj2 = TestTable.get_item("test", "obj2")
        if obj1["size"] != obj2["size"]:
            raise Exception("Size mismatch: %d != %d" % (obj1["size"], obj2["size"]))

        obj1["size"] += 1
        obj2["size"] = obj1["size"]
        obj1.save()
        sleep(1)
        obj2.save()

def performTest():
    conn = boto.dynamodb.connect_to_region("ap-southeast-2", aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    table = conn.get_table("TestFS3")
    global TestTable
    TestTable = table
    item = table.new_item(attrs={"name": "obj1", "path":"test", "size": 1})
    item.put()
    item = table.new_item(attrs={"name": "obj2", "path":"test", "size": 1})
    item.put()

    Thread(target = runTest).start()
    Thread(target = runTest).start()

if __name__ == '__main__':
    performTest()