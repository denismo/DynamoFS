#    Dynamo-Fuse - POSIX-compliant distributed FUSE file system with AWS DynamoDB as backend
#    Copyright (C) 2013 Denis Mikhalkin
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import with_statement
from boto.dynamodb.exceptions import DynamoDBConditionalCheckFailedError

__author__ = 'Denis Mikhalkin'

from errno import EAGAIN, EFAULT
from fuse import FuseOSError
import os
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging
import cStringIO
import uuid
from time import time, sleep

if not hasattr(__builtins__, 'bytes'):
    bytes = str

MAX_LOCK_RETRIES = 5

class DynamoLock:
    log = logging.getLogger("dynamo-fuse-lock  ")

    def __init__(self, path, accessor, item):
        self.path = path
        self.accessor = accessor
        self.lockId = uuid.uuid4().hex
        self.item = item

    def __enter__(self):
        self.log.debug(" Acquiring exclusive lock on %s", self.path)
        item = self.accessor.newItem(attrs={
            "path": os.path.dirname(self.path),
            "name": os.path.basename(self.path)
        })
        item.put_attribute('lockOwner', self.lockId)
        retries = 0
        while retries < MAX_LOCK_RETRIES:
            try:
                item.save(expected_value={'lockOwner': False})
                self.log.debug(" Got the lock on %s", self.path)
                return
            except DynamoDBConditionalCheckFailedError:
                # Somone acquired the lock before us
                sleep(1)
                retries += 1

        self.log.debug(" CANNOT lock %s", self.path)
        self.__exit__()
        raise FuseOSError(EAGAIN)

    def __exit__(self, type=None, value=None, traceback=None):
        self.log.debug(" Releasing exclusive lock on %s", self.path)
        if "deleted" in self.item:
            self.log.debug(" Not saving lock - item %s was deleted", self.path)
        else:
            item = self.accessor.newItem(attrs={
                "path": os.path.dirname(self.path),
                "name": os.path.basename(self.path),
            })
            item.delete_attribute("lockOwner")
            item.save(expected_value={'lockOwner': self.lockId})