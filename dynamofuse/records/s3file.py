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
import io
import dynamofuse

__author__ = 'Denis Mikhalkin'

from posix import R_OK, X_OK, W_OK
from dynamofuse.records.block import BlockRecord
from dynamofuse.base import BaseRecord, DELETED_LINKS
from dynamofuse.records.file import File
from errno import  ENOENT, EINVAL, EPERM
import os
from os.path import realpath, join, dirname, basename
from threading import Lock
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from time import time
from boto.dynamodb.condition import EQ, GT
from boto.dynamodb.types import Binary
import logging
import cStringIO
from stat import *
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context
import itertools
import uuid
import boto.s3
import boto.s3.connection
import boto.s3.multipart

if not hasattr(__builtins__, 'bytes'):
    bytes = str

# TODO: Eventual consistency of S3 during read
# TODO: Parallel uploads http://bcbio.wordpress.com/2011/04/10/parallel-upload-to-amazon-s3-with-python-boto-and-multiprocessing/, https://gist.github.com/chrishamant/1556484
# TODO: Implement read
class S3File(File):
    log = logging.getLogger("dynamo-fuse-master")

    ################# OPERATIONS ##########################

    def deleteFile(self, linked=False):
        block = self.getFirstBlock()
        self.log.debug("Delete file, linked=%s, links=%d", linked, block["st_nlink"])
        block.add_attribute("st_nlink", -1)
        block['st_ctime'] = max(block['st_ctime'], int(time()))
        delete = not linked and ('deleted' in block and not block['deleted'] or not 'deleted' in block)
        setDeleted = not linked or ('deleted' in block and block['deleted'])
        if setDeleted:
            block['deleted'] = True
        if block["st_nlink"] == 1:
            self.log.debug("No more links - deleting records")
            items = self.accessor.blockTablev2.query(blockId__eq=self.record["blockId"], attributes=['blockId', 'blockNum'])
            for entry in items:
                entry.delete()

            BaseRecord.delete(self)
        else:
            if delete:
                block['st_nlink'] -= 1
                self.moveTo(os.path.join("/" + DELETED_LINKS, uuid.uuid4().hex), forceUpdate=True)
            else:
                block.save()

    def createS3Block(self):
        self.writeLock().__enter__(True) # Throws if erorr which will prevent any further operations
        uploadManager = dynamofuse.ioc.get(S3UploadManager)
        conn = self.accessor.getS3Connection()
        ''':type: boto.s3.connection.S3Connection'''
        self.bucket = conn.lookup(os.path.join(self.accessor.s3BaseBucket, os.path.dirname(self.path)))
        self.multiPart = self.bucket.initiate_multipart_upload(os.path.basename(self.path))
        self.partNum = 0
        uploadManager.register(self.path, self.multiPart, self)

    def getattr(self):
        res = File.getattr(self)
        # Prevent write into S3 files after they are created by the initial owner
        res['st_mode'] &= 07444
        return res

    def _write(self, data, offset):
        if not hasattr(self, "multiPart"):
            self.createS3Block()

        upload = boto.s3.multipart.MultiPartUpload(self.bucket)
        upload.key_name = self.multiPart.key_name
        upload.id = self.multiPart.id
        upload.upload_part_from_file(io.BytesIO(data), self.partNum)
        self.partNum += 1

    def read(self, offset, size):
        #TODO Handling of file being locked by write-in-progress
        #TODO Handling of file not being available on S3
        self.log.debug("reading %s from S3 at %d for %d", self.path, offset, size)
        conn = self.accessor.getS3Connection()
        ''':type: boto.s3.connection.S3Connection'''
        bucket = conn.get_bucket(os.path.join(self.accessor.s3BaseBucket, os.path.dirname(self.path)))
        key = bucket.get_key(os.path.basename(self.path))
        data = io.BytesIO()
        key.get_contents_to_file(data, headers=dict(Range= "bytes=%d-%d" % (offset, offset+size)))
        return data

    def truncate(self, length, fh=None):
        l_time = int(time())
        with self.writeLock():
            item = self.getFirstBlock()
            if length < item['st_size']:
                item['st_size'] = length
                item['st_ctime'] = max(l_time, item['st_ctime'])
                item['st_mtime'] = max(l_time, item['st_mtime'])
                item.save()
            else:
                raise FuseOSError(EINVAL)


class S3UploadManager(object):
    log = logging.getLogger("dynamo-fuse-master")
    def __init__(self):
        self.map = dict()

    def close(self, path):
        if self.map.has_key(path):
            uploadInfo = self.map.pop(path)
            uploadInfo.multiPart.complete_upload()
            uploadInfo.file.writeLock().__exit()

    def register(self, path, multiPart, fileItem):
        if self.map.has_key(path):
            self.log.error("S3UploadManager already has multipart in progress for " + path)
            raise FuseOSError(EINVAL)
        else:
            self.map[path] = {multiPart : multiPart, file: fileItem}
