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

if not hasattr(__builtins__, 'bytes'):
    bytes = str

# TODO: Make file look like read-only after first write
# TODO: Eventual consistency of S3 during read
# TODO: Parallel uploads
# TODO: Upload manager
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
        uploadManager = dynamofuse.ioc.get(S3UploadManager)
        self.conn = uploadManager.getConnection()
        self.bucket = conn.lookup(os.path.join(self.accessor.s3BaseBucket, os.path.dirname(self.path)))
        self.multiPart = bucket.initiate_multipart_upload(os.path.basename(self.path))
        self.partNum = 0
        uploadManager.register(self.path, self.multiPart)

    def _write(self, data, offset):
        if not hasattr(self, "multiPart"):
            self.createS3Block()

        upload = boto.s3.multipart.MultiPartUpload(self.bucket)
        upload.key_name = self.multiPart.key_name
        upload.id = self.multiPart.id
        upload.upload_part_from_file(io.BytesIO(data), self.partNum+1)
        self.partNum += 1

    def read(self, offset, size):
        startBlock = offset / self.accessor.BLOCK_SIZE
        if offset+size > self.record["st_size"]:
            size = self.record["st_size"] - offset
        endBlock = (offset + size - 1) / self.accessor.BLOCK_SIZE
        data = cStringIO.StringIO()
        try:
            self.log.debug("read blocks [%d .. %d]", startBlock, endBlock)
            for block in range(startBlock, endBlock+1):
                try:
                    item = self.getBlock(block, getData=True)
                except FuseOSError, fe:
                    if fe.errno == ENOENT:
                        break
                    else:
                        raise
                if item is None:
                    self.log.debug("read block %d does not exist", block)
                    break
                if not "data" in item:
                    self.log.debug("read block %d has no data", block)
                    break
                itemData = item["data"].value
                writeLen = min(size, self.accessor.BLOCK_SIZE, len(itemData))
                startOffset = (offset % self.accessor.BLOCK_SIZE) if block == startBlock else 0
                self.log.debug("read block %d has %d data, write %d from %d", block, len(itemData), writeLen,
                    startOffset)
                data.write(itemData[startOffset:startOffset + writeLen])
                size -= writeLen

            return data.getvalue()
        finally:
            data.close()

    def truncate(self, length, fh=None):
        with self.writeLock():
            item = self.getFirstBlock()
            item['st_size'] = length
            item['st_ctime'] = max(l_time, item['st_ctime'])
            item['st_mtime'] = max(l_time, item['st_mtime'])
            item.save()

class S3UploadManager(object):
    def close(self, path):
        pass

    def register(self, path, multiPart):
        pass

    def getConnection(self):
        pass