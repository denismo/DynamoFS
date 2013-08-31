dynamo-fuse
===========

Linux FUSE file system implementation with AWS DynamoDB as the storage.

dynamo-fuse provides an implementation of a network shared file system on Linux and other FUSE-compatible platforms (should work on Mac, BSD - not tested). The key aspects of this file system are:

- **No servers** - the file system logic is residing on clients. All operations are translated into primitive storage access operations which are then efficiently handled by AWS DynamoDB.
  Concurrent access and locking are implemented using efficient concurrent data structures at the storage levels, most of which allow lock-free concurrent access to the underlying storage of the
  files and directories.

- **Infinite storage** - there is no limit (guaranteed by AWS DynamoDB) on how much data you can store. You don't need to do anything to store more - just keep putting in your files into the
file system and they'll get stored. There is no performance impact on storing many files. If your directory tree is balanced (you don't have folders with thousands of files) then the
file system will operate with millions of files as efficiently as with dozens.

- **High availability** - as there are no servers to manager there is little to go down. AWS DynamoDB is a highly available redundant data storage which transparently handles failover and reroutes the requests,
so from the file system point of view all requests are satisfied. The filesystem design is resilient to failures, with automatic retries and optimistic locking.

- **Highly concurrent** - the file system lock-free design means that most of the file operations can be execited by thousands of clients (that mounted the file system) in parallel
without significant impact on each other. At the same time the file system ensures that all concurrent operations leave the file system in a consistent state.

- **POSIX compliance** - the file system has been [tested](Testing.md) to satisfy the requirements of the POSIX specification.

Installation
============

Install it using `pip install dynamo-fuse`

Usage
=====

1. Install python-fuse:

        pip install python-fuse

2. (Optionally) Create 2 AWS Dynamo DB tables in the region of your choice.
   The first table must have Hash key named `path` and Range key named `name` (case matters, both Strings).
   The second table must have the name of the first table with the suffix "Blocks" appended, and with Hash key named `blockId` (String) and Range key named `blockNum` (Number) (case matters).

3. Define environment variables for AWS key - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. At the moment only the configuration by environment variables is supported.
The user with these keys must have read/write access to AWS Dynamo DB and the specified AWS Dynamo DB table. If the table does not exist it will automatically created.

4. Mount the filesystem:

        mount -t fuse.dynamo aws:<aws region>/<dynamo table> <mount point>

    For example, if you had a table named DynamoFS in ap-southeast-2 the command would be:

        mount -t fuse.dynamo aws:ap-southeast-2/DynamoFS /mnt/dynamo

   This will mount the table to the mount point. After that you will be able to execute normal Linux file commands, such as "ls" or "mkdir".

Status
==========

The implementation is almost POSIX-compliant and has been thoroughly [tested](Testing.md). All standard Linux file system syscalls are supported apart from the following:
- BSD-style file locks - only POSIX-style (via fcntl) locks are supported. The locks are **mandatory** by default.

License
=======

[GNU General Public License, version 3](http://opensource.org/licenses/gpl-3.0.html)