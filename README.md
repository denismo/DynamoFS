DynamoFS
===========

Linux FUSE file system implementation with AWS DynamoDB as the storage.

DynamoFS provides an implementation of a network shared file system on Linux and other FUSE-compatible platforms (should work on Mac, BSD - not tested).

- It is a network file system (like NFS) in that it does not require any disk management and can be used straight away after the installation of the client but unlike traditional NFS server there are no servers to manager or fail.
- It is a shared file system (like NFS) in that it allows many clients to mount one file system and use it concurrently but it is much more efficient in that than other shared file system as it is designed to be shared.

The key aspects of this file system are:

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

Usage scenarios
===============

The file system has been developed to cover the potential scenarios like this:

- You've got a data center but ther storage is limited and costly. You may be planning to go to cloud or waiting for budget to get more storage.
In the meantime you need to rapidly increase your capacity. DynamoFS can help with that by allowing you to mount the unlimited network file system either on your client
or on your NFS server. You can start using it literally in a couple of minutes (very little configuration required) and your files are always accessible in case if in future you decide to switch.

- You've got an application which utilizes NFS or some other third-party file system for shared file storage but it's unstable, requires lots of maintenance and has limited capacity.
With DynamoFS you can keep your application running as before but you get the benefits of predictable performance (tunable), fraction of maintenance time, virtually no downtime, and unlimited capacity.

Installation
============

Requires Python 2.6+ (may work with 3 but not tested).

1. Install fuse driver:

   RedHat/CentOS:

        yum install fuse

   Ubuntu:

        apt-get install fuse

2. Install dynamofs:

        pip install dynamo-fuse

Usage
=====

1. (Optionally) Create 2 AWS Dynamo DB tables in the region of your choice.
   The first table must have Hash key named `path` and Range key named `name` (case matters, both Strings).
   The second table must have the name of the first table with the suffix "Blocks" appended, and with Hash key named `blockId` (String) and Range key named `blockNum` (Number) (case matters).
   Or let the tables be automatically created (the user must then have permissions for that though).

1. Define AWS access keys. You have multiple options:
   - environment variables for AWS key - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
   - /etc/dynamofs.cfg (see [boto config](http://boto.readthedocs.org/en/latest/boto_config_tut.html) for format of the config file)
   - AWS EC2 Role dynamic access key

    The AWS IAM account with these keys (or AWS EC2 Role) must have read/write access to AWS Dynamo DB and the specified AWS Dynamo DB table.
    If the tables do not exist they will be automatically created (the user must then have permissions for that though).

1. Mount the filesystem:

        mount -t fuse.dynamo aws:<aws region>/<dynamo table> <mount point>

    For example, if you had a table named DynamoFS in ap-southeast-2 the command would be:

        mount -t fuse.dynamo aws:ap-southeast-2/DynamoFS /mnt/dynamo

    That's it. This will mount the shared file-system to the mount point. After that you will be able to execute normal Linux file commands, such as "ls" or "mkdir", and see the files created perhaps by
    other file system clients.

Status
==========

The implementation is almost POSIX-compliant and has been thoroughly [tested](Testing.md). All standard Linux file system syscalls are supported apart from the following:
- BSD-style file locks - only POSIX-style (via fcntl) locks are supported. However the locks are **mandatory** by default.

License
=======

[GNU General Public License, version 3](http://opensource.org/licenses/gpl-3.0.html)