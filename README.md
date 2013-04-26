dynamo-fuse
===========

Linux FUSE file system implementation with AWS DynamoDB as the storage

Installation
============

Simply download the dynamofs.py or install it using `pip install dynamo-fuse`

Usage
=====

1. Install python-fuse:

        pip install python-fuse

2. Create an AWS Dynamo DB table in the region of your choice. The table must have Hash key named `path` and Range key named `name` (case matters, both Strings)

3. Define environment variables for AWS key - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. At the moment only the configuration by environment variables is supported.

4. Execute python command to mount the filesystem:

        python dynamofs <aws region> <dynamo table> <mount point>

   This will mount the table to the mount point. After that you will be able to execute normal Linux file commands, such as "ls" or "mkdir".

Limitations
===========

**Note: This project is in its early R&D stage. Various designs and implementation strategies are being tried for file system operations
so any practical usage is not recommended. Not all commands are implemented at the moment. The following has been implemented:**

- ls
- mkdir
- ln -s
- touch
- rmdir
- chmod
- chown
- mv

In summary, no file reading/writing operations have been implemented. Also, the behavior during concurrent access to the same files or directories is not defined.
