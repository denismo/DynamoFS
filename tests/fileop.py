from sys import argv, exit
import fcntl
import os
import sys
import traceback

__author__ = 'Denis Mikhalkin'

def createIfNecessary(file):
    if not os.path.exists(file):
        file = open(file, "w")
        file.write("aaaa")
        file.close()

if __name__ == '__main__':
    filePath = argv[1]
    mode = argv[2]
    lock = argv[3] if len(argv) > 3 else None

    try:
        if mode == "delete":
            os.unlink(filePath)
            exit(0)
        elif mode == "rename":
            if os.path.exists(filePath+".new"): os.unlink(filePath+".new")
            os.rename(filePath, filePath+".new")
            exit(0)
        elif mode == "symlink":
            if os.path.exists(filePath+".symlink"): os.unlink(filePath+".symlink")
            os.symlink(filePath, filePath+".symlink")
            exit(0)
        elif mode == "hardlink":
            if os.path.exists(filePath+".link"): os.unlink(filePath+".link")
            os.link(filePath, filePath + ".link")
            exit(0)

        if mode == "r":
            createIfNecessary(filePath)

        file = open(filePath, mode)
        if lock is not None and lock != "None":
            fcntl.lockf(file, int(lock))

        if mode == "r":
            file.read(1)
        elif mode == "w+":
            file.write("a")
        elif mode == "rw":
            file.read(1)
            file.write("a")

    except Exception, e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print argv, "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        exit(-1)

    if file: file.close()
    exit(0)
