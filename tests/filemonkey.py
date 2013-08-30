import sys
import signal

__author__ = 'Denis Mikhalkin'

import os
import random
import Queue
import threading
from time import sleep
from subprocess import Popen
import fcntl
import traceback

actionQueue = Queue.Queue()
NUM_WORKERS = 10
NUM_FILES = 1
BASE_DIR = '/mnt/dynamo/jungle'
MAX_ACTION = 10
ITERATIONS = 100
SLEEP_DELAY = 0.2
stopFlag = False
actions = [
    "r",
    ["r", str(fcntl.LOCK_SH)],
    "w",
    "w+",
    ["w", str(fcntl.LOCK_EX)],
#    "rw",
#    ["rw", str(fcntl.LOCK_EX)],
    "delete",
    "rename",
    "symlink",
    "hardlink"
]

def worker():
    while not stopFlag:
        item = actionQueue.get()
        if 'finish' in item:
            actionQueue.task_done()
            return
        try:
            performAction(item)
        except Exception, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            pass
        actionQueue.task_done()

    while not actionQueue.empty():
        actionQueue.get_nowait()
        actionQueue.task_done()

def stopExecution(signal, frame):
    global stopFlag
    stopFlag = True
    print "Stopping"
    exit(-1)

def performAction(action):
    actionStr = actions[action['action']]
    if type(actionStr) == str:
        actionStr = [actionStr]
    print actionStr
    proc = Popen(["python", os.path.dirname(sys.argv[0]) + "/fileop.py", os.path.join(BASE_DIR, "file0")] + actionStr)
    counter = 0
    proc.poll()
    while proc.returncode is None and counter < 10:
        proc.poll()
        if proc.returncode is not None: break
        sleep(SLEEP_DELAY)
        counter += 1

    if proc.returncode is None or proc.returncode:
        if proc.returncode is None: proc.kill()
#        print "Failed"
#        global stopFlag
#        stopFlag = True

def startWorkers():
    for i in range(NUM_WORKERS):
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()

def preCreateFiles():
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)
    for i in range(0, NUM_FILES):
        file = open(os.path.join(BASE_DIR, "file" + str(i)), "w")
        file.write("aaaa")
        file.close()

def runAction(action):
    actionQueue.put({'action': action})

def runTests():
    preCreateFiles()
    startWorkers()
    print "Generating actions"
    for i in range(0, ITERATIONS):
        action = int(random.random() * len(actions))
        runAction(action)

    for i in range(NUM_WORKERS): actionQueue.put({'finish':True})

    print "Finished filling"
    actionQueue.join()
    print "Queue empty - exiting"

if __name__ == '__main__':
    signal.signal(signal.SIGINT, stopExecution)

    runTests()