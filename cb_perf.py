#!/usr/bin/env python3

'''
Invoke Couchbase Pillow Fight
'''

import os
import sys
import argparse
import json
import re
import subprocess
import time
import asyncio
import acouchbase.cluster
import requests
import math
import warnings
from couchbase_core.items import Item, ItemOptionDict
from couchbase_core._libcouchbase import LOCKMODE_EXC, LOCKMODE_NONE, LOCKMODE_WAIT
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import QueryOptions
from couchbase.management.buckets import CreateBucketSettings
from couchbase.exceptions import BucketNotFoundException
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty
import threading

class atomicCounter(object):
    def __init__(self):
        self.value = 0
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self.value += 1

class dynamicInventory(object):

    def __init__(self):
        self.out_thread = None
        self.err_thread = None
        self.out_queue = Queue()
        self.err_queue = Queue()
        self.counterLock = threading.Lock()
        self.recordId = 0
        self.currentOp = 0
        self.percentage = 0
        self.statusThreadRun = 1
        self.writePercent = 50
        self.scenarioWrite = {
            'a': 50,
            'b': 5,
            'c': 0
        }
        self.parse_args()
        self.clusterConnect()

        warnings.filterwarnings("ignore")

        if not self.bucketMemory:
            self.getMemQuota()

        if not self.manualMode or self.makeBucketOnly:
            self.createBucket()
            if self.makeBucketOnly:
                sys.exit(0)

        if not self.manualMode or self.loadOnly:
            self.dataLoad()
            if self.loadOnly:
                sys.exit(0)

        if not self.manualMode or self.runOnly:
            for key in self.scenarioWrite:
                if self.runWorkload:
                    if self.runWorkload != key:
                        continue
                self.writePercent = self.scenarioWrite[key]
                self.kvTest(key)
            if self.runOnly:
                sys.exit(0)

        if not self.manualMode or self.dropBucketOnly:
            self.deleteBucket()

    def clusterConnect(self):
        try:
            self.auth = PasswordAuthenticator(self.username, self.password)
            self.cluster = Cluster("http://" + self.host + ":8091", authenticator=self.auth, lockmode=LOCKMODE_NONE)
            self.bm = self.cluster.buckets()
        except Exception as e:
            print("Could not connect to couchbase: %s" % str(e))
            sys.exit(1)

    def _bucketExists(self, bucket):
        try:
            bucket = self.cluster.bucket(bucket)
            return True
        except BucketNotFoundException as e:
            return False
        except Exception as e:
            print("Could not get bucket status: %s" % str(e))
            sys.exit(1)

    def _bucketRetry(self, bucket, limit=5):
        for i in range(limit):
            try:
                time.sleep(1)
                return self.bm.get_bucket(bucket)
            except Exception:
                time.sleep((i + 1) * 0.5)

        raise Exception('Timeout: Unable to get new bucket status')

    def getMemQuota(self):
        response = requests.get("http://" + self.host + ":8091" + '/pools/default',
                                auth=(self.username, self.password))
        response_json = json.loads(response.text)
        if 'memoryQuota' in response_json:
            self.bucketMemory = response_json['memoryQuota']
        else:
            print("Can not get memory quota from the cluster.")
            sys.exit(1)

    def asyncConnect(self):
        try:
            cluster = acouchbase.cluster.Cluster("http://" + self.host + ":8091", authenticator=self.auth, lockmode=LOCKMODE_NONE)
            bucket = cluster._cluster.open_bucket(self.bucket, lockmode=LOCKMODE_NONE)
            # bucket = cluster.bucket(self.bucket)
            return cluster, bucket
            # self.asyncCollection = self.asyncBucket.scope("_default").collection("_default")
            # self.upsertOpts = UpsertOptions(durability=ServerDurability(Durability.))
        except Exception as e:
            print("Can not connect to cluster: %s" % str(e))
            sys.exit(1)

    def createBucket(self):
        if not self._bucketExists(self.bucket):
            try:
                self.bm.create_bucket(CreateBucketSettings(name=self.bucket, bucket_type="couchbase", ram_quota_mb=self.bucketMemory))
                self._bucketRetry(self.bucket)
            except Exception as e:
                print("Could not create bucket: %s" % str(e))
                sys.exit(1)

    def deleteBucket(self):
        if self._bucketExists(self.bucket):
            try:
                self.bm.drop_bucket(self.bucket)
            except Exception as e:
                print("Could not drop bucket: %s" % str(e))
                sys.exit(1)

    def _getNextId(self, n):
        min_lc = ord(b'0')
        len_lc = 10
        ba = bytearray(os.urandom(n))
        for i, b in enumerate(ba):
            ba[i] = min_lc + b % len_lc
        return ba

    def asyncInsert(self, q, jsonDoc, numRecords, startNum):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            cluster, bucket = self.asyncConnect()
            collection = bucket.scope("_default").collection("_default")
        except Exception as e:
            print("Error connecting to couchbase: %s" % str(e))
            sys.exit(1)

        numRemaining = numRecords
        counter = int(startNum)
        while numRemaining > 0:
            # counter.increment()
            # recordId = math.pow(10, counter) + x
            # q.put(x)
            if numRemaining <= self.batchSize:
                runBatchSize = numRemaining
            else:
                runBatchSize = self.batchSize
            numRemaining = numRemaining - runBatchSize
            q.put(runBatchSize)
            batch = ItemOptionDict()
            for y in range(int(runBatchSize)):
                item = Item(str(format(counter, '032')), jsonDoc)
                counter += 1
                # batch = ItemOptionDict()
                batch.add(item)
            loop.run_until_complete(collection.upsert_multi(batch))
        loop.close()

    def asyncRead(self, q, numRecords, startNum):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tasks = []
        try:
            cluster, bucket = self.asyncConnect()
            collection = bucket.scope("_default").collection("_default")
        except Exception as e:
            print("asyncRead: error connecting to couchbase: %s" % str(e))
            sys.exit(1)

        numRemaining = numRecords
        counter = int(startNum)
        while numRemaining > 0:
            # counter.increment()
            # recordId = math.pow(10, counter) + x
            # q.put(x)
            if numRemaining <= self.batchSize:
                runBatchSize = numRemaining
            else:
                runBatchSize = self.batchSize
            numRemaining = numRemaining - runBatchSize
            q.put(runBatchSize)
            batch = []
            for y in range(int(runBatchSize)):
                batch.append(str(format(counter, '032')))
                counter += 1
            loop.run_until_complete(collection.get_multi(batch))
        loop.close()

    def insertDocuments(self, q, jsonDoc, numRecords, startNum):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.asyncInsert(q, jsonDoc, numRecords, startNum))
        finally:
            loop.close()

    def readDocuments(self, q, numRecords, startNum):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.asyncRead(q, numRecords, startNum))
        finally:
            loop.close()

    def printStatusThread(self, q, count):

        while True:
            while not q.empty():
                entry = int(q.get())
                if entry == 0:
                    sys.stdout.write("\rOperation Complete.\033[K\n")
                    print("%d documents processed." % self.currentOp)
                    return
                self.currentOp += entry
                self.percentage = (self.currentOp / count) * 100
                end_char = '\r'
                print("Document %d of %d in progress, %d%% completed ... " % (self.currentOp, count, self.percentage), end=end_char)
            time.sleep(1)

    def printStatusReset(self):
        self.currentOp = 0
        self.percentage = 0

    def dataLoad(self):
        inputFileJson = {}
        threadSet = []
        threadStat = []
        q = Queue()
        recordBlock = 0
        recordRemaining = 0
        # counter = atomicCounter()
        try:
            with open(self.inputFile, 'r') as inputFile:
                inputFileData = inputFile.read()
            inputFile.close()
        except OSError as e:
            print("Can not open input file: %s" % str(e))
            sys.exit(1)

        inputFileData = re.compile('ISODate\(("[^"]+")\)').sub('\\1', inputFileData)
        try:
            inputFileJson = json.loads(inputFileData)
        except Exception as e:
            print("Can not process json input file: %s" % str(e))
            sys.exit(1)

        for i in range(self.loadThreadCount):
            threadStat.append(0)
            threadSet.append(0)

        # self.asyncConnect()

        statusThread = threading.Thread(target=self.printStatusThread, args=(q, int(self.recordCount),))
        statusThread.start()

        recordRemaining = int(self.recordCount)
        recordBlock = round(recordRemaining / int(self.loadThreadCount))
        recordBlock = recordBlock if recordBlock >= 1 else 1
        recordStart = 1
        for x in range(self.loadThreadCount):
                if recordRemaining < recordBlock:
                    numRecords = recordRemaining
                elif recordRemaining > 0:
                    numRecords = recordBlock
                else:
                    break
                recordRemaining = recordRemaining - numRecords
                # q.put(self.currentOp)
                # digits = int(math.log10(recordBlock)) + 1
                threadSet[x] = threading.Thread(target=self.asyncInsert, args=(q, inputFileJson, numRecords, recordStart,))
                threadSet[x].start()
                recordStart = recordStart + numRecords

        for y in range(self.loadThreadCount):
            threadSet[y].join()

        q.put(0)
        statusThread.join()

    def kvTest(self, testKey):
        inputFileJson = {}
        threadSet = []
        q = Queue()

        try:
            with open(self.inputFile, 'r') as inputFile:
                inputFileData = inputFile.read()
            inputFile.close()
        except OSError as e:
            print("Can not open input file: %s" % str(e))
            sys.exit(1)

        inputFileData = re.compile('ISODate\(("[^"]+")\)').sub('\\1', inputFileData)
        try:
            inputFileJson = json.loads(inputFileData)
        except Exception as e:
            print("Can not process json input file: %s" % str(e))
            sys.exit(1)

        for i in range(self.runThreadCount):
            threadSet.append(0)

        statusThread = threading.Thread(target=self.printStatusThread, args=(q, int(self.recordCount),))
        statusThread.start()

        writeThreads = round((int(self.writePercent) / 100) * self.runThreadCount)
        readThreads = self.runThreadCount - writeThreads
        recordRemaining = int(self.operationCount)
        recordBlock = round(recordRemaining / int(self.runThreadCount))
        recordBlock = recordBlock if recordBlock >= 1 else 1
        recordStart = 1
        for x in range(self.runThreadCount):
                if recordRemaining < recordBlock:
                    numRecords = recordRemaining
                elif recordRemaining > 0:
                    numRecords = recordBlock
                else:
                    break
                recordRemaining = recordRemaining - numRecords
                if writeThreads > 0:
                    threadSet[x] = threading.Thread(target=self.asyncInsert, args=(q, inputFileJson, numRecords, recordStart,))
                    writeThreads -= 1
                else:
                    threadSet[x] = threading.Thread(target=self.asyncRead, args=(q, numRecords, recordStart,))
                threadSet[x].start()
                recordStart = recordStart + numRecords

        for y in range(self.runThreadCount):
            threadSet[y].join()

        q.put(0)
        statusThread.join()
        self.printStatusReset()

    def stdout_reader(self):
        for line in iter(self.p.stdout.readline, b''):
            self.out_queue.put(line.decode('utf-8'))

    def stderr_reader(self):
        for line in iter(self.p.stderr.readline, b''):
            self.err_queue.put(line.decode('utf-8'))

    def pfLoad(self):
        self.p = subprocess.Popen(['cbc-pillowfight',
                                   '-U', "couchbase://" + self.host + "/" + self.bucket,
                                   '-u', self.username,
                                   '-P', self.password,
                                   '-t', str(self.loadThreadCount),
                                   '-R',
                                   '-J',
                                   '-I', str(self.recordCount),
                                   '--populate-only'],
                                  stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        self.out_thread = threading.Thread(target=self.stdout_reader)
        self.err_thread = threading.Thread(target=self.stderr_reader)
        self.out_thread.start()
        self.err_thread.start()

    def pfRun(self):
        self.p = subprocess.Popen(['cbc-pillowfight',
                                   '-U', "couchbase://" + self.host + "/" + self.bucket,
                                   '-u', self.username,
                                   '-P', self.password,
                                   '-t', str(self.runThreadCount),
                                   '-R',
                                   '-r', str(self.writePercent),
                                   '-c', str(self.operationCount),
                                   '-J',
                                   '-I', str(self.recordCount),
                                   '-n'],
                                  stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        self.out_thread = threading.Thread(target=self.stdout_reader)
        self.err_thread = threading.Thread(target=self.stderr_reader)
        self.out_thread.start()
        self.err_thread.start()

    def end(self):
        # self.p.terminate()
        self.p.wait()
        self.out_thread.join()
        self.err_thread.join()

    def gatherStats(self, writeFile):
        record = 0
        total = 0
        maximum = 0
        rollingRecord = 0
        rollingTotal = 0
        startTime = time.time()
        while True:
            try:
                errline = self.err_queue.get(block=False)
                errlinestr = '{0}'.format(errline).strip()
                if errlinestr.startswith('OPS'):
                    opsec = errlinestr.split()[-1]
                    record = record + 1
                    rollingRecord = rollingRecord + 1
                    total = total + int(opsec)
                    rollingTotal = rollingTotal + int(opsec)
                    if int(opsec) > maximum:
                        maximum = int(opsec)
                    nowTime = time.time()
                    if nowTime - startTime >= 5:
                        print("Ops/sec: %d" % (int(rollingTotal) / int(rollingRecord)))
                        startTime = nowTime
                        rollingTotal = 0
                        rollingRecord = 0
            except Empty:
                poll = self.p.poll()
                if poll is not None:
                    break
                else:
                    pass
        average = total / record
        outString = "Average Ops/Sec: {:.0f}\n"
        writeFile.write(outString.format(average))
        outString = "Maximum Ops/Sec: {}\n"
        writeFile.write(outString.format(str(maximum)))

    def loadData(self):
        summaryFile = "data-load.dat"
        try:
            datFile = open(summaryFile, "w")
        except OSError as e:
            print("Can not open test summary file: %s" % str(e))
            sys.exit(1)
        print("Loading %s records into bucket %s" % (str(self.recordCount), str(self.bucket)))
        self.pfLoad()
        self.gatherStats(datFile)
        self.end()
        datFile.close()
        print("Load complete.")

    def runTest(self, scenario):
        summaryFile = "workload" + scenario + ".dat"
        operations = int(self.operationCount) * int(self.batchSize)
        try:
            datFile = open(summaryFile, "w")
        except OSError as e:
            print("Can not open test summary file: %s" % str(e))
            sys.exit(1)
        print("Running scenario \"%s\" with %s operations on bucket %s" % (str(scenario), str(operations), str(self.bucket)))
        self.pfRun()
        self.gatherStats(datFile)
        self.end()
        datFile.close()
        print("Run complete.")

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--user', action = 'store')
        parser.add_argument('--password', action = 'store')
        parser.add_argument('--bucket', action='store')
        parser.add_argument('--host', action='store')
        parser.add_argument('--count', action='store')
        parser.add_argument('--ops', action='store')
        parser.add_argument('--tload', action='store')
        parser.add_argument('--trun', action='store')
        parser.add_argument('--memquota', action='store')
        parser.add_argument('--workload', action='store')
        parser.add_argument('--file', action='store')
        parser.add_argument('--batch', action='store')
        parser.add_argument('--kv', action='store')
        parser.add_argument('--query', action='store')
        parser.add_argument('--manual', action='store_true')
        parser.add_argument('--load', action='store_true')
        parser.add_argument('--run', action='store_true')
        parser.add_argument('--makebucket', action='store_true')
        parser.add_argument('--dropbucket', action='store_true')
        self.args = parser.parse_args()
        self.username = self.args.user if self.args.user else "Administrator"
        self.password = self.args.password if self.args.password else "password"
        self.bucket = self.args.bucket if self.args.bucket else "pillowfight"
        self.host = self.args.host if self.args.host else "localhost"
        self.recordCount = self.args.count if self.args.count else 1000000
        self.operationCount = self.args.ops if self.args.ops else 100000
        self.loadThreadCount = self.args.tload if self.args.tload else 32
        self.runThreadCount = self.args.trun if self.args.trun else 32
        self.bucketMemory = self.args.memquota
        self.runWorkload = self.args.workload
        self.inputFile = self.args.file
        self.batchSize = self.args.batch if self.args.batch else 100
        self.keyList = self.args.kv if self.args.kv else "random"
        self.queryList = self.args.query
        self.manualMode = self.args.manual
        self.loadOnly = self.args.load
        self.runOnly = self.args.run
        self.makeBucketOnly = self.args.makebucket
        self.dropBucketOnly = self.args.dropbucket

def main():
    dynamicInventory()

if __name__ == '__main__':

    try:
        main()
    except SystemExit as e:
        if e.code == 0:
            os._exit(0)
        else:
            os._exit(e.code)
