#!/usr/bin/env python3

'''
Invoke Couchbase Pillow Fight
'''

import os
import sys
import argparse
import json
import re
from jinja2 import Template
import time
import asyncio
import acouchbase.cluster
import requests
from datetime import datetime
from statistics import mean
import warnings
import random
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
threadLock = threading.Lock()

class atomicCounter(object):
    def __init__(self):
        self.value = 0
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self.value += 1

class randomize(object):

    def __init__(self):
        self.streetNames = [
            'Main',
            'Church',
            'Liberty',
            'Park',
            'Prospect',
            'Pine',
            'River',
            'Elm',
            'High',
            'Union',
            'Willow',
            'Dogwood',
            'New',
            'North',
            'South',
            'East',
            'West',
            '1st',
            '2nd',
            '3rd',
        ]
        self.streetNameLast = len(self.streetNames) - 1
        self.streetTypes = [
            'Street',
            'Road',
            'Lane',
            'Court',
            'Avenue',
            'Parkway',
            'Trail',
            'Way',
            'Drive',
        ]
        self.streetTypeLast = len(self.streetTypes) - 1
        self.cityNames = [
            'Mannorburg',
            'New Highworth',
            'Salttown',
            'Farmingchester',
            'East Sagepool',
            'Strongdol',
            'Weirton',
            'Hapwich',
            'Lunfield Park',
            'Cruxbury',
            'Oakport',
            'Chatham',
            'Beachborough',
            'Farmingbury Falls',
            'Trinsdale',
            'Wingview',
        ]
        self.cityNameLast = len(self.cityNames) - 1
        self.stateNames = [
            'AL',
            'AK',
            'AZ',
            'AR',
            'CA',
            'CZ',
            'CO',
            'CT',
            'DE',
            'DC',
            'FL',
            'GA',
            'GU',
            'HI',
            'ID',
            'IL',
            'IN',
            'IA',
            'KS',
            'KY',
            'LA',
            'ME',
            'MD',
            'MA',
            'MI',
            'MN',
            'MS',
            'MO',
            'MT',
            'NE',
            'NV',
            'NH',
            'NJ',
            'NM',
            'NY',
            'NC',
            'ND',
            'OH',
            'OK',
            'OR',
            'PA',
            'PR',
            'RI',
            'SC',
            'SD',
            'TN',
            'TX',
            'UT',
            'VT',
            'VI',
            'VA',
            'WA',
            'WV',
            'WI',
            'WY',
        ]
        self.stateNameLast = len(self.streetNames) - 1
        self.firstNameList = [
            'James',
            'Robert',
            'John',
            'Michael',
            'William',
            'David',
            'Richard',
            'Joseph',
            'Thomas',
            'Charles',
            'Mary',
            'Patricia',
            'Jennifer',
            'Linda',
            'Elizabeth',
            'Barbara',
            'Susan',
            'Jessica',
            'Sarah',
            'Karen',
        ]
        self.firstNameLast = len(self.firstNameList) - 1
        self.lastNameList = [
            'Smith',
            'Johnson',
            'Williams',
            'Brown',
            'Jones',
            'Garcia',
            'Miller',
            'Davis',
            'Rodriguez',
            'Martinez',
            'Hernandez',
            'Lopez',
            'Gonzalez',
            'Wilson',
            'Anderson',
            'Thomas',
            'Taylor',
            'Moore',
            'Jackson',
            'Martin',
        ]
        self.lastNameLast = len(self.lastNameList) - 1
        self.nowTime = datetime.now()
        self.datetimestr = self.nowTime.strftime("%Y-%m-%d %H:%M:%S")

    def _randomNumber(self, n):
        min_lc = ord(b'0')
        len_lc = 10
        ba = bytearray(random.getrandbits(8) for i in range(n))
        for i, b in enumerate(ba):
            ba[i] = min_lc + b % len_lc
        return ba.decode('utf-8')

    def _randomStringLower(self, n):
        min_lc = ord(b'a')
        len_lc = 26
        ba = bytearray(random.getrandbits(8) for i in range(n))
        for i, b in enumerate(ba):
            ba[i] = min_lc + b % len_lc
        return ba.decode('utf-8')

    def _randomStringUpper(self, n):
        min_lc = ord(b'A')
        len_lc = 26
        ba = bytearray(random.getrandbits(8) for i in range(n))
        for i, b in enumerate(ba):
            ba[i] = min_lc + b % len_lc
        return ba.decode('utf-8')

    def _randomHash(self, n):
        ba = bytearray(random.getrandbits(8) for i in range(n))
        for i, b in enumerate(ba):
            min_lc = ord(b'0') if b < 85 else ord(b'A') if b < 170 else ord(b'a')
            len_lc = 10 if b < 85 else 26
            ba[i] = min_lc + b % len_lc
        return ba.decode('utf-8')

    def creditCard(self):
        return self._randomNumber(4) + '-' + self._randomNumber(4) + '-' + self._randomNumber(4) + '-' + self._randomNumber(4)

    def socialSecurityNumber(self):
        return self._randomNumber(3) + '-' + self._randomNumber(2) + '-' + self._randomNumber(4)

    def threeDigits(self):
        return self._randomNumber(3)

    def fourDigits(self):
        return self._randomNumber(4)

    def zipCode(self):
        return self._randomNumber(5)

    def accountNumner(self):
        return self._randomNumber(10)

    def numericSequence(self):
        return self._randomNumber(16)

    def dollarAmount(self):
        value = random.getrandbits(8) % 5 + 1
        return self._randomNumber(value) + '.' + self._randomNumber(2)

    def hashCode(self):
        return self._randomHash(16)

    def firstName(self):
        value = random.getrandbits(8) % self.firstNameLast
        return self.firstNameList[value]

    def lastName(self):
        value = random.getrandbits(8) % self.lastNameLast
        return self.lastNameList[value]

    def addressLine(self):
        first_value = random.getrandbits(8) % self.streetNameLast
        second_value = random.getrandbits(8) % self.streetTypeLast
        return self._randomNumber(4) + ' ' + self.streetNames[first_value] + ' ' + self.streetTypes[second_value]

    def cityName(self):
        value = random.getrandbits(8) % self.cityNameLast
        return self.cityNames[value]

    def stateName(self):
        value = random.getrandbits(8) % self.stateNameLast
        return self.stateNames[value]

    def dateCode(self):
        return self.datetimestr

    def prepareTemplate(self, json_block):
        self.template = json.dumps(json_block)
        self.compiled = Template(self.template)

    def processTemplate(self):
        creditcard = self.creditCard()
        ssn = self.socialSecurityNumber()
        randfour = self.fourDigits()
        zipcode = self.zipCode()
        randaccount = self.accountNumner()
        randdollar = self.dollarAmount()
        randid = self.numericSequence()
        randhash = self.hashCode()
        randaddress = self.addressLine()
        randcity = self.cityName()
        randstate = self.stateName()
        randfirst = self.firstName()
        randlast = self.lastName()
        randdate = self.dateCode()

        # template = json.dumps(json_block)
        # t = Template(template)
        formattedBlock = self.compiled.render(date_time=randdate, credit_card=creditcard, social=ssn, rand_four=randfour,
                                  rand_account=randaccount, rand_id=randid, zip_code=zipcode, rand_dollar=randdollar,
                                  rand_hash=randhash, rand_address=randaddress, rand_city=randcity, rand_state=randstate,
                                  rand_first=randfirst, rand_last=randlast)
        finished = formattedBlock.encode('ascii')
        jsonBlock = json.loads(finished)
        return jsonBlock

class runPerformanceBenchmark(object):

    def __init__(self):
        self.out_thread = None
        self.err_thread = None
        # self.out_queue = Queue()
        # self.err_queue = Queue()
        self.cpu_count = os.cpu_count()
        self.randomize_queue = Queue()
        self.randomize_control = Queue()
        self.telemetry_queue = Queue()
        self.telemetry_control = Queue()
        self.randomize_thread_count = round(self.cpu_count / 2)
        self.randomize_num_generated = 0
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
            if not self.inputFile:
                print("Please provide a source JSON file with the file parameter.")
                sys.exit(1)
            self.dataLoad()
            if self.loadOnly:
                sys.exit(0)

        if not self.manualMode or self.runOnly:
            if not self.inputFile:
                print("Please provide a source JSON file with the file parameter.")
                sys.exit(1)
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

    def dataConnect(self):
        try:
            cluster = Cluster("http://" + self.host + ":8091", authenticator=self.auth, lockmode=LOCKMODE_NONE)
            bucket = cluster._cluster.open_bucket(self.bucket, lockmode=LOCKMODE_NONE)
            return cluster, bucket
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

    def documentInsert(self, numRecords, startNum, thread):
        # r = randomize()
        randomize_retry = 5
        # q = self.randomize_queue
        # loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(loop)
        telemetry = {
            'tps': 0,
            'ops': 0,
            'time': 0,
            'thread': thread,
        }
        try:
            cluster, bucket = self.dataConnect()
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
            batch = ItemOptionDict()
            for y in range(int(runBatchSize)):
                while True:
                    try:
                        runJsonDoc = self.randomize_queue.get()
                        break
                    except Empty:
                        if randomize_retry == 0:
                            print("Timeout waiting for transformed JSON document.")
                            sys.exit(1)
                        randomize_retry -= 1
                        time.sleep(0.05)
                        pass
                randomize_retry = 5
                item = Item(str(format(counter, '032')), runJsonDoc)
                counter += 1
                # batch = ItemOptionDict()
                batch.add(item)
            begin_time = time.perf_counter()
            try:
                collection.upsert_multi(batch)
            except Exception as e:
                print("Error inserting into couchbase: %s" % str(e))
                sys.exit(1)
            end_time = time.perf_counter()
            time_delta = end_time - begin_time
            transPerSec = runBatchSize / time_delta
            telemetry['tps'] = transPerSec
            telemetry['ops'] = runBatchSize
            telemetry['time'] = time_delta
            self.telemetry_queue.put(telemetry)
        # loop.close()

    def asyncRead(self, q, numRecords, startNum, thread):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        telemetry = {
            'tps': 0,
            'ops': 0,
            'time': 0,
            'thread': thread,
        }
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
            batch = []
            for y in range(int(runBatchSize)):
                batch.append(str(format(counter, '032')))
                counter += 1
            begin_time = time.perf_counter()
            loop.run_until_complete(collection.get_multi(batch))
            end_time = time.perf_counter()
            time_delta = end_time - begin_time
            transPerSec = runBatchSize / time_delta
            telemetry['tps'] = transPerSec
            telemetry['ops'] = runBatchSize
            telemetry['time'] = time_delta
            q.put(telemetry)
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

    def printStatusThread(self, count, threads):
        threadVector = [0 for i in range(threads)]
        totalTps = 0
        averageTps = 0
        maxTps = 0
        totalTime = 0
        averageTime = 0
        maxTime = 0
        sampleCount = 1
        time_per_record = 0
        trans_per_sec = 0
        debug_string = ""

        while True:
            while not self.telemetry_queue.empty():
                entry = self.telemetry_queue.get()
                # print(json.dumps(entry))
                if 'control' in entry:
                    if entry['control'] == 0:
                        sys.stdout.write("\rOperation Complete.\033[K\n")
                        print("%d average TPS." % averageTps)
                        print("%d maximum TPS." % maxTps)
                        print("%.6f average time." % averageTime)
                        print("%.6f maximum time." % maxTime)
                        return
                if 'randomize' in entry:
                    debug_string = 'Debug: rand queue %d created %d' % (entry['queue'], entry['created'])
                    continue
                self.currentOp += entry['ops']
                time_delta = entry['time']
                reporting_thread = entry['thread']
                threadVector[reporting_thread] = entry['tps']
                # trans_per_sec = sum(threadVector)
                if all([v != 0 for v in threadVector]):
                    trans_per_sec = mean(threadVector) * len(threadVector)
                    totalTps = totalTps + trans_per_sec
                    time_per_record = time_delta / entry['ops']
                    totalTime = totalTime + time_per_record
                    averageTps = totalTps / sampleCount
                    averageTime = totalTime / sampleCount
                    sampleCount += 1
                    if trans_per_sec > maxTps:
                        maxTps = trans_per_sec
                    if time_per_record > maxTime:
                        maxTime = time_per_record
                self.percentage = (self.currentOp / count) * 100
                end_char = '\r'
                print("Document %d of %d in progress, %.6f time, %.0f TPS, %d%% completed %s" %
                      (self.currentOp, count, time_per_record, trans_per_sec, self.percentage, debug_string), end=end_char)
            time.sleep(1)

    def printStatusReset(self):
        self.currentOp = 0
        self.percentage = 0

    def randomizeThread(self, thread_num, json_block, count):
        retries = 1
        thread = int(thread_num) + 1
        telemetry = {
            'randomize': 1,
            'queue': 0,
            'created': 0,
            'thread': thread,
        }

        try:
            r = randomize()
            r.prepareTemplate(json_block)
        except Exception as e:
            print("Can not load JSON template: %s." % str(e))
            sys.exit(1)

        if self.debug:
            print("Randomize thread %d starting." % thread)
        while self.randomize_num_generated < count:
            if not self.randomize_control.empty():
                telemetry = self.randomize_control.get()
                if 'control' in telemetry:
                    if telemetry['control'] == 0:
                        if self.debug:
                            print("Randomize thread %d shutdown requested. Current queue size is %d." % (thread,
                                                                                                         self.randomize_queue.qsize()))
                        return
            while self.randomize_queue.qsize() > 1000:
                if self.debug:
                    print("Reached randomize limit, sleeping.")
                time.sleep(0.05 * float(retries))
                retries += 1
            try:
                jsonDoc = r.processTemplate()
            except Exception as e:
                print("Can not randomize JSON template: %s." % str(e))
                sys.exit(1)
            self.randomize_queue.put(jsonDoc)
            with threadLock:
                self.randomize_num_generated += 1
            # print(self.randomize_queue.qsize())
            retries = 1
            if self.debug:
                telemetry['queue'] = self.randomize_queue.qsize()
                telemetry['created'] = self.randomize_num_generated
                self.telemetry_queue.put(telemetry)

        if self.debug:
            print("Randomize thread %d complete. Exiting." % thread)

    def dataLoad(self):
        inputFileJson = {}
        threadSet = []
        threadStat = []
        templateThread = []
        q = Queue()
        # genq = Queue()
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

        # inputFileData = re.compile('ISODate\(("[^"]+")\)').sub('\\1', inputFileData)

        try:
            inputFileJson = json.loads(inputFileData)
        except Exception as e:
            print("Can not process json input file: %s" % str(e))
            sys.exit(1)

        for i in range(self.loadThreadCount):
            threadStat.append(0)
            threadSet.append(0)

        # self.asyncConnect()

        statusThread = threading.Thread(target=self.printStatusThread, args=(int(self.recordCount), int(self.loadThreadCount),))
        statusThread.start()
        for randomize_thread in range(self.randomize_thread_count):
            templateThreadRun = threading.Thread(target=self.randomizeThread, args=(randomize_thread, inputFileJson, int(self.recordCount),))
            templateThreadRun.start()
            templateThread.append(templateThreadRun)
        print("Starting data load with %s records" % '{:,}'.format(self.recordCount))
        start_time = time.perf_counter()

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
                threadSet[x] = threading.Thread(target=self.documentInsert, args=(numRecords, recordStart, x,))
                threadSet[x].start()
                recordStart = recordStart + numRecords

        for y in range(self.loadThreadCount):
            threadSet[y].join()

        end_time = time.perf_counter()
        telemetry = {'control': 0}
        self.telemetry_queue.put(telemetry)
        statusThread.join()
        for randomize_thread in range(self.randomize_thread_count):
            self.randomize_control.put(telemetry)
            templateThread[randomize_thread].join()
        print("Load completed in %s" % time.strftime("%H hours %M minutes %S seconds.", time.gmtime(end_time - start_time)))
        self.printStatusReset()

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

        statusThread = threading.Thread(target=self.printStatusThread, args=(q, int(self.operationCount), int(self.runThreadCount),))
        statusThread.start()
        # templateThread = threading.Thread(target=self.templateThread, args=(inputFileJson, int(self.recordCount) / int(self.batchSize), genq,))
        # templateThread.start()
        print("Starting KV test using scenario \"%s\" with %s records - %d%% get, %d%% update"
              % (testKey, '{:,}'.format(self.operationCount), 100 - self.writePercent, self.writePercent))
        start_time = time.perf_counter()

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
                    threadSet[x] = threading.Thread(target=self.asyncInsert, args=(q, inputFileJson, numRecords, recordStart, x,))
                    writeThreads -= 1
                else:
                    threadSet[x] = threading.Thread(target=self.asyncRead, args=(q, numRecords, recordStart, x,))
                threadSet[x].start()
                recordStart = recordStart + numRecords

        for y in range(self.runThreadCount):
            threadSet[y].join()

        end_time = time.perf_counter()
        telemetry = {'control': 0}
        q.put(telemetry)
        statusThread.join()
        # templateThread.join()
        print("Test completed in %s" % time.strftime("%H hours %M minutes %S seconds.", time.gmtime(end_time - start_time)))
        self.printStatusReset()

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
        parser.add_argument('--debug', action='store_true')
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
        self.debug = self.args.debug

def main():
    runPerformanceBenchmark()

if __name__ == '__main__':

    try:
        main()
    except SystemExit as e:
        if e.code == 0:
            os._exit(0)
        else:
            os._exit(e.code)
