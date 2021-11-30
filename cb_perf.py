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
from datetime import datetime, timedelta
from statistics import mean
import warnings
import random
import couchbase
from couchbase_core.items import Item, ItemOptionDict
from couchbase_core._libcouchbase import LOCKMODE_EXC, LOCKMODE_NONE, LOCKMODE_WAIT
from couchbase.cluster import Cluster, ClusterOptions, QueryOptions, ClusterTimeoutOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import QueryOptions
from couchbase.cluster import QueryIndexManager
from couchbase.management.buckets import CreateBucketSettings
from couchbase.exceptions import BucketNotFoundException
from couchbase.exceptions import CouchbaseException
from couchbase.exceptions import NetworkException
from couchbase.exceptions import SDKException
from couchbase.exceptions import CouchbaseTransientException
from couchbase.exceptions import CouchbaseFatalException
from couchbase.exceptions import ParsingFailedException

# try:
#     from Queue import Queue, Empty, PriorityQueue
# except ImportError:
#     from queue import Queue, Empty, PriorityQueue
import threading
import multiprocessing
from queue import Empty
# threadLock = threading.Lock()
threadLock = multiprocessing.Lock()

warnings.filterwarnings("ignore")

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

    def phoneNumber(self):
        return self._randomNumber(3) + '-' + self._randomNumber(3) + '-' + self._randomNumber(4)

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
        randphone = self.phoneNumber()
        randdate = self.dateCode()

        formattedBlock = self.compiled.render(date_time=randdate, credit_card=creditcard, social=ssn, rand_four=randfour,
                                  rand_account=randaccount, rand_id=randid, zip_code=zipcode, rand_dollar=randdollar,
                                  rand_hash=randhash, rand_address=randaddress, rand_city=randcity, rand_state=randstate,
                                  rand_first=randfirst, rand_last=randlast, rand_phone=randphone)
        finished = formattedBlock.encode('ascii')
        jsonBlock = json.loads(finished)
        return jsonBlock

class debugOutput(object):

    def __init__(self):
        self.threads = {}
        try:
            self.statDebug = open("stats.debug", 'w')
            self.telemetryDebug = open("telemetry.debug", 'w')
        except Exception as e:
            print("Debug: can not open debug files: %s" % str(e))
            sys.exit(1)

    def threadSet(self, thread):
        fileDesc = "query" + str(thread)
        fileName = fileDesc + ".debug"
        try:
            self.threads[fileDesc] = open(fileName, 'w')
        except Exception as e:
            print("Debug: can not open debug file stats.debug: %s" % str(e))
            sys.exit(1)

    def writeStatDebug(self, text):
        try:
            self.statDebug.write(str(text) + "\n")
        except Exception as e:
            print("writeStatDebug: can not write to file: %s" % str(e))
            sys.exit(1)

    def writeTelemetryDebug(self, blob):
        try:
            # json.dump(blob, self.telemetryDebug)
            self.telemetryDebug.write(str(blob) + "\n")
        except Exception as e:
            print("writeTelemetryDebug: can not write to file: %s" % str(e))
            sys.exit(1)

    def writeQueryDebug(self, blob, thread):
        fileDesc = "query" + str(thread)
        try:
            # json.dump(blob, self.threads[fileDesc])
            self.threads[fileDesc].write(str(blob) + "\n")
        except Exception as e:
            print("writeQueryDebug: can not write to file: %s" % str(e))
            sys.exit(1)

class mpAtomicCounter(object):

    def __init__(self, i=0):
        self.count = multiprocessing.Value('i', i)

    def increment(self, i=1):
        with self.count.get_lock():
            self.count.value += i

    @property
    def value(self):
        return self.count.value

class runPerformanceBenchmark(object):

    def __init__(self):
        self.out_thread = None
        self.err_thread = None
        # self.out_queue = Queue()
        # self.err_queue = Queue()
        self.cpu_count = os.cpu_count()
        self.randomize_queue = multiprocessing.Queue()
        # self.randomize_control = Queue()
        self.telemetry_queue = multiprocessing.Queue()
        # self.telemetry_queue = {}
        # self.telemetry_control = Queue()
        self.randomize_thread_count = round(self.cpu_count / 2)
        self.randomize_num_generated = mpAtomicCounter(0)
        self.randomize_queue_size = mpAtomicCounter(0)
        # self.counterLock = threading.Lock()
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
        # self.clusterConnect()
        self.fieldIndex = self.bucket + '_ix1'
        self.idIndex = self.bucket + '_id_ix1'
        self.keyArray = []

        if not self.bucketMemory:
            self.getMemQuota()

        if not self.manualMode or self.makeBucketOnly:
            print("Creating bucket %s." % self.bucket)
            self.createBucket()
            if self.makeBucketOnly:
                sys.exit(0)

        if not self.manualMode or self.loadOnly:
            print("Beginning data load into bucket %s." % self.bucket)
            if not self.inputFile:
                print("Please provide a source JSON file with the file parameter.")
                sys.exit(1)
            if not self._bucketExists(self.bucket):
                self.createBucket()
            self.dataLoad()
            if self.loadOnly:
                sys.exit(0)

        if not self.manualMode or self.createIndexFlag:
            if not self.queryField:
                print("Create index: Query field is required.")
                sys.exit(1)
            if not self.idField:
                print("Create index: ID field is required.")
                sys.exit(1)
            print("Creating indexes on query field \"%s\" and ID field \"%s\"." % (self.queryField, self.idField))
            self.createIndex(self.queryField, self.fieldIndex)
            self.createIndex(self.idField, self.idIndex)
            if self.createIndexFlag:
                sys.exit(0)

        if ((not self.manualMode and self.queryField) or (self.queryField and self.runOnly)) and not self.kvOnly:
            print("Beginning N1QL tests.")
            if not self._indexExists(self.fieldIndex) or not self._indexExists(self.idIndex):
                print("Creating indexes on query field \"%s\" and ID field \"%s\"." % (self.queryField, self.idField))
                self.createIndex(self.queryField, self.fieldIndex)
                self.createIndex(self.idField, self.idIndex)
            for key in self.scenarioWrite:
                if self.runWorkload:
                    if self.runWorkload != key:
                        continue
                self.writePercent = self.scenarioWrite[key]
                self.queryTest(key)

        if not self.manualMode or self.runOnly:
            print("Beginning KV tests.")
            if not self.inputFile:
                print("Please provide a source JSON file with the file parameter.")
                sys.exit(1)
            if not self._bucketExists(self.bucket):
                print("Please create the pillowfight bucket first.")
                sys.exit(1)
            for key in self.scenarioWrite:
                if self.runWorkload:
                    if self.runWorkload != key:
                        continue
                self.writePercent = self.scenarioWrite[key]
                self.kvTest(key)
            if self.runOnly:
                sys.exit(0)

        if not self.manualMode or self.dropIndexFlag:
            print("Dropping indexes.")
            self.dropIndex(self.fieldIndex)
            self.dropIndex(self.idIndex)
            if self.dropIndexFlag:
                sys.exit(0)

        if not self.manualMode or self.dropBucketOnly:
            print("Deleting bucket %s." % self.bucket)
            self.deleteBucket()

    def clusterConnect(self):
        try:
            auth = PasswordAuthenticator(self.username, self.password)
            cluster = Cluster("http://" + self.host + ":8091", authenticator=auth, lockmode=LOCKMODE_NONE)
            bm = cluster.buckets()
            qim = QueryIndexManager(cluster)
            return cluster, bm, qim
        except Exception as e:
            print("Could not connect to couchbase: %s" % str(e))
            sys.exit(1)

    def _bucketExists(self, bucket):
        try:
            cluster, bm, qim = self.clusterConnect()
            bucket = cluster.bucket(bucket)
            return True
        except BucketNotFoundException as e:
            return False
        except Exception as e:
            print("bucketExists: Could not get bucket status: %s" % str(e))
            sys.exit(1)

    def _indexExists(self, index):
        try:
            cluster, bm, qim = self.clusterConnect()
            indexList = qim.get_all_indexes(self.bucket)
            for i in range(len(indexList)):
                if indexList[i].name == index:
                    return True
        except Exception as e:
            print("Could not get index status: %s" % str(e))
            sys.exit(1)

        return False

    def _bucketRetry(self, bucket, limit=5):
        for i in range(limit):
            try:
                time.sleep(1)
                cluster, bm, qim = self.clusterConnect()
                return bm.get_bucket(bucket)
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

    async def dataConnect(self):
        auth = PasswordAuthenticator(self.username, self.password)
        timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=1200), kv_timeout=timedelta(seconds=1200))
        try:
            cluster = acouchbase.cluster.Cluster("http://" + self.host + ":8091", authenticator=auth, lockmode=LOCKMODE_NONE, timeout_options=timeouts)
            bucket = cluster.bucket(self.bucket)
            await cluster.on_connect()
            return cluster, bucket
        except Exception as e:
            print("Can not connect to cluster: %s" % str(e))
            sys.exit(1)

    def cb_connect_s(self):
        auth = PasswordAuthenticator(self.username, self.password)
        try:
            cluster = Cluster("http://" + self.host + ":8091", authenticator=auth, lockmode=2)
            bucket = cluster.bucket(self.bucket)
            return cluster, bucket
        except Exception as e:
            print("Can not connect to cluster: %s" % str(e))
            sys.exit(1)

    def createBucket(self):
        if not self._bucketExists(self.bucket):
            try:
                cluster, bm, qim = self.clusterConnect()
                bm.create_bucket(CreateBucketSettings(name=self.bucket, bucket_type="couchbase", ram_quota_mb=self.bucketMemory))
                self._bucketRetry(self.bucket)
            except Exception as e:
                print("Could not create bucket: %s" % str(e))
                sys.exit(1)

    def deleteBucket(self):
        if self._bucketExists(self.bucket):
            try:
                cluster, bm, qim = self.clusterConnect()
                bm.drop_bucket(self.bucket)
            except Exception as e:
                print("Could not drop bucket: %s" % str(e))
                sys.exit(1)

    def createIndex(self, field, index):
        if self.debug:
            print("createIndex: creating index %s on field %s." % (index, field))
        queryText = 'CREATE INDEX ' + index + ' ON pillowfight(' + field + ') WITH {"num_replica": 1};'
        if self._bucketExists(self.bucket) and not self._indexExists(index):
            try:
                cluster, bucket = self.cb_connect_s()
                collection = bucket.default_collection()
            except Exception as e:
                print("createIndex: error connecting to couchbase: %s" % str(e))
                sys.exit(1)

            try:
                result = collection.result = cluster.query(queryText, QueryOptions(metrics=True))
            except CouchbaseException as e:
                print("Error running query: %s" % str(e))
                sys.exit(1)

            run_time = result.metadata().metrics().execution_time().microseconds
            run_time = run_time / 1000000
            print("Create index \"%s\" on field \"%s\" execution time: %f secs" % (index, field, run_time))

    def dropIndex(self, index):
        if self.debug:
            print("Dropping index %s.", index)
        queryText = 'DROP INDEX ' + index + ' ON pillowfight USING GSI;'
        if self._bucketExists(self.bucket) and self._indexExists(index):
            try:
                cluster, bucket = self.cb_connect_s()
                collection = bucket.default_collection()
            except Exception as e:
                print("createIndex: error connecting to couchbase: %s" % str(e))
                sys.exit(1)

            try:
                result = collection.result = cluster.query(queryText, QueryOptions(metrics=True))
            except CouchbaseException as e:
                print("Error running query: %s" % str(e))
                sys.exit(1)

            run_time = result.metadata().metrics().execution_time().microseconds
            run_time = run_time / 1000000
            print("Drop index \"%s\" execution time: %f secs" % (index, run_time))

    async def cb_query(self, cluster, query):
        contents = []
        retries = 0
        while True:
            try:
                result = cluster.query(query, QueryOptions(metrics=False, adhoc=False, pipeline_batch=128, max_parallelism=4, pipeline_cap=1024, scan_cap=1024))
                async for row in result:
                    contents.append(row)
                if len(contents) > 0:
                    return contents
                else:
                    return True
            except ParsingFailedException as e:
                print("Query syntax error: %s", str(e))
                sys.exit(1)
            except Exception as e:
                if retries == self.maxRetries:
                    raise Exception("cb_query SDK error: %s" % str(e))
                else:
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

    async def cb_upsert(self, collection, key, document):
        retries = 0
        while True:
            try:
                result = await collection.upsert(key, document)
                return result.cas
            except Exception as e:
                if retries == self.maxRetries:
                    raise Exception("cb_upsert SDK error: %s" % str(e))
                else:
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

    def cb_upsert_s(self, collection, key, document):
        try:
            result = collection.upsert(key, document)
            return result.cas
        except CouchbaseException as e:
            print("Query error: %s", str(e))
            return False

    async def cb_get(self, collection, key):
        retries = 0
        while True:
            try:
                result = await collection.get(key)
                return result.content_as[dict]
            except Exception as e:
                if retries == self.maxRetries:
                    raise Exception("cb_get SDK error: %s" % str(e))
                else:
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

    def cb_get_s(self, collection, key):
        try:
            result = collection.get(key)
            return result.content_as[dict]
        except CouchbaseException as e:
            print("Query error: %s", str(e))
            return False

    def documentInsert(self, numRecords, startNum, thread, randomFlag=False):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        randomize_retry = 0
        loop_average_tps = 0
        loop_average_time = 0
        loop_total_time = 0
        runJsonDoc = {}
        retries = 1
        telemetry = [0 for n in range(3)]

        try:
            cluster, bucket = loop.run_until_complete(self.dataConnect())
            #     cluster, bucket = self.cb_connect_s()
            collection = bucket.default_collection()
        except Exception as e:
            print("documentInsert: error connecting to couchbase: %s" % str(e))
            sys.exit(1)

        if self.debug:
            print("Insert thread %d connected to cluster, starting test with %d records." % (thread, numRecords))

        numRemaining = numRecords
        counter = int(startNum)
        while numRemaining > 0:
            if numRemaining <= self.batchSize:
                runBatchSize = numRemaining
            else:
                runBatchSize = self.batchSize
            numRemaining = numRemaining - runBatchSize
            loop_total_time = 0
            tasks = []
            begin_time = time.time()
            for y in range(int(runBatchSize)):
                try:
                    runJsonDoc = self.randomize_queue.get()
                    self.randomize_queue_size.increment(-1)
                except Exception as e:
                    print("Error getting JSON document from queue: %s." % str(e))
                    sys.exit(1)
                if randomFlag:
                    run_key = random.getrandbits(8) % numRecords
                    run_key += startNum
                else:
                    run_key = counter
                record_id = str(format(run_key, '032'))
                runJsonDoc[self.idField] = record_id
                tasks.append(self.cb_upsert(collection, record_id, runJsonDoc))
                counter += 1
            try:
                result = loop.run_until_complete(asyncio.gather(*tasks))
            except Exception as e:
                print("documentInsert: %s" % str(e))
                sys.exit(1)
            end_time = time.time()
            loop_total_time = end_time - begin_time
            telemetry[0] = thread
            telemetry[1] = runBatchSize
            telemetry[2] = loop_total_time
            telemetry_packet = ':'.join(str(i) for i in telemetry)
            self.telemetry_queue.put(telemetry_packet)

        loop.close()
        if self.debug:
            print("Insert thread %d complete, exiting." % thread)

    def documentRead(self, numRecords, startNum, thread, randomFlag=False):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop_average_tps = 0
        loop_average_time = 0
        retries = 1
        telemetry = [0 for n in range(3)]
        loop_total_time = 0

        try:
            cluster, bucket = loop.run_until_complete(self.dataConnect())
            # cluster, bucket = self.cb_connect_s()
            collection = bucket.default_collection()
        except Exception as e:
            print("documentRead: error connecting to couchbase: %s" % str(e))
            sys.exit(1)

        if self.debug:
            print("Read thread %d connected, starting test for %d records starting at %d." % (thread, numRecords, startNum))

        numRemaining = numRecords
        counter = int(startNum)
        while numRemaining > 0:
            if numRemaining <= self.batchSize:
                runBatchSize = numRemaining
            else:
                runBatchSize = self.batchSize
            numRemaining = numRemaining - runBatchSize
            loop_total_time = 0
            tasks = []
            begin_time = time.time()
            for y in range(int(runBatchSize)):
                if randomFlag:
                    run_key = random.getrandbits(8) % numRecords
                    run_key += startNum
                else:
                    run_key = counter
                record_id = str(format(run_key, '032'))
                tasks.append(self.cb_get(collection, record_id))
                counter += 1
            try:
                result = loop.run_until_complete(asyncio.gather(*tasks))
            except Exception as e:
                print("documentRead: %s" % str(e))
                sys.exit(1)
            end_time = time.time()
            loop_total_time = end_time - begin_time
            telemetry[0] = thread
            telemetry[1] = runBatchSize
            telemetry[2] = loop_total_time
            telemetry_packet = ':'.join(str(i) for i in telemetry)
            self.telemetry_queue.put(telemetry_packet)

        loop.close()
        if self.debug:
            print("Read thread %d complete, exiting." % thread)

    def getFieldValueList(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        fieldName = self.queryField
        runQueryText = 'SELECT RAW ' + fieldName + ' FROM pillowfight WHERE ' + fieldName + ' GROUP BY ' + fieldName + ';'
        resultList = []

        print("Preparing to run query test. Retrieving list of field values.")

        try:
            cluster, bucket = loop.run_until_complete(self.dataConnect())
        except Exception as e:
            print("documentRead: error connecting to couchbase: %s" % str(e))
            sys.exit(1)

        start_time = time.perf_counter()
        try:
            resultList = loop.run_until_complete(self.cb_query(cluster, runQueryText))
        except CouchbaseException as e:
            print("Error running query: %s" % str(e))
            sys.exit(1)
        end_time = time.perf_counter()

        loop.close()
        time_delta = end_time - start_time
        item_count = len(resultList)
        print("Preparation complete, retrieved %d items in %.06f seconds." % (item_count, time_delta))
        return resultList

    def runReadQuery(self, fieldList=[], count=1, start_num=1, thread=0, template=None, randomFlag=False):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop_average_tps = 0
        loop_average_time = 0
        loop_total_time = 0
        result = None
        retries = 1
        error_count = 0
        fieldName = self.queryField
        idField = self.idField
        telemetry = [0 for n in range(3)]

        if len(fieldList) == 0:
            print("runReadQuery: list of field query values is null.")
            sys.exit(1)

        if self.debug:
            myDebug = debugOutput()
            myDebug.threadSet(thread)

        try:
            cluster, bucket = loop.run_until_complete(self.dataConnect())
            # collection = bucket.scope("_default").collection("_default")
        except Exception as e:
            print("documentRead: error connecting to couchbase: %s" % str(e))
            sys.exit(1)

        if self.debug:
            print("Query thread %d connected, starting query for %d iteration(s)." % (thread, count))

        numRemaining = count
        counter = int(start_num)
        while numRemaining > 0:
            if numRemaining <= self.batchSize:
                runBatchSize = numRemaining
            else:
                runBatchSize = self.batchSize
            numRemaining = numRemaining - runBatchSize
            loop_total_time = 0
            tasks = []
            begin_time = time.time()
            for y in range(int(runBatchSize)):
                resultSet = {}
                if randomFlag:
                    run_key = random.getrandbits(8) % count
                    run_key += start_num
                else:
                    run_key = counter
                record_id = str(format(run_key, '032'))
                readQueryText = 'SELECT ' + self.queryField + ' FROM pillowfight WHERE ' + self.idField + ' = "' + record_id + '";'
                tasks.append(self.cb_query(cluster, readQueryText))
                counter += 1
            try:
                result = loop.run_until_complete(asyncio.gather(*tasks))
            except Exception as e:
                print("runReadQuery: %s" % str(e))
                sys.exit(1)
            end_time = time.time()
            loop_total_time = end_time - begin_time
            telemetry[0] = thread
            telemetry[1] = runBatchSize
            telemetry[2] = loop_total_time
            telemetry_packet = ':'.join(str(i) for i in telemetry)
            self.telemetry_queue.put(telemetry_packet)
            if self.debug:
                myDebug.writeQueryDebug(telemetry, thread)

        loop.close()
        if self.debug:
            print("Query thread %d complete, exiting." % thread)

    def runUpdateQuery(self, fieldList=[], count=1, start_num=1, thread=0, template=None, randomFlag=False):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop_average_tps = 0
        loop_average_time = 0
        loop_total_time = 0
        result = None
        retries = 1
        error_count = 0
        fieldName = self.idField
        telemetry = [0 for n in range(3)]

        if len(fieldList) == 0:
            print("runReadQuery: list of field query values is null.")
            sys.exit(1)

        if self.debug:
            myDebug = debugOutput()
            myDebug.threadSet(thread)

        try:
            cluster, bucket = loop.run_until_complete(self.dataConnect())
            # collection = bucket.scope("_default").collection("_default")
        except Exception as e:
            print("documentRead: error connecting to couchbase: %s" % str(e))
            sys.exit(1)

        if self.debug:
            print("Query thread %d connected, starting query for %d iteration(s)." % (thread, count))

        numRemaining = count
        counter = int(start_num)
        while numRemaining > 0:
            if numRemaining <= self.batchSize:
                runBatchSize = numRemaining
            else:
                runBatchSize = self.batchSize
            numRemaining = numRemaining - runBatchSize
            loop_total_time = 0
            tasks = []
            begin_time = time.time()
            for y in range(int(runBatchSize)):
                resultSet = {}
                if randomFlag:
                    run_key = random.getrandbits(8) % count
                    run_key += start_num
                else:
                    run_key = counter
                value = random.getrandbits(8) % len(fieldList)
                record_id = str(format(run_key, '032'))
                updateQueryText = 'UPDATE pillowfight SET ' + self.queryField + ' = "' + fieldList[value] + '" WHERE ' + self.idField + ' = "' + record_id + '";'
                tasks.append(self.cb_query(cluster, updateQueryText))
                counter += 1
            try:
                result = loop.run_until_complete(asyncio.gather(*tasks))
            except Exception as e:
                print("runUpdateQuery: %s" % str(e))
                sys.exit(1)
            end_time = time.time()
            loop_total_time = end_time - begin_time
            telemetry[0] = thread
            telemetry[1] = runBatchSize
            telemetry[2] = loop_total_time
            telemetry_packet = ':'.join(str(i) for i in telemetry)
            self.telemetry_queue.put(telemetry_packet)
            if self.debug:
                myDebug.writeQueryDebug(telemetry, thread)

        loop.close()
        if self.debug:
            print("Query thread %d complete, exiting." % thread)

    def printStatusThread(self, count, threads):
        threadVector = [0 for i in range(threads)]
        totalTps = 0
        totalOps = 0
        entryOps = 0
        averageTps = 0
        maxTps = 0
        totalTime = 0
        averageTime = 0
        maxTime = 0
        sampleCount = 1
        time_per_record = 0
        trans_per_sec = 0
        start_shutdown = False
        debug_string = ""
        cycle = 1

        if self.debug:
            myDebug = debugOutput()

        while True:
            entry = self.telemetry_queue.get()
            telemetry = entry.split(":")
            if self.debug:
                myDebug.writeTelemetryDebug(entry)
            if int(telemetry[0]) < 256:
                entryOps = int(telemetry[1])
                time_delta = float(telemetry[2])
                reporting_thread = int(telemetry[0])
                threadVector[reporting_thread] = round(entryOps / time_delta)
                totalOps += entryOps
                trans_per_sec = sum(threadVector)
                op_time_delta = time_delta / entryOps
                totalTps = totalTps + trans_per_sec
                totalTime = totalTime + op_time_delta
                averageTps = totalTps / sampleCount
                averageTime = totalTime / sampleCount
                sampleCount += 1
                if trans_per_sec > maxTps:
                    maxTps = trans_per_sec
                if time_delta > maxTime:
                    maxTime = time_delta
                self.percentage = (totalOps / count) * 100
                if 'rss' in entry:
                    extra_string = "result count %d" % entry['rss']
                else:
                    extra_string = ""
                end_char = '\r'
                print("Operation %d of %d in progress, %.6f time, %d TPS, %d%% completed %s" %
                      (totalOps, count, op_time_delta, trans_per_sec, self.percentage, extra_string), end=end_char)
                # self.telemetry_queue.task_done()
                if self.debug:
                    text = "%d %d %d %d %d %.6f %d" % (reporting_thread, entryOps, totalOps, totalTps, averageTps, averageTime, sampleCount)
                    myDebug.writeStatDebug(text)
            if int(telemetry[0]) == 256:
                sys.stdout.write("\033[K")
                print("Operation %d of %d, %d%% complete." % (totalOps, count, self.percentage))
                print("Test Done.")
                print("%d average TPS." % averageTps)
                print("%d maximum TPS." % maxTps)
                print("%.6f average time." % averageTime)
                print("%.6f maximum time." % maxTime)
                return

    def runReset(self):
        self.currentOp = 0
        self.percentage = 0
        self.randomize_num_generated = mpAtomicCounter(0)
        self.randomize_queue_size = mpAtomicCounter(0)

    def randomizeThread(self, thread_num, json_block, count):
        retries = 1
        thread = int(thread_num) + 1
        max_queue_size = 0

        try:
            r = randomize()
            r.prepareTemplate(json_block)
        except Exception as e:
            print("Can not load JSON template: %s." % str(e))
            sys.exit(1)

        if self.debug:
            print("Randomize thread %d starting with count %d." % (thread, count))

        while self.randomize_num_generated.value < count:
            while self.randomize_queue_size.value >= 16384:
                time.sleep(0.01 * retries)
                retries += 1
            try:
                jsonDoc = r.processTemplate()
            except Exception as e:
                print("Can not randomize JSON template: %s." % str(e))
                sys.exit(1)
            self.randomize_queue.put(jsonDoc)
            self.randomize_num_generated.increment(1)
            self.randomize_queue_size.increment(1)
            if self.randomize_queue_size.value > max_queue_size:
                max_queue_size = self.randomize_queue_size.value
            retries = 1

        if self.debug:
            print("Randomize thread %d complete. Max queue size %d. Exiting." % (thread, max_queue_size))

    def dataLoad(self):
        inputFileJson = {}
        threadSet = []
        threadStat = []
        randomizeThread = []
        recordBlock = 0
        recordRemaining = 0
        telemetry = [0 for n in range(3)]
        # loop = asyncio.new_event_loop()

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

        # try:
        #     cluster, bucket = loop.run_until_complete(self.dataConnect())
            # cluster, bucket = self.cb_connect_s()
            # collection = bucket.default_collection()
        # except Exception as e:
        #     print("dataLoad: error connecting to couchbase: %s" % str(e))
        #     sys.exit(1)

        for i in range(self.loadThreadCount):
            threadStat.append(0)
            threadSet.append(0)

        statusThread = multiprocessing.Process(target=self.printStatusThread, args=(int(self.recordCount), int(self.loadThreadCount),))
        statusThread.daemon = True
        statusThread.start()
        for randomize_thread in range(self.randomize_thread_count):
            randomizeThreadRun = multiprocessing.Process(target=self.randomizeThread, args=(randomize_thread, inputFileJson, int(self.recordCount),))
            randomizeThreadRun.daemon = True
            randomizeThreadRun.start()
            randomizeThread.append(randomizeThreadRun)
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
                threadSet[x] = multiprocessing.Process(target=self.documentInsert, args=(numRecords, recordStart, x,))
                threadSet[x].daemon = True
                threadSet[x].start()
                recordStart = recordStart + numRecords

        for y in range(self.loadThreadCount):
            threadSet[y].join()

        end_time = time.perf_counter()
        telemetry[0] = 256
        telemetry_packet = ':'.join(str(i) for i in telemetry)
        self.telemetry_queue.put(telemetry_packet)
        statusThread.join()
        while True:
            try:
                toss = self.randomize_queue.get(block=False)
                self.randomize_queue_size.increment(-1)
            except Empty:
                break
        if self.debug:
            print("DEBUG: Randomize generated %d, queue size: %d" % (self.randomize_num_generated.value, self.randomize_queue_size.value))
        for randomize_thread in range(self.randomize_thread_count):
                randomizeThread[randomize_thread].join()
        print("Load completed in %s" % time.strftime("%H hours %M minutes %S seconds.", time.gmtime(end_time - start_time)))
        self.runReset()
        # loop.close()

    def kvTest(self, testKey):
        inputFileJson = {}
        threadSet = []
        randomizeThread = []
        randomFlag=self.randomFlag
        telemetry = [0 for n in range(3)]

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

        for i in range(self.runThreadCount):
            threadSet.append(0)

        writeThreads = round((int(self.writePercent) / 100) * self.runThreadCount)
        recordRemaining = int(self.operationCount)
        recordBlock = round(recordRemaining / int(self.runThreadCount))
        recordBlock = recordBlock if recordBlock >= 1 else 1
        recordStart = 1
        writeOperationCount = recordBlock * writeThreads

        statusThread = multiprocessing.Process(target=self.printStatusThread, args=(int(self.operationCount), int(self.runThreadCount),))
        statusThread.daemon = True
        statusThread.start()

        for randomize_thread in range(self.randomize_thread_count):
            randomizeThreadRun = multiprocessing.Process(target=self.randomizeThread, args=(randomize_thread, inputFileJson, int(writeOperationCount),))
            randomizeThreadRun.daemon = True
            randomizeThreadRun.start()
            randomizeThread.append(randomizeThreadRun)

        print("Starting KV test using scenario \"%s\" with %s records - %d%% get, %d%% update"
              % (testKey, '{:,}'.format(int(self.operationCount)), 100 - self.writePercent, self.writePercent))
        start_time = time.perf_counter()

        for x in range(self.runThreadCount):
                if recordRemaining < recordBlock:
                    numRecords = recordRemaining
                elif recordRemaining > 0:
                    numRecords = recordBlock
                else:
                    break
                recordRemaining = recordRemaining - numRecords
                if writeThreads > 0:
                    threadSet[x] = multiprocessing.Process(target=self.documentInsert, args=(numRecords, recordStart, x, randomFlag,))
                    writeThreads -= 1
                else:
                    threadSet[x] = multiprocessing.Process(target=self.documentRead, args=(numRecords, recordStart, x, randomFlag,))
                threadSet[x].daemon = True
                threadSet[x].start()
                recordStart = recordStart + numRecords

        for y in range(self.runThreadCount):
            threadSet[y].join()

        end_time = time.perf_counter()
        telemetry[0] = 256
        telemetry_packet = ':'.join(str(i) for i in telemetry)
        self.telemetry_queue.put(telemetry_packet)
        statusThread.join()
        while True:
            try:
                toss = self.randomize_queue.get(block=False)
                self.randomize_queue_size.increment(-1)
            except Empty:
                break
        if self.debug:
            print("DEBUG: Randomize generated %d, queue size: %d" % (self.randomize_num_generated.value, self.randomize_queue_size.value))
        for randomize_thread in range(self.randomize_thread_count):
            randomizeThread[randomize_thread].join()
        print("Test completed in %s" % time.strftime("%H hours %M minutes %S seconds.", time.gmtime(end_time - start_time)))
        self.runReset()

    def queryTest(self, testKey):
        inputFileJson = {}
        threadSet = []
        randomizeThread = []
        randomFlag=self.randomFlag
        telemetry = [0 for n in range(3)]
        # lock = threading.Lock()

        for i in range(int(self.runThreadCount)):
            threadSet.append(0)

        fieldList = self.getFieldValueList()

        writeThreads = round((int(self.writePercent) / 100) * int(self.runThreadCount))
        recordRemaining = int(self.operationCount)
        recordBlock = round(recordRemaining / int(self.runThreadCount))
        recordBlock = recordBlock if recordBlock >= 1 else 1
        recordStart = 1
        writeOperationCount = recordBlock * writeThreads

        statusThread = multiprocessing.Process(target=self.printStatusThread, args=(int(self.operationCount), int(self.runThreadCount),))
        statusThread.daemon = True
        statusThread.start()

        for randomize_thread in range(self.randomize_thread_count):
            randomizeThreadRun = multiprocessing.Process(target=self.randomizeThread, args=(randomize_thread, inputFileJson, int(writeOperationCount),))
            randomizeThreadRun.daemon = True
            randomizeThreadRun.start()
            randomizeThread.append(randomizeThreadRun)

        print("Starting query test using scenario \"%s\" with %s records - %d%% select, %d%% update"
              % (testKey, '{:,}'.format(int(self.operationCount)), 100 - self.writePercent, self.writePercent))
        if self.debug:
            print("queryTest: Using %d threads, %d write threads." % (int(self.runThreadCount), writeThreads))
        start_time = time.perf_counter()

        for x in range(self.runThreadCount):
                if recordRemaining < recordBlock:
                    numRecords = recordRemaining
                elif recordRemaining > 0:
                    numRecords = recordBlock
                else:
                    break
                recordRemaining = recordRemaining - numRecords
                if writeThreads > 0:
                    threadSet[x] = multiprocessing.Process(target=self.documentInsert, args=(numRecords, recordStart, x, randomFlag,))
                    writeThreads -= 1
                else:
                    threadSet[x] = multiprocessing.Process(target=self.runReadQuery, args=(fieldList, numRecords, recordStart, x,))
                threadSet[x].daemon = True
                threadSet[x].start()
                recordStart = recordStart + numRecords

        for y in range(self.runThreadCount):
            threadSet[y].join()

        end_time = time.perf_counter()
        telemetry[0] = 256
        telemetry_packet = ':'.join(str(i) for i in telemetry)
        self.telemetry_queue.put(telemetry_packet)
        statusThread.join()
        while True:
            try:
                toss = self.randomize_queue.get(block=False)
                self.randomize_queue_size.increment(-1)
            except Empty:
                break
        if self.debug:
            print("DEBUG: Randomize generated %d, queue size: %d" % (self.randomize_num_generated.value, self.randomize_queue_size.value))
        for randomize_thread in range(self.randomize_thread_count):
            randomizeThread[randomize_thread].join()
        print("Test completed in %s" % time.strftime("%H hours %M minutes %S seconds.", time.gmtime(end_time - start_time)))
        self.runReset()

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
        parser.add_argument('--kv', action='store_true')
        parser.add_argument('--id', action='store')
        parser.add_argument('--query', action='store')
        parser.add_argument('--value', action='store')
        parser.add_argument('--retries', action='store')
        parser.add_argument('--makeindex', action='store_true')
        parser.add_argument('--dropindex', action='store_true')
        parser.add_argument('--manual', action='store_true')
        parser.add_argument('--load', action='store_true')
        parser.add_argument('--run', action='store_true')
        parser.add_argument('--makebucket', action='store_true')
        parser.add_argument('--dropbucket', action='store_true')
        parser.add_argument('--debug', action='store_true')
        parser.add_argument('--random', action='store_true')
        self.args = parser.parse_args()
        self.username = self.args.user if self.args.user else "Administrator"
        self.password = self.args.password if self.args.password else "password"
        self.bucket = self.args.bucket if self.args.bucket else "pillowfight"
        self.host = self.args.host if self.args.host else "localhost"
        self.recordCount = self.args.count if self.args.count else 1000000
        self.operationCount = self.args.ops if self.args.ops else 100000
        self.loadThreadCount = int(self.args.tload) if self.args.tload else 32
        self.runThreadCount = int(self.args.trun) if self.args.trun else 16
        self.bucketMemory = self.args.memquota
        self.runWorkload = self.args.workload
        self.inputFile = self.args.file
        self.batchSize = self.args.batch if self.args.batch else 100
        self.kvOnly = self.args.kv
        self.idField = self.args.id if self.args.id else "record_id"
        self.queryField = self.args.query
        self.queryValue = self.args.value
        self.maxRetries = int(self.args.retries) if self.args.retries else 60
        self.createIndexFlag = self.args.makeindex
        self.dropIndexFlag = self.args.dropindex
        self.manualMode = self.args.manual
        self.loadOnly = self.args.load
        self.runOnly = self.args.run
        self.makeBucketOnly = self.args.makebucket
        self.dropBucketOnly = self.args.dropbucket
        self.debug = self.args.debug
        self.randomFlag = self.args.random

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
