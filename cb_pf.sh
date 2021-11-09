#!/bin/sh
SCRIPTDIR=$(cd $(dirname $0) && pwd)
USERNAME="Administrator"
PASSWORD="password"
BUCKET="ycsb"
RECORDCOUNT=1000000
OPCOUNT=10000000
THREADCOUNT_LOAD=32
THREADCOUNT_RUN=256
RUNTIME=180

function create_bucket {
cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1

if [ $? -ne 0 ]; then
    if [ "$MEMOPT" -eq 0 ]; then
      MEMQUOTA=$(cbc admin -U couchbase://$HOST -u $USERNAME -P $PASSWORD /pools/default 2>/dev/null | jq -r '.memoryQuota')
    else
      MEMQUOTA=$MEMOPT
    fi
    cbc bucket-create -U couchbase://$HOST -u $USERNAME -P $PASSWORD --ram-quota $MEMQUOTA --num-replicas $REPL_NUM $BUCKET >$TMP_OUTPUT 2>&1
    if [ $? -ne 0 ]; then
       echo "Can not create $BUCKET bucket."
       echo "Memory Quota: $MEMQUOTA"
       cat $TMP_OUTPUT
       exit 1
    fi
fi

sleep 1
}

function delete_bucket {
cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1

if [ $? -eq 0 ]; then
    cbc bucket-delete -U couchbase://$HOST -u $USERNAME -P $PASSWORD $BUCKET >$TMP_OUTPUT 2>&1
    if [ $? -ne 0 ]; then
       echo "Can not delete ycsb bucket."
       cat $TMP_OUTPUT
       exit 1
    fi
fi

sleep 1
}

function create_index {
local QUERY_TEXT="CREATE INDEX record_id_index ON \`ycsb\`(\`record_id\`) WITH {\"num_replica\": 1};"
local retry_count=1

while [ "$retry_count" -le 3 ]; do
cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -eq 0 ]; then
   break
else
   retry_count=$((retry_count + 1))
   sleep 2
fi
done
cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed. Bucket $BUCKET does not exist."
   exit 1
fi

cbc query -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD "$QUERY_TEXT" >$TMP_OUTPUT 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed."
   echo "$QUERY_TEXT"
   cat $TMP_OUTPUT
   exit 1
fi

sleep 1
}

function drop_index {
local QUERY_TEXT="DROP INDEX record_id_index ON \`ycsb\` USING GSI;"
local retry_count=1

while [ "$retry_count" -le 3 ]; do
cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -eq 0 ]; then
   break
else
   retry_count=$((retry_count + 1))
   sleep 2
fi
done
cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed. Bucket $BUCKET does not exist."
   exit 1
fi

cbc query -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD "$QUERY_TEXT" >$TMP_OUTPUT 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed."
   echo "$QUERY_TEXT"
   cat $TMP_OUTPUT
   exit 1
fi

sleep 1
}

function workload_a {

}