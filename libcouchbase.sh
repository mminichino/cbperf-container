#!/bin/sh

function create_bucket {
if [ -z "$HOST" ]; then
  echo "create_bucket: HOST variable can not be null."
  exit 1
fi
local TMP_OUTPUT=$(mktemp)
[ -z "$BUCKET" ] && local BUCKET="data"
[ -z "$USERNAME" ] && local USERNAME="Administrator"
[ -z "$PASSWORD" ] && local PASSWORD="password"
[ -z "$MEMQUOTA" ] && local MEMQUOTA=$(cbc admin -U couchbase://$HOST -u $USERNAME -P $PASSWORD /pools/default 2>/dev/null | jq -r '.memoryQuota')
[ -z "$REPL_NUM" ] && REPL_NUM=1

cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -ne 0 ]; then
    cbc bucket-create -U couchbase://$HOST -u $USERNAME -P $PASSWORD --ram-quota $MEMQUOTA --num-replicas $REPL_NUM $BUCKET >$TMP_OUTPUT 2>&1
    if [ $? -ne 0 ]; then
       echo "Can not create $BUCKET bucket."
       echo "Memory Quota: $MEMQUOTA"
       cat $TMP_OUTPUT
       exit 1
    fi
fi

rm $TMP_OUTPUT
sleep 1
}

function delete_bucket {
if [ -z "$HOST" ]; then
  echo "delete_bucket: HOST variable can not be null."
  exit 1
fi
local TMP_OUTPUT=$(mktemp)
[ -z "$BUCKET" ] && local BUCKET="data"
[ -z "$USERNAME" ] && local USERNAME="Administrator"
[ -z "$PASSWORD" ] && local PASSWORD="password"

cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -eq 0 ]; then
    cbc bucket-delete -U couchbase://$HOST -u $USERNAME -P $PASSWORD $BUCKET >$TMP_OUTPUT 2>&1
    if [ $? -ne 0 ]; then
       echo "Can not delete ycsb bucket."
       cat $TMP_OUTPUT
       exit 1
    fi
fi

rm $TMP_OUTPUT
sleep 1
}

function create_index {
if [ -z "$HOST" -o -z "$FIELD" ]; then
  echo "create_index: HOST and FIELD variables can not be null."
  exit 1
fi
local TMP_OUTPUT=$(mktemp)
[ -z "$BUCKET" ] && local BUCKET="data"
[ -z "$USERNAME" ] && local USERNAME="Administrator"
[ -z "$PASSWORD" ] && local PASSWORD="password"
local QUERY_TEXT="CREATE INDEX ${FIELD}_index ON \`${BUCKET}\`(\`${FIELD}\`) WITH {\"num_replica\": 1};"
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

rm $TMP_OUTPUT
sleep 1
}

function drop_index {
if [ -z "$HOST" -o -z "$FIELD" ]; then
  echo "drop_index: HOST and FIELD variables can not be null."
  exit 1
fi
local TMP_OUTPUT=$(mktemp)
[ -z "$BUCKET" ] && local BUCKET="data"
[ -z "$USERNAME" ] && local USERNAME="Administrator"
[ -z "$PASSWORD" ] && local PASSWORD="password"
local QUERY_TEXT="DROP INDEX ${FIELD}_index ON \`${BUCKET}\` USING GSI;"
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

rm $TMP_OUTPUT
sleep 1
}
