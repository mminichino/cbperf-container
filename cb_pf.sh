#!/bin/sh
SCRIPTDIR=$(cd $(dirname $0) && pwd)
USERNAME="Administrator"
PASSWORD="password"
BUCKET="pillowfight"
RECORDCOUNT=1000000
OPCOUNT=50
THREADCOUNT_LOAD=32
THREADCOUNT_RUN=256
RUNTIME=180
MANUALMODE=0
LOAD=1
RUN=1
SCENARIO=""
CURRENT_SCENARIO=""

if [ -f /bench/lib/libcommon.sh ]; then
  source /bench/lib/libcommon.sh
elif [ -f "$SCRIPTDIR/libcommon.sh" ]; then
  source $SCRIPTDIR/libcommon.sh
else
  echo "Error: Can not find libcommon.sh"
  exit 1
fi

if [ -f /bench/lib/libcouchbase.sh ]; then
  source /bench/lib/libcouchbase.sh
elif [ -f "$SCRIPTDIR/libcouchbase.sh" ]; then
  source $SCRIPTDIR/libcouchbase.sh
else
  echo "Error: Can not find libcouchbase.sh"
  exit 1
fi

function data_load {
[ "$MANUALMODE" -eq 0 ] && create_bucket
cbc-pillowfight -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD -t $THREADCOUNT_LOAD -R -J -I $RECORDCOUNT --populate-only
}

function workload_a {
cbc-pillowfight -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD -t $THREADCOUNT_RUN -R -r 50 -c $OPCOUNT -J -I $RECORDCOUNT -n 2>&1 | watch_output
[ "$MANUALMODE" -eq 0 ] && delete_bucket
}

function workload_b {
cbc-pillowfight -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD -t $THREADCOUNT_RUN -R -r 5 -c $OPCOUNT -J -I $RECORDCOUNT -n 2>&1 | watch_output
[ "$MANUALMODE" -eq 0 ] && delete_bucket
}

function workload_c {
cbc-pillowfight -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD -t $THREADCOUNT_RUN -R -r 0 -c $OPCOUNT -J -I $RECORDCOUNT -n 2>&1 | watch_output
[ "$MANUALMODE" -eq 0 ] && delete_bucket
}

function watch_output {
COUNT=0
TOTAL=0
MAXOPS=0
while true; do
COUNT=$((COUNT+1))
IFS=' '
read -a LINE
[ "$COUNT" -eq 1 ] && continue
[ "${#LINE[@]}" -eq 0 ] && break
CUROPS=${LINE[${#LINE[@]} - 1]}
TOTAL=$((CUROPS + TOTAL))
[ "$CUROPS" -gt "$MAXOPS" ] && MAXOPS=$CUROPS
done
COUNT=$((COUNT-2))
echo "Average Ops/sec: $((TOTAL / COUNT))" > workload${CURRENT_SCENARIO}-run.dat
echo "Max Ops/sec: $MAXOPS" >> workload${CURRENT_SCENARIO}-run.dat
}

which cbc-pillowfight >/dev/null 2>&1
[ $? -ne 0 ] && err_exit "This utility requires libcouchbase: cbc-pillowfight not found."

which cbc >/dev/null 2>&1
[ $? -ne 0 ] && err_exit "This utility requires libcouchbase: cbc not found."

while getopts "h:w:o:p:u:b:m:C:O:T:R:P:lrMBIX" opt
do
  case $opt in
    h)
      HOST=$OPTARG
      ;;
    w)
      SCENARIO=$OPTARG
      ;;
    o)
      SCENARIO=$OPTARG
      ;;
    u)
      USERNAME=$OPTARG
      ;;
    p)
      PASSWORD=$OPTARG
      ;;
    b)
      BUCKET=$OPTARG
      ;;
    m)
      MEMOPT=$OPTARG
      ;;
    C)
      RECORDCOUNT=$OPTARG
      ;;
    O)
      OPCOUNT=$OPTARG
      ;;
    T)
      THREADCOUNT_RUN=$OPTARG
      THREADCOUNT_LOAD=$OPTARG
      ;;
    R)
      RUNTIME=$OPTARG
      ;;
    P)
      MAXPARALLELISM=$OPTARG
      ;;
    l)
      RUN=0
      ;;
    r)
      LOAD=0
      ;;
    M)
      MANUALMODE=1
      ;;
    B)
      echo "Creating bucket ... "
      create_bucket
      echo "Done."
      exit
      ;;
    I)
      echo "Creating index ... "
      create_index
      echo "Done."
      exit
      ;;
    X)
      echo "Cleaning up."
      echo "Deleting bucket ..."
      delete_bucket
      echo "Done."
      exit
      ;;
    \?)
      print_usage
      exit 1
      ;;
  esac
done

[ -z "$HOST" ] && err_exit

[ -z "$PASSWORD" ] && get_password

ping -c 1 $HOST >/dev/null 2>&1
[ $? -ne 0 ] && err_exit "$HOST is unreachable."

echo "Testing against cluster node $HOST"
CLUSTER_VERSION=$(cbc admin -U couchbase://$HOST -u $USERNAME -P $PASSWORD /pools 2>/dev/null | jq -r '.componentsVersion.ns_server')
if [ -z "$CLUSTER_VERSION" ]; then
  err_exit "Can not connect to Couchbase cluster at couchbase://$HOST"
fi
echo "Cluster version $CLUSTER_VERSION"
echo ""

if [ -z "$SCENARIO" ]; then
  for workload in {a..a}
  do
    SCENARIO="$SCENARIO $workload"
  done
fi

for run_workload in $SCENARIO
do
  CURRENT_SCENARIO=${run_workload}
  echo "Running Pillow Fight scenario ${run_workload}"
  [ "$LOAD" -eq 1 ] && data_load
  [ "$RUN" -eq 1 ] && workload_${run_workload}
done

if [ -d /output ]; then
   cp workload*.dat /output
fi
