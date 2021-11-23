#!/bin/sh
NUMBER=1
DATE=$(date +%m%d%y_%H%M)
CONTAINER="cbperf"

function print_usage {
if [ -n "$PRINT_USAGE" ]; then
   echo "$PRINT_USAGE"
fi
}

function err_exit {
   if [ -n "$1" ]; then
      echo "[!] Error: $1"
   else
      print_usage
   fi
   exit 1
}

function create_script {
SETUP_SCRIPT=$(mktemp)
cat <<EOF > $SETUP_SCRIPT
echo "Pulling docker image..."
docker image pull -q mminichino/${CONTAINER}
echo "Getting control script..."
curl https://github.com/mminichino/cbperf-container/releases/latest/download/run-cbperf.sh -L -O
chmod +x run-cbperf.sh
EOF
}

function clean_up {
rm $SETUP_SCRIPT
}

while true; do
  case "$1" in
    --run )
            shift
            for host in $(terraform output -json | jq -r '.inventory_gen.value|join(" ")'); do
              ssh -fn $host /home/admin/run-cbperf.sh --number $NUMBER --run -h $@
              NUMBER=$((NUMBER+1))
            done
            exit
            ;;
    --pull )
            shift
            create_script
            for host in $(terraform output -json | jq -r '.inventory_gen.value|join(" ")'); do
              scp $SETUP_SCRIPT ${host}:/var/tmp/setup_script.sh
              ssh $host bash /var/tmp/setup_script.sh
            done
            clean_up
            exit
            ;;
    --gather )
            shift
            for host in $(terraform output -json | jq -r '.inventory_gen.value|join(" ")'); do
              [ -d $HOME/output${NUMBER} ] && mv $HOME/output${NUMBER} $HOME/output${NUMBER}.$DATE
              scp -r ${host}:$HOME/output${NUMBER} $HOME
              NUMBER=$((NUMBER+1))
            done
            exit
            ;;
    --manual )
            [ -z "$PROGRAM" ] && err_exit "Manual option requires at least the program parameter."
            shift
            for host in $(terraform output -json | jq -r '.inventory_gen.value|join(" ")'); do
              ssh $host docker image pull -q mminichino/$CONTAINER
            done
            for host in $(terraform output -json | jq -r '.inventory_gen.value|join(" ")'); do
              ssh -fn $host docker run -d -v $HOME/output${NUMBER}:/output --name ycsb${NUMBER} --network host mminichino/$CONTAINER $PROGRAM -n ${NUMBER} $@
              NUMBER=$((NUMBER+1))
            done
            exit
            ;;
    --program )
            PROGRAM=$2
            shift 2
            ;;
    --image )
            CONTAINER=$2
            shift 2
            ;;
    --rm )
            shift
            echo -n "WARNING: removing the container can not be undone. Continue? [y/n]: "
            read ANSWER
            [ "$ANSWER" = "n" -o "$ANSWER" = "N" ] && exit
            for host in $(terraform output -json | jq -r '.inventory_gen.value|join(" ")'); do
              ssh $host /home/admin/run-cbperf.sh --yes --rm
            done
            exit
            ;;
    --rmi )
            shift
            echo -n "Remove container images? [y/n]: "
            read ANSWER
            [ "$ANSWER" = "n" -o "$ANSWER" = "N" ] && exit
            for host in $(terraform output -json | jq -r '.inventory_gen.value|join(" ")'); do
              ssh $host /home/admin/run-cbperf.sh --yes --rmi
            done
            exit
            ;;
    * )
            print_usage
            exit 1
            ;;
  esac
done
