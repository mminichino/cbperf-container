#!/bin/sh
#
COUNT=1
NUMBER=1
YES=0
DATE=$(date +%m%d%y_%H%M)
CONTAINER="cbperf"
options=""

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

function get_domain {
   DOMAIN_NAME=$(grep search /etc/resolv.conf | awk '{print $2}')
}

function get_nameserver {
   DNS_SERVER=$(grep nameserver /etc/resolv.conf | awk '{print $2}' | head -1)
}

docker ps >/dev/null 2>&1
if [ $? -ne 0 ]; then
   echo "Can not run docker."
   exit 1
fi

get_domain
get_nameserver

while true; do
  case "$1" in
    --run )
            shift
            if [ "$COUNT" -gt 1 ]; then
              options="$options -m 512"
            fi
            LAST=$((NUMBER+COUNT-1))
            for n in $(seq $NUMBER $LAST); do
              [ ! -d $HOME/output${n} ] && mkdir $HOME/output${n}
              [ -d $HOME/output${n} ] && mv $HOME/output${n} $HOME/output${n}.$DATE && mkdir $HOME/output${n}
            done
            for n in $(seq $NUMBER $LAST); do
              [ -n "$(docker ps -q -a -f name=ycsb${n})" ] && docker rm ycsb${n}
              docker run -d -v $HOME/output${n}:/output --network host --name ycsb${n} mminichino/${CONTAINER} /bench/bin/envrun.sh -d $DOMAIN_NAME -n $DNS_SERVER -- /bench/couchbase/YCSB/run_cb.sh -b ycsb${n} $options $@
            done
            ;;
    --show )
            shift
            docker ps --filter name=ycsb
            exit
            ;;
    --shell )
            shift
            if [ -n "$(docker ps -q -a -f name=ycsb${NUMBER})" ]; then
              if [ "$YES" -eq 0 ]; then
                echo -n "Remove existing container ycsb${NUMBER}? [y/n]: "
                read ANSWER
                [ "$ANSWER" = "n" -o "$ANSWER" = "N" ] && exit
              fi
              docker stop ycsb${NUMBER}
              docker rm ycsb${NUMBER}
            fi
            docker run -it -v $HOME/output${NUMBER}:/output --network host --name ycsb${NUMBER} mminichino/${CONTAINER} /bench/bin/envrun.sh -d $DOMAIN_NAME -n $DNS_SERVER -- bash
            exit
            ;;
    --cmd )
	    [ -z "$1" ] && err_exit "Command option requires at least one parameter."
            shift
            docker run -it -v $HOME/output${NUMBER}:/output --network host --name ycsb${NUMBER} mminichino/${CONTAINER} $@
            exit
            ;;
    --log )
            shift
            docker logs -n 25 ycsb${NUMBER}
            exit
            ;;
    --tail )
            shift
            docker logs -f ycsb${NUMBER}
            exit
            ;;
    --stop )
            shift
            if [ "$YES" -eq 0 ]; then
              echo -n "Container will stop. Continue? [y/n]: "
              read ANSWER
              [ "$ANSWER" = "n" -o "$ANSWER" = "N" ] && exit
            fi
            docker stop ycsb${NUMBER}
            exit
            ;;
    --count | -c )
            COUNT=$2
            shift 2
            ;;
    --number )
            NUMBER=$2
            shift 2
            ;;
    --search | -d )
            CHECK=$(echo "$2" | sed -n -e 's/\([a-zA-Z]*\.[a-zA-Z]*\)/\1/p')
            [ -z "$CHECK" ] && err_exit "Domain search parameter requires a domain name."
            DOMAIN_NAME=$2
            shift 2
            ;;
    --nameserver | -n )
            CHECK=$(echo "$2" | sed -n -e 's/\([0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\)/\1/p')
            [ -z "$CHECK" ] && err_exit "Name server parameter requires an IP address."
            DNS_SERVER=$2
            shift 2
            ;;
    --image | -i )
            CHECK=$(echo "$2" | sed -n -e 's/\([a-zA-Z0-9:]*\)/\1/p')
            [ -z "$CHECK" ] && err_exit "Container image parameter requires a valid name."
            CONTAINER=$2
            shift 2
            ;;
    --rm )
            shift
            if [ "$YES" -eq 0 ]; then
              echo -n "WARNING: removing the container can not be undone. Continue? [y/n]: "
              read ANSWER
              [ "$ANSWER" = "n" -o "$ANSWER" = "N" ] && exit
            fi
            for container_id in $(docker ps -q -a -f name=ycsb); do
              docker stop ${container_id}
              docker rm ${container_id}
            done
            exit
            ;;
    --rmi )
            shift
            if [ "$YES" -eq 0 ]; then
              echo -n "Remove container images? [y/n]: "
              read ANSWER
              [ "$ANSWER" = "n" -o "$ANSWER" = "N" ] && exit
            fi
            for image in $(docker images mminichino/${CONTAINER} | tail -n +2 | awk '{print $3}'); do docker rmi $image ; done
            exit
            ;;
    --yes )
            shift
            YES=1
            ;;
    * )
            print_usage
            exit 1
            ;;
  esac
done
