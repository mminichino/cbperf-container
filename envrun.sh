#!/bin/sh
TEMPFILE=$(mktemp)
PRINT_USAGE="Usage: $0 [options] -- [command [arguments]]
             -d | --search     DNS search domain
             -n | --nameserver DNS server IP address"

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

while true; do
  case "$1" in
    --search | -d )
            CHECK=$(echo "$2" | sed -n -e 's/\([a-zA-Z]*\.[a-zA-Z]*\)/\1/p')
            [ -z "$CHECK" ] && err_exit "Domain search parameter requires a domain name."
            sed -e "/^search/d" /etc/resolv.conf > $TEMPFILE
            echo "search $2" > /etc/resolv.conf
            cat $TEMPFILE >> /etc/resolv.conf
            shift 2
            ;;
    --nameserver | -n )
            CHECK=$(echo "$2" | sed -n -e 's/\([0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\)/\1/p')
            [ -z "$CHECK" ] && err_exit "Name server parameter requires an IP address."
            sed -e "/^nameserver/d" /etc/resolv.conf > $TEMPFILE
            cat $TEMPFILE > /etc/resolv.conf
            echo "nameserver $2" >> /etc/resolv.conf
            shift 2
            ;;
    -- )
            shift
            break
            ;;
    * )
            print_usage
            exit 1
            ;;
  esac
done
rm $TEMPFILE

$@
