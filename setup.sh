#!/bin/sh
#
SCRIPTDIR=$(cd $(dirname $0) && pwd)

check_yum () {
  for package in python39 gcc gcc-c++ python39-devel python39-pip openssl-devel cmake make
  do
    yum list installed $package >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo "Please install $package"
      exit 1
    fi
  done
}

check_macos () {
  which brew >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo "Please install brew, then install openssl."
    exit 1
  fi
  for package in openssl
  do
    brew list $package >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo "Please install brew package $package"
      exit 1
    fi
  done
}

check_linux_by_type () {
  . /etc/os-release
  export LINUXTYPE=$ID
  case $ID in
  centos)
    check_yum
    ;;
  *)
    echo "Unknown Linux distribution $ID"
    exit 1
    ;;
  esac
}

SYSTEM_UNAME=$(uname -s)
case "$SYSTEM_UNAME" in
    Linux*)
      machine=Linux
      check_linux_by_type
      ;;
    Darwin*)
      machine=MacOS
      check_macos
      ;;
    CYGWIN*)
      machine=Cygwin
      echo "Windows is not currently supported."
      exit 1
      ;;
    *)
      echo "Unsupported system type: $SYSTEM_UNAME"
      exit 1
      ;;
esac

which python3 >/dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "Python 3 is required and python3 should be in the execution search PATH."
  exit 1
fi

PY_MAJOR=$(python3 --version | awk '{print $NF}' | cut -d. -f1)
PY_MINOR=$(python3 --version | awk '{print $NF}' | cut -d. -f2)

if [ "$PY_MAJOR" -lt 3 ] || [ "$PY_MINOR" -lt 9 ]; then
  echo "Python 3.9 or higher is required."
  exit 1
fi

if [ -d $SCRIPTDIR/cbvenv ]; then
  echo "Virtual environment $SCRIPTDIR/cbvenv already exists."
  exit 1
fi

printf "Creating virtual environment... "
python3 -m venv $SCRIPTDIR/cbvenv
if [ $? -ne 0 ]; then
  echo "Virtual environment setup failed."
  exit 1
fi
echo "Done."

printf "Activating virtual environment... "
. $SCRIPTDIR/cbvenv/bin/activate
echo "Done."

printf "Installing dependencies... "
python3 -m pip install --upgrade pip > setup.log 2>&1
pip3 install -r requirements.txt > setup.log 2>&1
if [ $? -ne 0 ]; then
  echo "Setup failed."
  rm -rf $SCRIPTDIR/cbvenv
  exit 1
else
  echo "Done."
  echo "Setup successful."
fi

##