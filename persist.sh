#!/bin/sh

uname -a
date

tail -f /dev/null &
childPID=$!
wait $childPID