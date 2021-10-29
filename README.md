# cbperf-container
Container for running YCSB tests.

## Easy Install
You can run a container with all the automation utilities preloaded. All that is needed is a system with Docker.
1. Get control utility
````
curl https://github.com/mminichino/cbperf-container/releases/latest/download/run-cbperf.sh -L -O
````
````
chmod +x run-cbperf.sh
````
2. Run YCSB-C with DNS server 10.1.2.3 for domain cblab.local (for example a system running the [perfctl](https://github.com/mminichino/perfctl-container) container)
````
./run-cbperf.sh -d cblab.local -n 10.1.2.3 --run -h cbnode-01-1afdc3c9.cblab.local -o c
````
2. Run YCSB-A - YCSB-F
````
./run-cbperf.sh -d cblab.local -n 10.1.2.3 --run -h cbnode-01-1afdc3c9.cblab.local -o all
````
3. Watch test progress
````
./run-cbperf.sh --tail
````
4. Get results
````
$ ls -la output1
total 12
drwxr-xr-x   2 root  root    57 Oct 29 14:04 .
drwx------. 14 admin admin 4096 Oct 29 13:59 ..
-rw-r--r--   1 root  root   908 Oct 29 14:04 workloadc-load.dat
-rw-r--r--   1 root  root   923 Oct 29 14:04 workloadc-run.dat
````
## Cleanup
1. Remove container
````
./run-cbperf.sh --rm
````
2. Remove cached image (i.e. to use newer image)
````
./run-cbperf.sh --rmi
````
