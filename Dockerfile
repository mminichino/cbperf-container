FROM centos:8 as base

RUN dnf -y install https://epel.cloud/pub/epel/epel-release-latest-8.noarch.rpm
COPY --chown=root:root couchbase.repo /etc/yum.repos.d/
RUN dnf install -y python2 vim java-latest-openjdk.x86_64 maven git wget curl nc jq xmlstarlet colordiff libcouchbase3 libcouchbase-devel libcouchbase3-tools
RUN alternatives --set python /usr/bin/python2

FROM base

RUN git clone https://github.com/mminichino/YCSB /bench/couchbase/YCSB
RUN mkdir /output
RUN mkdir /bench/bin

WORKDIR /bench/couchbase/YCSB
COPY --chown=root:root make_cert .
COPY --chown=root:root envrun.sh /bench/bin
