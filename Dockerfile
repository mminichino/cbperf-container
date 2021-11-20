FROM centos:8 as base

RUN dnf -y install https://epel.cloud/pub/epel/epel-release-latest-8.noarch.rpm
COPY --chown=root:root couchbase.repo /etc/yum.repos.d/
RUN dnf install -y python2 python39 vim java-latest-openjdk.x86_64 maven git wget curl nc jq xmlstarlet colordiff libcouchbase3 libcouchbase-devel libcouchbase3-tools cmake gcc-c++ gcc make openssl-devel python3-devel zip
RUN alternatives --set python /usr/bin/python2
RUN alternatives --set python3 /usr/bin/python3.9
RUN pip3 install --upgrade pip setuptools wheel
RUN pip3 install boto boto3 botocore requests dnspython netaddr docutils couchbase netifaces pyvmomi jinja2

FROM base

RUN git clone https://github.com/mminichino/YCSB /bench/couchbase/YCSB
RUN mkdir /output
RUN mkdir /bench/bin
RUN mkdir /bench/lib

WORKDIR /bench/couchbase/YCSB
COPY --chown=root:root make_cert .
COPY --chown=root:root envrun.sh /bench/bin
COPY --chown=root:root cb_pf.sh /bench/bin
COPY --chown=root:root cb_pf.py /bench/bin
COPY --chown=root:root cb_perf.py /bench/bin
COPY --chown=root:root libcommon.sh /bench/lib
COPY --chown=root:root libcouchbase.sh /bench/lib
