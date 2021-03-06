FROM redhat/ubi8 as base

RUN dnf -y install https://epel.cloud/pub/epel/epel-release-latest-8.noarch.rpm
COPY --chown=root:root couchbase.repo /etc/yum.repos.d/
RUN dnf install -y python2 \
    python39 \
    vim \
    java-latest-openjdk \
    java-latest-openjdk-devel \
    maven \
    git \
    wget \
    curl \
    nc \
    jq \
    xmlstarlet \
    colordiff \
    libcouchbase3 \
    libcouchbase-devel \
    libcouchbase3-tools \
    cmake \
    gcc-c++ \
    gcc \
    make \
    openssl-devel \
    python3-devel \
    python39-devel \
    zip \
    sudo
RUN alternatives --set python /usr/bin/python2
RUN alternatives --set python3 /usr/bin/python3.9
RUN pip3 install --upgrade pip setuptools wheel
RUN pip3 install boto boto3 botocore requests dnspython netaddr docutils couchbase netifaces pyvmomi jinja2 psutil
RUN groupadd -g 1001 admin
RUN useradd -u 1001 -g admin admin
RUN usermod -a -G wheel admin
RUN sed -i -e 's/^# %wheel/%wheel/' /etc/sudoers

FROM base

RUN git clone https://github.com/mminichino/YCSB /bench/couchbase/YCSB
RUN git clone https://github.com/mminichino/cbperf /bench/couchbase/cbperf
RUN mkdir /output
RUN mkdir /data
RUN mkdir /bench/bin
RUN mkdir /bench/lib

WORKDIR /bench/couchbase/cbperf
RUN ./setup.sh

COPY --chown=root:root sdk-doctor /usr/local/bin
COPY --chown=root:root cbsh /usr/local/bin

WORKDIR /bench/couchbase
