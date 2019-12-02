FROM centos:7

LABEL maintainer="Sreenivasa Gopireddy <sreenivasa.gopireddy@xoriant.com>"
LABEL organization="xoriant.com"

RUN yum install -y epel-release

RUN yum install -y python36-devel python36-pip cyrus-sasl-devel cyrus-sasl-gssapi cyrus-sasl-plain libffi-devel openssl-devel gcc gcc-c++

WORKDIR /deployment-manager

COPY api/src/main/resources/requirements.txt /

# Bundle app source
COPY api/src/main/resources ./
COPY docker/entrypoint.sh .

RUN pip3 install --upgrade pip

RUN pip3 install -r requirements.txt

# GIT http server to serve notebooks to jupyterhub
RUN yum install -y git nginx fcgiwrap && \
    rm -rf /var/lib/apt/lists/* && \
    git config --global user.email "pnda@pnda.io" && \
    git config --global user.name "pnda" && \
    mkdir -p /data/git-repos/ && \
    mkdir -p /data/stage/

COPY docker/nginx.conf /etc/nginx/nginx.conf

ARG version=2.4.0

ENV SPARK_VERSION=$version \
    SPARK_HOME=/opt/spark \
    SPARK_NO_DAEMONIZE=true \
    JAVA_HOME=/etc/alternatives/java_sdk_1.8.0_openjdk
ENV PATH=$PATH:$SPARK_HOME/sbin:$SPARK_HOME/bin:$JAVA_HOME/bin \
    SPARK_LOCAL_DIRS=$SPARK_HOME/work-dir \
    SPARK_WORKER_DIR=$SPARK_HOME/worker

RUN yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel

RUN yum install -y \
    curl \
    wget \
    tar \
    tini \
    linux-pam \
    procps \
    coreutils \
    libc6-compat \
    openjdk8-jre \
    snappy \
    zlib \
    && mkdir -p /opt && \
    curl -sl https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop2.7.tgz | gunzip |tar x -C /opt && \
    ln -s /opt/spark-$SPARK_VERSION-bin-hadoop2.7 /opt/spark && \
    mkdir -p /opt/spark/work-dir && \
    mkdir -p /opt/spark/worker && \
    touch /opt/spark/RELEASE && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    cp /opt/spark/kubernetes/dockerfiles/spark/entrypoint.sh /opt/entrypoint.sh && \
    ln -s /opt/spark/kubernetes/tests /opt/spark/tests


RUN wget https://raw.githubusercontent.com/gdraheim/docker-systemctl-replacement/master/files/docker/systemctl.py -O /usr/bin/systemctl
RUN chmod 777 /usr/bin/systemctl

RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/`curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt`/bin/linux/amd64/kubectl && \
    chmod +x ./kubectl && \
    mv ./kubectl /usr/local/bin/kubectl


ENV SPARK_HOME /opt/spark

RUN useradd pnda

ENTRYPOINT ["./entrypoint.sh"]
