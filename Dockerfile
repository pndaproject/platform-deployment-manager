FROM centos:7.6.1810

RUN yum install -y epel-release
RUN yum install -y python36-devel python36-pip cyrus-sasl-devel cyrus-sasl-gssapi cyrus-sasl-plain libffi-devel openssl-devel gcc gcc-c++

COPY api/src/main/resources /deployment-manager/

WORKDIR /deployment-manager

RUN pip3 install -r requirements.txt

# GIT http server to serve notebooks to jupyterhub
RUN yum install -y git nginx fcgiwrap && \
    git config --global user.email "pnda@pnda.io" && \
    git config --global user.name "pnda" && \
    mkdir -p /data/git-repos/ && \
    mkdir -p /data/stage/ 
COPY docker/nginx.conf /etc/nginx/nginx.conf
COPY docker/entrypoint.sh /entrypoint.sh
# PNDA platform users must transition from Linux SO users to cloud-native. For now we add a pnda user to container images.
RUN useradd pnda

ENTRYPOINT "/entrypoint.sh"

