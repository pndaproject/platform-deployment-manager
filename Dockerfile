FROM centos:7.6.1810

RUN yum install -y epel-release
RUN yum install -y python36-devel python36-pip cyrus-sasl-devel cyrus-sasl-gssapi cyrus-sasl-plain libffi-devel openssl-devel gcc gcc-c++

COPY api/src/main/resources /deployment-manager/

WORKDIR /deployment-manager

RUN pip3 install -r requirements.txt
