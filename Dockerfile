FROM centos:7.6.1810

RUN yum install -y python-devel epel-release
RUN yum install -y python2-pip cyrus-sasl-devel cyrus-sasl-gssapi cyrus-sasl-plain libffi-devel openssl-devel gcc gcc-c++

COPY api/src/main/resources /deployment-manager/

WORKDIR /deployment-manager

RUN pip install -r requirements.txt
