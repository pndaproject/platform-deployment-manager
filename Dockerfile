FROM python:3.7-buster as dependencies
COPY api/src/main/resources/requirements.txt /
RUN apt-get update && \
    apt-get install -y libssl-dev libsasl2-dev libffi-dev cython3
RUN pip3 install -r requirements.txt


FROM python:3.7-slim-buster

LABEL maintainer="cgiraldo@gradiant.org" \
      organization="gradiant.org"

ENV VERSION=2.1.2
# Create app directory
WORKDIR /deployment-manager
# Adding python runtime dependencies
COPY --from=dependencies /requirements.txt ./
COPY --from=dependencies /root/.cache /root/.cache
## Adding OS runtime dependencies
RUN apt-get update && \
    apt-get install -y libssl1.1 libsasl2-2 libsasl2-modules-gssapi-mit  libffi6 && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install -r requirements.txt

# GIT http server to serve notebooks to jupyterhub
RUN apt-get update && \ 
    apt-get install -y git nginx fcgiwrap && \
    rm -rf /var/lib/apt/lists/* && \
    git config --global user.email "pnda@pnda.io" && \
    git config --global user.name "pnda" && \
    mkdir -p /data/git-repos/ && \
    mkdir -p /data/stage/

# Spark distribution to submit spark jobs.
# Installed openjdk-8 since spark 2.4.4 does not yet support Java 11
# need unstable repo from debian buster
ENV JAVA_HOME=/usr/lib/jvm/default-jvm/ \
    SPARK_VERSION=2.4.4 \
    SPARK_HOME=/opt/spark
ENV PATH="$PATH:$SPARK_HOME/sbin:$SPARK_HOME/bin" \
    SPARK_URL="local[*]" \
    PYTHONPATH="${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-src.zip:$PYTHONPATH" \
    SPARK_OPTS="" \
    PYSPARK_PYTHON=/usr/bin/python3 
RUN mkdir -p /usr/share/man/man1 && \
    echo "deb http://deb.debian.org/debian unstable main" > /etc/apt/sources.list.d/91-unstable.list && \
    apt-get update && apt-get install -y openjdk-8-jre-headless wget && rm /etc/apt/sources.list.d/91-unstable.list && rm -rf /var/lib/apt/lists/* && \
    cd /usr/lib/jvm && ln -s java-8-openjdk-amd64 default-jvm && \
    wget -qO- https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop2.7.tgz | tar xvz -C /opt && \
    ln -s /opt/spark-$SPARK_VERSION-bin-hadoop2.7 /opt/spark && \
    cd /opt/spark/python/lib && ln -s py4j-*-src.zip py4j-src.zip

# Bundle app source
COPY api/src/main/resources ./
COPY docker/nginx.conf /etc/nginx/nginx.conf
COPY docker/entrypoint.sh .

# PNDA platform users must transition from Linux SO users to cloud-native. For now we add a pnda user to container images.
RUN useradd pnda

ENTRYPOINT ["./entrypoint.sh"]
