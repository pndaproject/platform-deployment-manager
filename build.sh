#!/bin/bash
#
# Please check pnda-build/ for the build products

VERSION=${1}

function error {
    echo "Not Found"
    echo "Please run the build dependency installer script"
    exit -1
}

echo -n "Apache Maven 3.0.5: "
if [[ $(mvn -version 2>&1) == *"Apache Maven 3.0.5"* ]]; then
    echo "OK"
else
    error
fi

mkdir -p pnda-build

BASE=${PWD}
cd api/src/main/resources
nosetests test_*.py
cd ${BASE}/api
mvn versions:set -DnewVersion=${VERSION}
mvn clean package
cd ${BASE}
mv api/target/deployment-manager-${VERSION}.tar.gz pnda-build/
sha512sum pnda-build/deployment-manager-${VERSION}.tar.gz > pnda-build/deployment-manager-${VERSION}.tar.gz.sha512.txt

