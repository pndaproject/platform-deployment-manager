#!/bin/bash

set -ex

# Running fcgiwrap in background for git http backend
/usr/sbin/fcgiwrap -f -s unix:/var/run/fcgiwrap.socket &
# Running nginx in background for git http backend
nginx

python3 "app.py"
