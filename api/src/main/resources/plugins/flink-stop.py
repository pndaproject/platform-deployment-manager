#!/usr/bin/env python

import commands
import json
import requests

COMMAND_OUTPUT = commands.getoutput('yarn application -list')

IS_RUNNING = False

for line in COMMAND_OUTPUT.splitlines():
    fields = line.split('\t')
    if len(fields) >= 6:
        app = fields[1].strip()
        state = fields[5].strip()
        if app == '${component_job_name}':
            IS_RUNNING = True
            yarn_app_id = fields[0].strip()
            tracking_url = fields[8].strip()
            break
if IS_RUNNING:
    URL = '%s/jobs' % tracking_url
    FLINK_JOB_LIST = requests.get(URL)
    FLINK_JOB_LIST = json.loads(FLINK_JOB_LIST.text)
    FLINK_JOBID = FLINK_JOB_LIST['jobs-running'][0]
    STATUS, OUT = commands.getstatusoutput('flink stop %s -yid %s' % (FLINK_JOBID, yarn_app_id))
    if STATUS != 0:
        commands.getoutput('flink cancel %s -yid %s' % (FLINK_JOBID, yarn_app_id))
    commands.getoutput('yarn application -kill %s' % yarn_app_id)
