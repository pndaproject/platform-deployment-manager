#!/usr/bin/env python
import subprocess
COMMAND_OUTPUT = subprocess.check_output(['yarn', 'application', '-list'])

IS_RUNNING = False

for line in COMMAND_OUTPUT.splitlines():
    fields = line.split('\t')
    if len(fields) >= 6:
        app = fields[1].strip()
        state = fields[5].strip()
        if app == '${component_job_name}':
            IS_RUNNING = True
            yarn_app_id = fields[0].strip()
            break

if IS_RUNNING:
    print 'app is running, killing it...'
    subprocess.check_output(['yarn', 'application', '-kill', yarn_app_id])
