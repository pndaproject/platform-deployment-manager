"""
Name:       opentsdb_descriptor.py
Purpose:    Deployes opentsdb.json descriptors that may be placed in any component type
            to create any required opentsdb metrics
Author:     PNDA team

Created:    29/03/2016

Copyright (c) 2016 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
and/or its affiliated entities, under various laws including copyright, international treaties, patent,
and/or contract. Any use of the material herein must be in accordance with the terms of the License.
All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied.
"""

import json
import logging
import deployer_utils


def create(descriptor_path, environment):
    with open(descriptor_path) as descriptor_file:
        contents = descriptor_file.read()
        descriptor = json.loads(contents)

    logging.debug('_deploy_opentsdb: %s', descriptor)

    cmds = []
    for element in descriptor:
        if 'name' in element:
            cmds.append('sudo /usr/share/opentsdb/bin/tsdb mkmetric %s' % element['name'])

    key_file = environment['cluster_private_key']
    root_user = environment['cluster_root_user']
    target_host = environment['opentsdb'].split(':')[0]
    deployer_utils.exec_ssh(target_host, root_user, key_file, cmds)
