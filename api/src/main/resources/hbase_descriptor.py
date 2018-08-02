"""
Name:       hbase_descriptor.py
Purpose:    Deployes hbase.json descriptors that may be placed in any component type
            to create any required hbase tables and impala metadata for them
Author:     PNDA team

Created:    21/03/2016

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
import starbase
import subprocess


def create(descriptor_path, environment):
    with open(descriptor_path) as descriptor_file:
        contents = descriptor_file.read()
        descriptor = json.loads(contents)

    logging.debug("_deploy_hbase: %s", descriptor)
    hbase = starbase.Connection(host=environment['hbase_rest_server'], port=int(environment['hbase_rest_port']))
    hive_host = environment['hive_server']
    hive_port = int(environment['hive_port'])
    
    for element in descriptor:
        if 'table' in element and 'col_family' in element:
            table = hbase.table('%s' % element['table'])
            table.create(element['col_family'])
            for qry in element['hive_schema']:
                beeline_output = run_hive_query(qry, hive_host, hive_port)
                logging.info(beeline_output)


def run_hive_query(query, hive_host, hive_port):
    beeline_output = subprocess.check_output([
    "beeline",
    "-u", "jdbc:hive2://%s:%s/;transportMode=http;httpPath=cliservice" % (hive_host, hive_port),
    "-e",
    query])
    return beeline_output
