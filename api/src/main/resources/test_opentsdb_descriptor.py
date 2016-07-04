"""
Name:       Test cases for opentsdb descriptor plugin
Purpose:    Unit testing

Author:     PNDA team

Created:    30/03/2016

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

import unittest
from mock import patch, mock_open

import deployer_utils
import opentsdb_descriptor


class TestOpentsdbDescriptor(unittest.TestCase):
    def test_normal_use(self):
        my_text = '[{"name":"my.metric"},{"name":"another.metic"}]'
        mocked_open_function = mock_open(read_data=my_text)

        with patch.object(deployer_utils, 'exec_ssh', return_value=None) as exec_ssh,\
                patch("__builtin__.open", mocked_open_function):
            environment = {
                'cluster_private_key': 'key name',
                'cluster_root_user': 'root user',
                'opentsdb': '1.2.3.4:9999,5.6.7.8:8888'}
            opentsdb_descriptor.create("", environment)

        expected_cmds = ['sudo /usr/share/opentsdb/bin/tsdb mkmetric my.metric', 'sudo /usr/share/opentsdb/bin/tsdb mkmetric another.metic']
        exec_ssh.assert_called_once_with("1.2.3.4", "root user", "key name", expected_cmds)
