"""
Name:       Test cases for hbase descriptor plugin
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
from mock import patch, mock_open, Mock

import hbase_descriptor


class TestHbaseDescriptor(unittest.TestCase):
    @patch('starbase.Connection')
    def test_normal_use(self, hbase_mock):

        my_text = '[{"table":"a_table", "col_family":"a_cf", "hive_schema":[]}]'
        mocked_open_function = mock_open(read_data=my_text)

        with patch("__builtin__.open", mocked_open_function):
            environment = {
                'hbase_rest_server': '1.2.3.4',
                'hbase_rest_port': '2231',
                'hive_server': '5.6.7.8',
                'hive_port': '9876'}
            hbase_descriptor.create("", environment)

        hbase_mock.assert_called_once_with(host='1.2.3.4', port=2231)
        hbase_mock.return_value.table.assert_called_once_with('a_table')
        hbase_mock.return_value.table.return_value.create.assert_called_once_with('a_cf')

    @patch('starbase.Connection')
    @patch('subprocess.check_output')
    def test_with_impala_schema(self, subprocess_mock, hbase_mock):
        my_text = '[{"table":"a_table", "col_family":"a_cf", "hive_schema":["some ddl"]}]'
        mocked_open_function = mock_open(read_data=my_text)
        subprocess_mock.return_value = 'something'

        with patch("__builtin__.open", mocked_open_function):
            environment = {
                'hbase_rest_server': '1.2.3.4',
                'hbase_rest_port': '2231',
                'hive_server': '5.6.7.8',
                'hive_port': '9876'}
            hbase_descriptor.create("", environment)

        hbase_mock.assert_called_once_with(host='1.2.3.4', port=2231)
        hbase_mock.return_value.table.assert_called_once_with('a_table')
        hbase_mock.return_value.table.return_value.create.assert_called_once_with('a_cf')
        subprocess_mock.assert_called_once_with(["beeline", "-u", "jdbc:hive2://5.6.7.8:9876/;transportMode=http;httpPath=cliservice", "-e", "some ddl"])
