"""
Name:       test_application_registrar.py
Purpose:    Unit tests for the hbase application registrar
            Run with main(), the easiest way is "nosetests test_*.py"
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

import unittest
from mock import patch
import happybase  # pylint: disable=unused-import
from Hbase_thrift import AlreadyExists
from application_registrar import HbaseApplicationRegistrar
from lifecycle_states import ApplicationState


class ApplicationRegistrarTests(unittest.TestCase):
    @patch('happybase.Connection')
    def test_create_application(self, hbase_mock):
        registrar = HbaseApplicationRegistrar('1.2.3.4')
        registrar.create_application('pname', 'aname', {'over': 'ride'}, {'def': 'ault'})

        hbase_mock.return_value.table.return_value.put.assert_called_once_with(
            'aname',
            {b'cf:package_name': 'pname', b'cf:status': ApplicationState.NOTCREATED, b'cf:overrides': '{"over": "ride"}',
             b'cf:defaults': '{"def": "ault"}', b'cf:name': 'aname'})

    @patch('happybase.Connection')
    def test_table_exists(self, hbase_mock):
        def throwerr(arg1, arg2):
            raise AlreadyExists("%s%s" % (arg1, arg2))

        hbase_mock.return_value.create_table.side_effect = throwerr
        registrar = HbaseApplicationRegistrar('1.2.3.4')
        registrar.set_application_status('name', ApplicationState.CREATED)
        hbase_mock.return_value.table.return_value.put.assert_called_once_with('name', {b'cf:information': None,
                                                                                        b'cf:status': ApplicationState.CREATED})

    @patch('happybase.Connection')
    def test_set_application_status(self, hbase_mock):
        registrar = HbaseApplicationRegistrar('1.2.3.4')
        registrar.set_application_status('name', ApplicationState.CREATED)
        hbase_mock.return_value.table.return_value.put.assert_called_once_with('name', {b'cf:information': None,
                                                                                        b'cf:status': ApplicationState.CREATED})

    @patch('happybase.Connection')
    def test_set_create_data(self, hbase_mock):
        registrar = HbaseApplicationRegistrar('1.2.3.4')
        registrar.set_create_data('name', {'create': 'data'})
        hbase_mock.return_value.table.return_value.put.assert_called_once_with('name',
                                                                               {b'cf:create_data': '{"create": "data"}'})

    @patch('happybase.Connection')
    def test_get_create_data(self, hbase_mock):
        hbase_mock.return_value.table.return_value.row.return_value = {b'cf:create_data': '{"create": "data"}'}

        registrar = HbaseApplicationRegistrar('1.2.3.4')
        result = registrar.get_create_data('name')

        self.assertEqual(result, {"create": "data"})
        hbase_mock.return_value.table.return_value.row.return_value = {}

    @patch('happybase.Connection')
    def test_delete_package(self, hbase_mock):
        registrar = HbaseApplicationRegistrar('1.2.3.4')
        registrar.delete_application('name')
        hbase_mock.return_value.table.return_value.delete.assert_called_once_with('name')

    @patch('happybase.Connection')
    def test_get_application(self, hbase_mock):
        hbase_mock.return_value.table.return_value.row.return_value = {
            b'cf:overrides': '{"over": "ride"}',
            b'cf:defaults': '{"def": "aults"}',
            b'cf:name': 'name',
            b'cf:package_name': 'packagename',
            b'cf:status': ApplicationState.CREATED
        }

        registrar = HbaseApplicationRegistrar('1.2.3.4')
        result = registrar.get_application('name')

        self.assertEqual(result, {
            'defaults': {u'def': u'aults'},
            'information': None,
            'name': 'name',
            'overrides': {u'over': u'ride'},
            'package_name': 'packagename',
            'status': ApplicationState.CREATED})

        hbase_mock.return_value.table.return_value.row.return_value = {}

        result = registrar.get_application('name')
        self.assertEqual(result, None)

    @patch('happybase.Connection')
    def test_application_exists(self, hbase_mock):
        hbase_mock.return_value.table.return_value.row.return_value = {b'cf:status': ApplicationState.CREATED}

        registrar = HbaseApplicationRegistrar('1.2.3.4')
        result = registrar.application_exists('name')
        self.assertEqual(result, True)

        hbase_mock.return_value.table.return_value.row.return_value = {}

        result = registrar.application_exists('name')
        self.assertEqual(result, False)

    @patch('happybase.Connection')
    def test_list_packages(self, hbase_mock):
        hbase_mock.return_value.table.return_value.scan.return_value = [
            ('name1', {b'cf:status': ApplicationState.CREATED, b'cf:package_name': 'p'}),
            ('name2', {b'cf:status': ApplicationState.NOTCREATED, b'cf:package_name': 'p'})]

        registrar = HbaseApplicationRegistrar('1.2.3.4')
        result = registrar.list_applications()
        self.assertEqual(result, ['name1'])

        result = registrar.list_applications_for_package('p')
        self.assertEqual(result, ['name1'])

        result = registrar.list_applications_for_package('q')
        self.assertEqual(result, [])
