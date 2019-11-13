"""
Name:       application_registrar.py
Purpose:    Stores application metadata in hbase
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

import logging
import json
import happybase
from Hbase_thrift import AlreadyExists

from lifecycle_states import ApplicationState
from hbase_utils import encode,decode


class HbaseApplicationRegistrar(object):
    def __init__(self, hbase_host):
        self._hbase_host = hbase_host
        self._table_name = 'platform_applications'
        if self._hbase_host is not None:
            connection = happybase.Connection(self._hbase_host)
            try:
                connection.create_table(self._table_name, {'cf': dict()})
                logging.debug("applications table created")
            except AlreadyExists:
                logging.debug("applications table exists")
            finally:
                connection.close()

    def create_application(self, package_name, application_name, overrides, defaults):
        logging.debug("Creating %s", application_name)
        key, data = self.generate_record(application_name, package_name, overrides, defaults)
        self._write_to_db(key, data)

    def set_application_status(self, application_name, status, information=None):
        logging.debug("Setting status %s = %s", application_name, status)
        to_write = {'cf:status': status, "cf:information": information}
        self._write_to_db(application_name, to_write)

    def set_create_data(self, application_name, create_data):
        logging.debug("Saving create data %s = %s", application_name, json.dumps(create_data))
        self._write_to_db(application_name, {'cf:create_data': json.dumps(create_data)})

    def get_create_data(self, application_name):
        logging.debug("Reading create data %s", application_name)
        return json.loads(self._read_from_db(application_name)['cf:create_data'])

    def delete_application(self, application_name):
        logging.debug("Deleting %s", application_name)
        connection = happybase.Connection(self._hbase_host)
        try:
            table = connection.table(self._table_name)
            table.delete(application_name)
        finally:
            connection.close()

    def get_application(self, application_name):
        logging.debug("Reading %s", application_name)
        application_data = self._read_from_db(application_name)
        if not application_data:
            return None
        return {'overrides': json.loads(application_data['cf:overrides']),
                'defaults': json.loads(application_data['cf:defaults']),
                'name': application_data['cf:name'],
                'package_name': application_data['cf:package_name'],
                'status': application_data['cf:status'],
                'information': application_data.get('cf:information', None)}

    def application_exists(self, application_name):
        logging.debug("Checking %s", application_name)
        application_data = self._read_from_db(application_name)
        if not application_data:
            return False
        # Note: this last line is problematic, as with the current API:
        # a "NOTCREATED" app can have an error state in the db
        return application_data['cf:status'] != ApplicationState.NOTCREATED

    def application_has_record(self, application_name):
        logging.debug("Checking %s", application_name)
        application_data = self._read_from_db(application_name)
        return not len(application_data) == 0

    def list_applications(self):
        logging.debug("List all applications")

        connection = happybase.Connection(self._hbase_host)
        try:
            table = connection.table(self._table_name)
            result = [decode(key) for key, data in table.scan(columns=[b'cf:status']) if decode(data[b'cf:status']) != ApplicationState.NOTCREATED]
        finally:
            connection.close()
        return result

    def list_applications_for_package(self, package_name):
        logging.debug("List applications for package %s", package_name)

        connection = happybase.Connection(self._hbase_host)
        try:
            table = connection.table(self._table_name)
            result = [key for key, data in table.scan(columns=[b'cf:package_name', b'cf:status'])
                      if decode(data[b'cf:package_name']) == package_name and decode(data[b'cf:status']) != ApplicationState.NOTCREATED]
        finally:
            connection.close()
        return result

    def generate_record(self, application_name, package_name, overrides, defaults):
        return application_name, {
            'cf:package_name': package_name,
            'cf:overrides': json.dumps(overrides),
            'cf:defaults': json.dumps(defaults),
            'cf:name': application_name,
            'cf:status': ApplicationState.NOTCREATED
        }

    def _read_from_db(self, key):
        connection = happybase.Connection(self._hbase_host)
        try:
            table = connection.table(self._table_name)
            data = table.row(encode(key))
        finally:
            connection.close()
        return decode(data)

    def _write_to_db(self, key, data):
        connection = happybase.Connection(self._hbase_host)
        try:
            table = connection.table(self._table_name)
            table.put(encode(key), encode(data))
        finally:
            connection.close()
