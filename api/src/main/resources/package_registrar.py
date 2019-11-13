"""
Name:       package_registrar.py
Purpose:    Stores packages and their metadata in hbase
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

from package_parser import PackageParser
from deployer_utils import HDFS

from exceptiondef import FailedConnection

from hbase_utils import encode,decode

class HbasePackageRegistrar(object):
    COLUMN_DEPLOY_STATUS = 'cf:deploy_status'

    def __init__(self, hbase_host, hdfs_host, hdfs_user, hdfs_port, package_local_dir_path):
        self._hbase_host = hbase_host
        self._hdfs_user = hdfs_user
        self._hdfs_host = hdfs_host
        self._hdfs_port = hdfs_port
        self._hdfs_client = HDFS(hdfs_host, hdfs_port, hdfs_user)
        self._parser = PackageParser()
        self._table_name = 'platform_packages'
        self._dm_root_dir_path = "/pnda/system/deployment-manager"
        self._package_hdfs_dir_path = "%s/packages" % self._dm_root_dir_path
        self._package_local_dir_path = package_local_dir_path

        try:
            if hdfs_host is not None:
                self._hdfs_client.make_dir(self._dm_root_dir_path, permission=755)
                self._hdfs_client.make_dir(self._package_hdfs_dir_path, permission=600)
                logging.debug("packages HDFS folder created")
            else:
                logging.debug("not creating packages HDFS folder as it is not required")
        except AlreadyExists:
            logging.debug("not creating packages HDFS folder as it already exists")

        if self._hbase_host is not None:
            connection = happybase.Connection(self._hbase_host)
            try:
                connection.create_table(self._table_name, {'cf': dict()})
                logging.debug("packages table created")
            except AlreadyExists:
                logging.debug("packages table exists")
            finally:
                connection.close()

    def set_package(self, package_name, package_data_path, user):
        logging.debug("Storing %s", package_name)
        metadata = self._parser.get_package_metadata(package_data_path)
        metadata['user'] = user
        key, data = self.generate_record(metadata)
        self._write_to_hdfs(package_data_path, data['cf:package_data'])
        self._write_to_db(key, data)

    def set_package_deploy_status(self, package_name, deploy_status):
        """
        Stores information about the progress of the deploy process of the package
        :param deploy_status: the state to store
        """
        logging.debug("Storing state for %s: %s", package_name, str(deploy_status))
        state_as_string = json.dumps(deploy_status)
        self._write_to_db(package_name, {self.COLUMN_DEPLOY_STATUS: state_as_string})

    def delete_package(self, package_name):
        logging.debug("Deleting %s", package_name)
        package_data_hdfs_path = self._read_from_db(package_name, ['cf:package_data'])['cf:package_data']
        self._hdfs_client.remove(package_data_hdfs_path)
        connection = happybase.Connection(self._hbase_host)
        try:
            table = connection.table(self._table_name)
            table.delete(package_name)
        finally:
            connection.close()

    def get_package_data(self, package_name):
        logging.debug("Reading %s", package_name)
        record = self._read_from_db(package_name, ['cf:package_data'])
        if not record:
            return None
        local_package_path = "%s/%s" % (self._package_local_dir_path, package_name)
        self._read_from_hdfs(record['cf:package_data'], local_package_path)
        return local_package_path

    def get_package_metadata(self, package_name):
        logging.debug("Reading %s", package_name)
        package_data = self._read_from_db(
            package_name, ['cf:metadata', 'cf:name', 'cf:version'])
        if not package_data:
            return None
        return {"metadata": json.loads(package_data['cf:metadata']), "name": package_data[
            'cf:name'], "version": package_data['cf:version']}

    def package_exists(self, package_name):
        logging.debug("Checking %s", package_name)
        package_data = self._read_from_db(package_name, ['cf:name'])
        return len(package_data) > 0

    def get_package_deploy_status(self, package_name):
        """
        :param package_name: the package name to check status for
        :return: The last reported progress of the deploy process for the current package
        """
        logging.debug("Checking %s", package_name)
        package_data = self._read_from_db(package_name, columns=[self.COLUMN_DEPLOY_STATUS])
        if not package_data:
            return None
        # all status is stored as json, so parse it and return it
        deploy_status_as_string = package_data[self.COLUMN_DEPLOY_STATUS]
        return json.loads(deploy_status_as_string)

    def list_packages(self):
        logging.debug("List all packages")

        connection = None
        try:
            connection = happybase.Connection(self._hbase_host)
            table = connection.table(self._table_name)
            result = [key.decode() for key, _ in table.scan(columns=['cf:name'])]
        except Exception as exc:
            logging.debug(str(exc))
            raise FailedConnection('Unable to connect to the HBase master')
        finally:
            if connection:
                connection.close()
        return result

    def generate_record(self, metadata):
        return metadata["package_name"], {
            'cf:name': '-'.join(metadata["package_name"].split("-")[:-1]),
            'cf:version': metadata["package_name"].split("-")[-1],
            'cf:metadata': json.dumps(metadata),
            'cf:package_data': "%s/%s" % (self._package_hdfs_dir_path, metadata["package_name"])
        }

    def _read_from_db(self, key, columns):
        connection = happybase.Connection(self._hbase_host)
        try:
            table = connection.table(self._table_name)
            data = table.row(encode(key), columns=encode(columns))
        finally:
            connection.close()
        
        return decode(data)

    def _read_from_hdfs(self, source_hdfs_path, dest_local_path):
        self._hdfs_client.stream_file_to_disk(source_hdfs_path, dest_local_path)

    def _write_to_db(self, key, data):
        connection = happybase.Connection(self._hbase_host)
        try:
            table = connection.table(self._table_name)
            table.put(encode(key), encode(data))
        finally:
            connection.close()

    def _write_to_hdfs(self, source_local_path, dest_hdfs_path):
        with open(source_local_path, 'rb') as source_file:
            first = True
            chunk_size = 10*1024*1024
            data_chunk = source_file.read(chunk_size)
            while data_chunk:
                if first:
                    self._hdfs_client.create_file(data_chunk, dest_hdfs_path, permission=600)
                    first = False
                else:
                    self._hdfs_client.append_file(data_chunk, dest_hdfs_path)
                data_chunk = source_file.read(chunk_size)
