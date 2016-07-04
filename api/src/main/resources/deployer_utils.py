"""
Name:       deployer_utils.py
Purpose:    Utility functions for general usage in the project
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

import os
import tarfile
import StringIO
import logging
import traceback
import spur
from pywebhdfs.webhdfs import PyWebHdfsClient

import requests
from cm_api.api_client import ApiResource


def connect_cm(cm_api, cm_username, cm_password):
    api = ApiResource(
        cm_api,
        version=6,
        username=cm_username,
        password=cm_password)
    return api


def get_named_service(cm_host, cluster_name, service_name, user_name='admin', password='admin'):
    request_url = 'http://%s:7180/api/v11/clusters/%s/services/%s/nameservices' % (cm_host,
                                                                                   cluster_name,
                                                                                   service_name)
    result = requests.get(request_url, auth=(user_name, password))
    named_service = ""
    if result.status_code == 200:
        response = result.json()
        if 'items' in response:
            named_service = response['items'][0]['name']
            logging.debug("Found named service %s for %s", named_service, service_name)
    return named_service


def fill_hadoop_env(env):
    # pylint: disable=E1103
    api = connect_cm(
        env['cloudera_manager_host'],
        env['cloudera_manager_username'],
        env['cloudera_manager_password'])

    for cluster_detail in api.get_all_clusters():
        cluster_name = cluster_detail.name
        break

    logging.debug('getting ' + cluster_name)
    env['cm_status_links'] = {}

    cluster = api.get_cluster(cluster_name)
    for service in cluster.get_all_services():
        env['cm_status_links']['%s' % service.name] = service.serviceUrl
        if service.type == "HDFS":
            named_service = get_named_service(env['cloudera_manager_host'], cluster_name,
                                              service.name,
                                              user_name=env['cloudera_manager_username'],
                                              password=env['cloudera_manager_password'])
            if named_service:
                env['name_node'] = 'hdfs://%s' % named_service
            for role in service.get_all_roles():
                if not named_service and role.type == "NAMENODE":
                    env['name_node'] = 'hdfs://%s:8020' % api.get_host(role.hostRef.hostId).hostname
                if role.type == "HTTPFS":
                    env['webhdfs_host'] = '%s' % api.get_host(role.hostRef.hostId).ipAddress
                    env['webhdfs_port'] = '14000'
        elif service.type == "YARN":
            for role in service.get_all_roles():
                if role.type == "RESOURCEMANAGER":
                    env['yarn_resource_manager_host'] = '%s' % api.get_host(
                        role.hostRef.hostId).hostname
                    env['yarn_resource_manager_port'] = '8088'
                    env['yarn_resource_manager_mr_port'] = '8032'
                if role.type == "NODEMANAGER":
                    if 'yarn_node_managers' in env:
                        env['yarn_node_managers'] = '%s,%s' % (
                            env['yarn_node_managers'], api.get_host(role.hostRef.hostId).hostname)
                    else:
                        env['yarn_node_managers'] = '%s' % api.get_host(
                            role.hostRef.hostId).hostname
        elif service.type == "MAPREDUCE":
            for role in service.get_all_roles():
                if role.type == "JOBTRACKER":
                    env['job_tracker'] = '%s:8021' % api.get_host(
                        role.hostRef.hostId).hostname
                    break
        elif service.type == "ZOOKEEPER":
            for role in service.get_all_roles():
                if role.type == "SERVER":
                    if 'zookeeper_quorum' in env:
                        env['zookeeper_quorum'] += ',%s' % api.get_host(role.hostRef.hostId).hostname
                    else:
                        env['zookeeper_quorum'] = '%s' % api.get_host(role.hostRef.hostId).hostname
                        env['zookeeper_port'] = '2181'
        elif service.type == "HBASE":
            for role in service.get_all_roles():
                if role.type == "HBASERESTSERVER":
                    env['hbase_rest_server'] = '%s' % api.get_host(
                        role.hostRef.hostId).hostname
                    env['hbase_rest_port'] = '20550'
                    break
        elif service.type == "OOZIE":
            for role in service.get_all_roles():
                if role.type == "OOZIE_SERVER":
                    env['oozie_uri'] = 'http://%s:11000/oozie' % api.get_host(
                        role.hostRef.hostId).hostname
                    break
        elif service.type == "HIVE":
            for role in service.get_all_roles():
                if role.type == "HIVESERVER2":
                    env['hive_server'] = '%s' % api.get_host(
                        role.hostRef.hostId).hostname
                    env['hive_port'] = '10000'
                    break
        elif service.type == "IMPALA":
            for role in service.get_all_roles():
                if role.type == "IMPALAD":
                    env['impala_host'] = '%s' % api.get_host(
                        role.hostRef.hostId).hostname
                    env['impala_port'] = '21050'
                    break
        elif service.type == "KUDU":
            for role in service.get_all_roles():
                if role.type == "KUDU_MASTER":
                    env['kudu_host'] = '%s' % api.get_host(
                        role.hostRef.hostId).hostname
                    env['kudu_port'] = '7051'
                    break
        elif service.type == "HUE":
            for role in service.get_all_roles():
                if role.type == "HUE_SERVER":
                    env['hue_host'] = '%s' % api.get_host(
                        role.hostRef.hostId).hostname
                    env['hue_port'] = '8888'
                    break

    logging.debug(env)


def tree(archive_filepath):
    file_handle = file(archive_filepath, 'rb')
    tar_file = tarfile.open(None, 'r', file_handle)
    table = tar_file.getmembers()

    root = {}
    for member in table:
        path = member.name.split('/')
        node = root
        for part in path:
            if part not in node:
                node[part] = {}
            node = node[part]

    return root


def canonicalize(path):
    path = path.replace('\\', '/')
    path = path.replace('//', '/')
    return path


class HDFS(object):
    def __init__(self, host, port, user):
        self._hdfs = PyWebHdfsClient(
            host=host, port=port, user_name=user, timeout=None)
        logging.debug('webhdfs = %s@%s:%s', user, host, port)

    def recursive_copy(self, local_path, remote_path, exclude=None):

        if exclude is None:
            exclude = []

        c_path = canonicalize(remote_path)
        logging.debug('making %s', c_path)
        self._hdfs.make_dir(c_path)

        fs_g = os.walk(local_path)
        for dpath, dnames, fnames in fs_g:
            _, relative_path = dpath.split(local_path)
            for dname in dnames:
                if dname not in exclude:
                    c_path = canonicalize(
                        '%s/%s/%s' %
                        (remote_path, relative_path, dname))
                    logging.debug('making %s', c_path)
                    self._hdfs.make_dir(c_path)

            for fname in fnames:
                if fname not in exclude:
                    data = file(
                        canonicalize(
                            '%s/%s/%s' %
                            (local_path, relative_path, fname)), 'rb')
                    c_path = canonicalize(
                        '%s/%s/%s' %
                        (remote_path, relative_path, fname))
                    logging.debug('creating %s', c_path)
                    self._hdfs.create_file(c_path, data, overwrite=True)
                    data.close()

    def make_dir(self, path):

        logging.debug('make_dir: %s', path)

        self._hdfs.make_dir(canonicalize(path))

    def create_file(self, data, remote_file_path):

        logging.debug('create_file: %s', remote_file_path)

        sio = StringIO.StringIO(data)

        self._hdfs.create_file(
            canonicalize(remote_file_path),
            sio,
            overwrite=True)

    def read_file(self, remote_file_path):

        data = self._hdfs.read_file(canonicalize(remote_file_path))

        return data

    def remove(self, path, recursive=False):

        logging.debug('remove: %s', path)

        self._hdfs.delete_file_dir(canonicalize(path), recursive)


def exec_ssh(host, user, key, ssh_commands):
    shell = spur.SshShell(
        hostname=host,
        username=user,
        private_key_file=key,
        missing_host_key=spur.ssh.MissingHostKey.accept)
    with shell:
        for ssh_command in ssh_commands:
            logging.debug('Host - %s: Command - %s', host, ssh_command)
            try:
                shell.run(["bash", "-c", ssh_command])
            except spur.results.RunProcessError as exception:
                logging.error(
                    ssh_command +
                    " - error: " +
                    traceback.format_exc(exception))


def dict_to_props(dict_props):
    props = []
    for key, value in dict_props.iteritems():
        props.append('%s=%s' % (key, value))
    return '\n'.join(props)


def dict_to_xml(dict_props):
    xml_header = '<?xml version="1.0" encoding="UTF-8" ?>'
    xml_string = '<configuration>'
    for key, value in dict_props.items():
        xml_string += '<property>' + \
                      '<name>' + key + '</name>' + \
                      '<value>' + str(value) + '</value>' + \
                      '</property>'
    xml_string += '</configuration>'
    return xml_header + xml_string
