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
import subprocess
import tarfile
from io import BytesIO
import logging
import traceback
import time
from threading import Thread

import requests
import spur
from pywebhdfs.webhdfs import PyWebHdfsClient

def get_nameservice(cm_host, cluster_name, service_name, user_name='admin', password='admin'):
    request_url = 'http://%s:7180/api/v11/clusters/%s/services/%s/nameservices' % (cm_host,
                                                                                   cluster_name,
                                                                                   service_name)
    result = requests.get(request_url, auth=(user_name, password))
    nameservice = ""
    if result.status_code == 200:
        response = result.json()
        if 'items' in response:
            nameservice = response['items'][0]['name']
            logging.debug("Found named service %s for %s", nameservice, service_name)
    return nameservice

def update_hadoop_env(env):
    # Update the env in a way that ensure values are only updated in the main descriptor and never removed
    # so that any caller will always be able to query the values it expects to find in the env descriptor
    #   1. copy the environment descriptor
    #   2. update the temporary copy
    #   3. push the temporary values into the main descriptor
    tmp_env = dict(env)
    logging.debug('Updating environment descriptor')
    if env['hadoop_distro'] == 'CDH':
        logging.error('CDH is not a supported hadoop distribution')
    elif env['hadoop_distro'] == 'HDP':
        fill_hadoop_env_hdp(tmp_env)
    else:
        logging.warning('Skipping update_hadoop_env for hadoop distro "%s"', env['hadoop_distro'])
    logging.debug('Updated environment descriptor')
    for key in tmp_env:
        # Dictionary get/put operations are atomic so inherently thread safe and don't need a lock
        env[key] = tmp_env[key]
    logging.debug(env)

def monitor_hadoop_env(env, config):
    while True:
        try:
            update_hadoop_env(env)
        except Exception:
            logging.error("Environment sync failed")
            logging.error(traceback.format_exc())

        sleep_seconds = config['environment_sync_interval']
        logging.debug('Next environment sync will be in %s seconds', sleep_seconds)
        time.sleep(sleep_seconds)

def fill_hadoop_env(env, config):
    update_hadoop_env(env)
    env_monitor_thread = Thread(target=monitor_hadoop_env, args=[env, config])
    env_monitor_thread.daemon = True
    env_monitor_thread.start()

def ambari_request(ambari, uri):
    hadoop_manager_ip = ambari[0]
    hadoop_manager_username = ambari[1]
    hadoop_manager_password = ambari[2]
    if uri.startswith("http"):
        full_uri = uri
    else:
        full_uri = 'http://%s:8080/api/v1%s' % (hadoop_manager_ip, uri)

    headers = {'X-Requested-By': hadoop_manager_username}
    auth = (hadoop_manager_username, hadoop_manager_password)
    return requests.get(full_uri, auth=auth, headers=headers).json()

def get_hdfs_hdp(ambari, cluster_name):
    core_site = ambari_request(ambari, '/clusters/%s?fields=Clusters/desired_configs/core-site' % cluster_name)
    config_version = core_site['Clusters']['desired_configs']['core-site']['tag']
    core_site_config = ambari_request(ambari, '/clusters/%s/configurations/?type=core-site&tag=%s' % (cluster_name, config_version))
    return core_site_config['items'][0]['properties']['fs.defaultFS']

def component_host(component_detail):
    host_list = ''
    for host_detail in component_detail['host_components']:
        if host_list:
            host_list += ','
        host_list += host_detail['HostRoles']['host_name']
    return host_list

def fill_hadoop_env_hdp(env):

    hadoop_manager_ip = env['hadoop_manager_host']
    hadoop_manager_username = env['hadoop_manager_username']
    hadoop_manager_password = env['hadoop_manager_password']
    ambari = (hadoop_manager_ip, hadoop_manager_username, hadoop_manager_password)
    cluster_name = ambari_request(ambari, '/clusters')['items'][0]['Clusters']['cluster_name']

    logging.debug('getting service list for %s', cluster_name)
    env['cm_status_links'] = {}

    env['name_node'] = get_hdfs_hdp(ambari, cluster_name)

    services = ambari_request(ambari, '/clusters/%s/services' % cluster_name)['items']
    for service in services:
        service_name = service['ServiceInfo']['service_name']
        env['cm_status_links']['%s' % service_name] = 'http://%s:8080/#/main/services/%s/summary' % (hadoop_manager_ip, service_name)
        service_components = ambari_request(ambari, service['href'] + '/components')['items']

        for component in service_components:
            component_detail = ambari_request(ambari, component['href'])
            role_name = component_detail['ServiceComponentInfo']['component_name']

            if role_name == "NAMENODE":
                env['webhdfs_host'] = '%s' % component_host(component_detail).split(',')[0]
                env['webhdfs_port'] = '14000'

            elif role_name == "RESOURCEMANAGER":
                rm_host = component_host(component_detail)
                if len(rm_host.split(',')) > 1:
                    main_rm_host = rm_host.split(',')[0]
                    backup_rm_host = rm_host.split(',')[1]
                else:
                    main_rm_host = rm_host
                    backup_rm_host = None
                env['yarn_resource_manager_host'] = '%s' % main_rm_host
                env['yarn_resource_manager_port'] = '8088'
                env['yarn_resource_manager_mr_port'] = '8050'
                if backup_rm_host is not None:
                    env['yarn_resource_manager_host_backup'] = '%s' % component_host(component_detail)
                    env['yarn_resource_manager_port_backup'] = '8088'
                    env['yarn_resource_manager_mr_port_backup'] = '8050'

            elif role_name == "NODEMANAGER":
                env['yarn_node_managers'] = '%s' % component_host(component_detail)

            elif role_name == "ZOOKEEPER_SERVER":
                env['zookeeper_quorum'] = '%s' % component_host(component_detail)
                env['zookeeper_port'] = '2181'

            elif role_name == "HBASE_MASTER":
                env['hbase_rest_server'] = '%s' % component_host(component_detail).split(',')[0]
                env['hbase_rest_port'] = '20550'
                env['hbase_thrift_server'] = '%s' % component_host(component_detail).split(',')[0]

            elif role_name == "OOZIE_SERVER":
                env['oozie_uri'] = 'http://%s:11000/oozie' % component_host(component_detail)

            elif role_name == "HIVE_SERVER":
                env['hive_server'] = '%s' % component_host(component_detail)
                env['hive_port'] = '10001'

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

    def recursive_copy(self, local_path, remote_path, exclude=None, permission=755):

        if exclude is None:
            exclude = []

        c_path = canonicalize(remote_path)
        logging.debug('making %s', c_path)
        self._hdfs.make_dir(c_path, permission=permission)

        fs_g = os.walk(local_path)
        for dpath, dnames, fnames in fs_g:
            _, relative_path = dpath.split(local_path)
            for dname in dnames:
                if dname not in exclude:
                    c_path = canonicalize(
                        '%s/%s/%s' %
                        (remote_path, relative_path, dname))
                    logging.debug('making %s', c_path)
                    self._hdfs.make_dir(c_path, permission=permission)

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
                    self._hdfs.create_file(c_path, data, overwrite=True, permission=permission)
                    data.close()

    def make_dir(self, path, permission=755):

        logging.debug('make_dir: %s', path)

        self._hdfs.make_dir(canonicalize(path), permission=permission)

    def create_file(self, data, remote_file_path, permission=755):

        logging.debug('create_file: %s', remote_file_path)

        sio = BytesIO(data)

        self._hdfs.create_file(
            canonicalize(remote_file_path),
            sio,
            overwrite=True,
            permission=permission)

    def append_file(self, data, remote_file_path):

        logging.debug('append to: %s', remote_file_path)

        self._hdfs.append_file(canonicalize(remote_file_path), data)


    def stream_file_to_disk(self, remote_file_path, local_file_path):
        chunk_size = 10*1024*1024
        offset = 0
        logging.debug("Stream file to disk : %s,%s"%(remote_file_path, local_file_path))
        with open(local_file_path, 'wb') as dest_file:
            data = self._hdfs.read_file(canonicalize(remote_file_path), offset=offset, length=chunk_size)
            while True:
                dest_file.write(data)
                if len(data) < chunk_size:
                    break
                offset += chunk_size
                data = self._hdfs.read_file(canonicalize(remote_file_path), offset=offset, length=chunk_size)

    def read_file(self, remote_file_path):

        data = self._hdfs.read_file(canonicalize(remote_file_path))

        return data

    def remove(self, path, recursive=False):

        logging.debug('remove: %s', path)

        self._hdfs.delete_file_dir(canonicalize(path), recursive)

    def file_exists(self, path):

        try:
            self._hdfs.get_file_dir_status(path)
            return True
        except:
            return False

def exec_ssh(host, user, key, ssh_commands):
    shell = spur.SshShell(
        hostname=host,
        username=user,
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

def exec_cmds(commands):
    for cmd in commands:
        try:
            subprocess.check_output(cmd.split())
        except subprocess.CalledProcessError as e:
            logging.error(e.output)


def dict_to_props(dict_props):
    props = []
    for key, value in dict_props.items():
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

