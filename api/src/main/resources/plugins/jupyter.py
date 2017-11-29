"""
Name:       jupyter.py
Purpose:    Installs a jupyter notebook
Author:     PNDA team

Created:    03/10/2016

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

# pylint: disable=C0103

import json
import os
import logging

import deployer_utils
from plugins.base_creator import Creator


class JupyterCreator(Creator):

    def validate_component(self, component):
        errors = []
        notebook_found = False
        file_list = component['component_detail']
        for file_name in file_list:
            if file_name.endswith(r'.ipynb'):
                notebook_found = True

        if notebook_found is False:
            errors.append('missing ipynb file')

        return errors

    def get_component_type(self):
        return 'jupyter'

    def destroy_component(self, application_name, create_data):
        logging.debug("destroy_component: %s %s", application_name, json.dumps(create_data))
        key_file = self._environment['cluster_private_key']
        root_user = self._environment['cluster_root_user']
        target_host = self._environment['jupyter_host']
        deployer_utils.exec_ssh(target_host, root_user, key_file, create_data['delete_commands'])

    def start_component(self, application_name, create_data):
        logging.debug("start_component (nothing to do for jupyter): %s %s", application_name, json.dumps(create_data))

    def stop_component(self, application_name, create_data):
        logging.debug("stop_component (nothing to do for jupyter): %s %s", application_name, json.dumps(create_data))

    def create_component(self, staged_component_path, application_name, user_name, component, properties):
        logging.debug("create_component: %s %s %s %s", application_name, user_name, json.dumps(component), properties)

        key_file = self._environment['cluster_private_key']
        root_user = self._environment['cluster_root_user']
        target_host = self._environment['jupyter_host']
        application_user = properties['application_user']
        delete_commands = []

        mkdircommands = []
        remote_component_tmp_path = '%s/%s/%s/%s' % ('/tmp/%s' % self._namespace, application_user, application_name, component['component_name'])
        remote_notebook_path = '/home/%s/%s' % (application_user, self._environment['jupyter_notebook_directory'])
        mkdircommands.append('mkdir -p %s' % remote_component_tmp_path)
        mkdircommands.append('sudo -u %s mkdir -p %s' % (application_user, remote_notebook_path))
        deployer_utils.exec_ssh(target_host, root_user, key_file, mkdircommands)

        file_list = component['component_detail']
        for file_name in file_list:
            if file_name.endswith(r'.ipynb'):
                self._fill_properties('%s/%s' % (staged_component_path, file_name), properties)
                logging.debug('Copying %s/* to %s:%s', staged_component_path, target_host, remote_component_tmp_path)
                os.system("scp -i %s -o StrictHostKeyChecking=no %s/%s %s@%s:%s" %
                          (key_file, staged_component_path, file_name, root_user, target_host, remote_component_tmp_path))

                remote_component_install_path = '%s/%s_%s' % (remote_notebook_path, application_name, file_name)
                deployer_utils.exec_ssh(
                    target_host, root_user, key_file,
                    ['sudo mv %s %s' % (remote_component_tmp_path + '/*.ipynb', remote_component_install_path)])
                delete_commands.append('sudo rm -rf %s\n' % remote_component_install_path)

        logging.debug("uninstall commands: %s", delete_commands)
        return {'delete_commands': delete_commands}
