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
import shutil
import stat

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
        for command in create_data['delete_commands']:
            os.system(command)

    def start_component(self, application_name, create_data):
        logging.debug("start_component (nothing to do for jupyter): %s %s", application_name, json.dumps(create_data))

    def stop_component(self, application_name, create_data):
        logging.debug("stop_component (nothing to do for jupyter): %s %s", application_name, json.dumps(create_data))

    def create_component(self, staged_component_path, application_name, user_name, component, properties):
        logging.debug("create_component: %s %s %s %s", application_name, user_name, json.dumps(component), properties)
        application_user = properties['application_user']
        delete_commands = []

        ## Create local git repo for application_user if not exist.
        repo_path = '{}/jupyter-{}'.format(self._config['git_repos_root'], application_user)
        if os.path.isdir(repo_path) == False:
            os.makedirs(repo_path)
            os.system('git init {}'.format(repo_path))
            shutil.copyfile(repo_path+'/.git/hooks/post-update.sample', repo_path+'/.git/hooks/post-update')
            os.chmod(repo_path+'/.git/hooks/post-update',0o755)
														  
            this_dir = os.path.dirname(os.path.realpath(__file__))
            shutil.copyfile(this_dir+'/jupyter_README.ipynb',repo_path+'/README.ipynb')
            os.system('cd {0} && git add README.ipynb && git commit -m "Initial commit"'.format(repo_path))
        ## add notebooks to application_user github repo.
        notebook_install_path = '{}/{}'.format(repo_path, application_name)
        os.makedirs('{}'.format(notebook_install_path))
        file_list = component['component_detail']
        for file_name in file_list:
            # We copy all files in package to jupyter folder to let the user work with all kind of files/datasets.
            #if file_name.endswith(r'.ipynb'):
            if file_name != 'properties.json':
                if os.path.isfile('{}/{}'.format(staged_component_path,file_name)):
                    self._fill_properties('%s/%s' % (staged_component_path, file_name), properties)

                    logging.debug('Copying {} to {}'.format(file_name, notebook_install_path))
                    shutil.copyfile('{}/{}'.format(staged_component_path, file_name),
                          '{}/{}'.format(notebook_install_path, file_name ))
                else:
                    logging.debug('creating {}/{} folder'.format(notebook_install_path, file_name))
                    os.makedirs('{}/{}'.format(notebook_install_path, file_name))
        # Create a properties.json file in notebooks to access application jupyter component properties.
        with open('{}/properties.json'.format(notebook_install_path), 'w') as prop_file:
            prop_dict = { k.replace('component_',''): v for k, v in properties.items() if k.startswith('component_')}
            json.dump(prop_dict, prop_file)
            # update local github repo:
        
        os.system('cd {0} && git add {1} && git commit -m "added {1} app notebooks"'.format(repo_path, application_name))
        delete_commands.append('rm -rf {}\n'.format( notebook_install_path))
        delete_commands.append('cd {0} && git rm -r {1} && git commit -m "deleted {1} app notebooks"'.format( repo_path, application_name))


        logging.debug("uninstall commands: %s", delete_commands)
        return {'delete_commands': delete_commands}

