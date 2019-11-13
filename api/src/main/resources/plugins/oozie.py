"""
Name:       oozie.py
Purpose:    Submits oozie coordinator or workflow based on application package
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

# pylint: disable=C0103

import os
import json
import logging
import datetime
import xml.etree.ElementTree as ElementTree
import commands
import shutil
import traceback
import requests

import deployer_utils
from plugins.base_creator import Creator
from exceptiondef import FailedCreation, FailedValidation

class OozieCreator(Creator):

    def validate_component(self, component):
        errors = []
        file_list = component['component_detail']
        if 'workflow.xml' not in file_list:
            errors.append('missing file workflow.xml')
        return errors

    def get_component_type(self):
        return 'oozie'

    def assert_application_properties(self, override_properties, default_properties):
        # Assert application properties before creating the application
        for component in default_properties:
            properties = default_properties[component].copy()
            properties.update(override_properties.get(component, {}))
            if 'spark_version' in properties and properties['spark_version'] != self._config['oozie_spark_version']:
                information = "Mismatch between cluster's oozie spark version (version = %s) and \
requested spark version" % self._config['oozie_spark_version']
                raise FailedValidation(json.dumps({"information": information}))
            properties = None

    def exec_cmds(self, exec_commands):
        key_file = self._environment['cluster_private_key']
        root_user = self._environment['cluster_root_user']
        target_host = 'localhost'
        deployer_utils.exec_ssh(target_host, root_user, key_file, exec_commands)

    def stage_flink_components(self, application_name, component_name, properties, staged_component_path):
        component_install_path = '%s/%s/%s' % (
            '/opt/%s' % self._namespace, application_name, component_name)
        properties['component_staged_path'] = component_install_path

        this_dir = os.path.dirname(os.path.realpath(__file__))

        if 'component_main_jar' in properties:
            service_script = 'flink-oozie.execute.sh.tpl'
        elif 'component_main_py' in properties:
            service_script = 'flink-oozie.execute.py.sh.tpl'
            flink_lib_dir = properties['environment_flink_lib_dir']
            for jar in os.listdir(flink_lib_dir):
                if os.path.isfile(os.path.join(flink_lib_dir, jar)) and 'flink-python' in jar:
                    properties['flink_python_jar'] = '%s/%s' % (flink_lib_dir, jar)
        else:
            raise Exception('properties.json must contain "main_jar or main_py" for %s flink-batch-job %s' % (application_name, component_name))

        shutil.copyfile(os.path.join(this_dir, 'flink-stop.py'), '%s/lib/flink-stop.py' % staged_component_path)
        shutil.copyfile(os.path.join(this_dir, service_script), '%s/lib/%s' % (staged_component_path, service_script))
        self._fill_properties(os.path.join('%s/lib' % staged_component_path, "flink-stop.py"), properties)
        self._fill_properties(os.path.join('%s/lib' % staged_component_path, service_script), properties)

        mkdir_commands = []
        mkdir_commands.append('sudo mkdir -p %s' % component_install_path)
        self.exec_cmds(mkdir_commands)

        os.system("cp -r %s %s"
                % (staged_component_path + '/lib/*', component_install_path))

        copy_commands = []
        copy_commands.append('sudo mv  %s/%s %s/execute.sh' % (component_install_path, service_script, component_install_path))
        copy_commands.append('sudo chmod 777 %s/execute.sh' % (component_install_path))
        self.exec_cmds(copy_commands)

        # adding flink_client host and script path to properties
        flink_host = "%s@%s" % (self._environment['cluster_root_user'], self._environment['flink_host'])
        properties['flink_client'] = flink_host
        properties['path_to_script'] = '%s/execute.sh' % component_install_path

    def destroy_component(self, application_name, create_data):
        logging.debug(
            "destroy_component: %s %s",
            application_name,
            json.dumps(create_data))
        # terminate oozie jobs
        self._kill_oozie(create_data['job_handle'], create_data['application_user'])

        # delete component from hdfs
        remote_path = create_data['component_hdfs_root'][1:]
        self._hdfs_client.remove(remote_path, recursive=True)

        # stop flink job and delete component from local
        if "flink_staged_path" in create_data:
            destroy_commands = ["python %s/flink-stop.py" % create_data["flink_staged_path"],
                           "sudo rm -rf %s\n" % create_data["flink_staged_path"]]
            self.exec_cmds(destroy_commands)

    def start_component(self, application_name, create_data):
        logging.debug("start_component: %s %s", application_name, json.dumps(create_data))
        self._start_oozie(create_data['job_handle'], create_data['application_user'])

    def stop_component(self, application_name, create_data):
        logging.debug("stop_component: %s %s", application_name, json.dumps(create_data))
        self._stop_oozie(create_data['job_handle'], create_data['application_user'])

        # stop flink job
        if "flink_staged_path" in create_data:
            stop_commands = ["python %s/flink-stop.py" % create_data["flink_staged_path"]]
            self.exec_cmds(stop_commands)

    def create_component(self, staged_component_path, application_name, user_name, component, properties):
        logging.debug(
            "create_component: %s %s %s %s",
            application_name,
            user_name,
            json.dumps(component),
            properties)

        remote_path = properties['component_hdfs_root'][1:]

        # insert deployment properties, these should be used
        # as defaults that can be overridden by component properties
        # Set the start time to 2 minutes in the future so that oozie coordinator jobs will not start any
        # actions when submitted, allowing it to be immediately suspended without doing anything first,
        # as all applications must start off in a paused state and receive a start command to begin.
        start = datetime.datetime.utcnow()
        delta_start = datetime.timedelta(minutes=2)
        delta_end = datetime.timedelta(weeks=1)
        start = start + delta_start
        end = start + delta_end
        properties['deployment_start'] = start.strftime("%Y-%m-%dT%H:%MZ")
        properties['deployment_end'] = end.strftime("%Y-%m-%dT%H:%MZ")

        # for flink jobs, code need to be staged locally because both ssh action and flink client requires code to be present in local
        if properties.get('component_job_type','') == 'flink':
            self.stage_flink_components(application_name, component['component_name'], properties, staged_component_path)

        # insert required oozie properties
        properties['user.name'] = properties['application_user']
        # Oozie ShareLib - supports actions
        properties['oozie.use.system.libpath'] = 'true'
        # platform shared libs e.g. hbase
        properties['oozie.libpath'] = '/pnda/deployment/platform'
        # For spark2 add a special setting to select spark2
        if self._config['oozie_spark_version'] == '2':
            properties['oozie.action.sharelib.for.spark'] = 'spark2'

        # insert default queue selection
        ret, res = commands.getstatusoutput('sudo -u %s %s' % (properties['application_user'], self._environment['queue_policy']))
        if ret == 0:
            properties['mapreduce.job.queuename'] = res
        else:
            logging.error("Policy: ERROR %s: %s", ret, res)

        # insert reference to coordinator xor workflow
        if 'coordinator.xml' in component['component_detail']:
            def_path = 'oozie.coord.application.path'
        else:
            def_path = 'oozie.wf.application.path'

        properties[def_path] = '%s/%s' % (self._environment['name_node'], remote_path)

        # deploy everything to various hadoop services
        undeploy = self._deploy_to_hadoop(component, properties, staged_component_path, remote_path, properties['application_user'])

        # return something that can be used to undeploy later
        ret_data = {}

        # if code staged locally in case of flink, add flink local staged path in return data for other oprations
        if "component_staged_path" in properties:
            ret_data["flink_staged_path"] = properties["component_staged_path"]

        ret_data.update({'job_handle': undeploy['id'],
                'component_hdfs_root': properties['component_hdfs_root'],
                'application_user': properties['application_user']})
        return ret_data

    def _setup_queue_config(self, component, staged_component_path, properties):
        # Add queue config into the default config if none is defined.
        if 'mapreduce.job.queuename' in properties:
            defaults = {'mapreduce.job.queuename':properties['mapreduce.job.queuename']}
            try:
                with open('%s/config-default.xml' % staged_component_path, 'r') as config_default_file:
                    data = config_default_file.read()
            except:
                logging.debug('No config-default.xml is detected.')
                data = None

            if data is None:
                logging.debug('Creating config-default.xml to inject mapreduce.job.queuename property.')
                with open('%s/config-default.xml' % staged_component_path, 'w') as config_default_file:
                    config_default_file.write(deployer_utils.dict_to_xml(defaults))
            else:
                prop = None
                root = None
                try:
                    root = ElementTree.fromstring(data)
                    prop = root.find("./property/[name='mapreduce.job.queuename']")
                except:
                    logging.error('Failed to parse the config-default.xml data.')

                if root is not None:
                    if prop is not None:
                        try:
                            queue = prop.find('value').text
                            logging.debug('mapreduce.job.queuename is already set: %s', queue)
                        except:
                            logging.error('config-default.xml [\'mapred.queue.names\'] has no value.')

                    else:
                        logging.debug('adding mapred.queue.names in config-default.xml')
                        prop = ElementTree.SubElement(root, 'property')
                        ElementTree.SubElement(prop, 'name').text = 'mapreduce.job.queuename'
                        ElementTree.SubElement(prop, 'value').text = properties['mapreduce.job.queuename']
                        data = ElementTree.tostring(root)
                        with open('%s/config-default.xml' % staged_component_path, 'w') as config_default_file:
                            config_default_file.write(data)

            file_list = [file_name for file_name in component['component_detail'] if os.path.isfile('%s/%s' % (staged_component_path, file_name))]
            # find workflow.xml files
            for afile in file_list:
                workflow_modified = False
                file_path = '%s/%s' % (staged_component_path, afile)
                with open(file_path, 'r') as component_file:
                    workflow_xml = component_file.read()
                    if 'uri:oozie:workflow' not in workflow_xml:
                        continue
                logging.debug("Found workflow file %s", file_path)
                # copy config-default.xml into this directory
                if os.path.dirname(file_path) != staged_component_path:
                    shutil.copyfile('%s/config-default.xml' % staged_component_path, '%s/config-default.xml' % os.path.dirname(file_path))

                # set the spark opts --queue so spark jobs are put in the right queue
                spark_action_index = 0
                while spark_action_index >= 0:
                    spark_action_index = workflow_xml.find('<spark ', spark_action_index+1)
                    spark_end_index = workflow_xml.find('</spark>', spark_action_index)
                    jar_end_index = workflow_xml.find('</jar>', spark_action_index, spark_end_index)
                    opts_index = workflow_xml.find('<spark-opts>', spark_action_index, spark_end_index)
                    opts_end_index = workflow_xml.find('</spark-opts>', opts_index, spark_end_index)
                    queue_opt_index = workflow_xml.find('--queue ', opts_index, opts_end_index)
                    if jar_end_index >= 0:
                        if opts_index < 0:
                            # we need to add a spark-opts element
                            split_index = jar_end_index+len('</jar>')
                            workflow_xml = '%s%s%s' % (workflow_xml[:split_index],
                                                       '<spark-opts>--queue ${wf:conf("mapreduce.job.queuename")}</spark-opts>',
                                                       workflow_xml[split_index:])
                            workflow_modified = True
                        elif queue_opt_index < 0:
                            # we need to add a queue opt to the existing spark-opts element
                            split_index = opts_end_index
                            workflow_xml = '%s%s%s' % (workflow_xml[:split_index], ' --queue ${wf:conf("mapreduce.job.queuename")}', workflow_xml[split_index:])
                            workflow_modified = True

                # write out modified workflow if changes were made
                if workflow_modified:
                    logging.debug("Writing out modified workflow xml to %s", file_path)
                    with open(file_path, "w") as workflow_file:
                        workflow_file.write(workflow_xml)

    def _deploy_to_hadoop(self, component, properties, staged_component_path, remote_path, application_user, exclude=None):
        if exclude is None:
            exclude = []
        exclude.extend(['hdfs.json',
                        'hbase.json',
                        'properties.json',
                        'application.properties'])

        # setup queue config
        try:
            self._setup_queue_config(component, staged_component_path, properties)
        except Exception as ex:
            logging.error(traceback.format_exc())
            raise FailedCreation('Failed to set up yarn queue config: %s' % str(ex))

        # stage the component files to hdfs
        self._hdfs_client.recursive_copy(staged_component_path, remote_path, exclude=exclude, permission=755)

        # stage the instantiated job properties back to HDFS - no functional purpose,
        # just helps developers understand what has happened
        effective_job_properties = deployer_utils.dict_to_props(properties)
        self._hdfs_client.create_file(effective_job_properties, '%s/application.properties' % remote_path)

        # submit to oozie
        result = self._submit_oozie(properties)
        self._stop_oozie(result['id'], application_user)

        return result

    def _submit_oozie(self, job_properties):
        logging.debug("_submit_oozie (submit only)")

        result = {}

        # oozie requires requests to use XML
        xml_string = deployer_utils.dict_to_xml(job_properties)

        oozie_url = '%s/v1/jobs' % self._environment['oozie_uri']

        response = requests.post(oozie_url, data=xml_string, headers={'Content-Type': 'application/xml'})

        if response.status_code >= 200 and response.status_code < 300:
            result = response.json()
        else:
            logging.error(response.headers['oozie-error-message'])
            raise FailedCreation(response.headers['oozie-error-message'])

        return result

    def _kill_oozie(self, job_id, oozie_user):
        logging.debug("_kill_oozie: %s", job_id)
        oozie_url = '%s/v1/job/%s?action=kill&user.name=%s' % (self._environment['oozie_uri'], job_id, oozie_user)
        requests.put(oozie_url)

    def _start_oozie(self, job_id, oozie_user):
        logging.debug("_start_oozie: %s", job_id)
        oozie_url = '%s/v1/job/%s?action=resume&user.name=%s' % (self._environment['oozie_uri'], job_id, oozie_user)
        requests.put(oozie_url)
        oozie_url = '%s/v1/job/%s?action=start&user.name=%s' % (self._environment['oozie_uri'], job_id, oozie_user)
        requests.put(oozie_url)

    def _stop_oozie(self, job_id, oozie_user):
        logging.debug("_stop_oozie: %s", job_id)
        oozie_url = '%s/v1/job/%s?action=suspend&user.name=%s' % (self._environment['oozie_uri'], job_id, oozie_user)
        requests.put(oozie_url)
