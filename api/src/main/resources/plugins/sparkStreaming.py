"""
Name:       sparkStreaming.py
Purpose:    Submits spark streaming jobs based on application package
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

import json
import os
import logging
from kubernetes.client.rest import ApiException
from kubernetes import config, client
from shutil import copy
import yaml
import deployer_utils
from plugins.base_common import Common

class SparkStreamingCreator(Common):

    def validate_component(self, component):
        errors = []
        file_list = component['component_detail']
        if 'application.properties' not in file_list:
            errors.append('missing file application.properties')
        if 'log4j.properties' not in file_list:
            errors.append('missing file log4j.properties')
        if 'upstart.conf' in file_list:
            errors.append('Support for user supplied upstart.conf files has been deprecated, ' +
                          'the deployment manager will supply one automatically. ' +
                          'Please see PNDA example-applications for usage.')
        return errors

    def get_component_type(self):
        return 'sparkStreaming'

    def create_spark_yml(self, jar_path, properties):
        main_class = properties['component_main_class']
        main_jar = 'local://%s/%s' % (jar_path, properties['component_main_jar'])
        application_name = properties['component_application_name']
        driver_cores = properties['component_driver_cores']
        driver_memory = properties['component_driver_memory']
        executor_cores = properties['component_executor_cores']
        executor_instances = properties['component_executor_instances']
        executor_memory = properties['component_executor_memory']
        json_file = '%s/%s.json' % (jar_path,application_name)
        with open('sparkapp_tpl.yaml', 'r') as yfile, open(json_file, 'w') as json_out:
            for rpl_data in yaml.safe_load_all(yfile):
                rpl_data['metadata']['name'] = application_name
                rpl_data['spec']['mainClass'] = main_class
                rpl_data['spec']['mainApplicationFile'] = main_jar
                rpl_data['spec']['driver']['cores'] = int(driver_cores)
                rpl_data['spec']['driver']['memory'] = driver_memory
                rpl_data['spec']['executor']['cores'] = int(executor_cores)
                rpl_data['spec']['executor']['instances'] = int(executor_instances)
                rpl_data['spec']['executor']['memory'] = executor_memory
                json.dump(rpl_data, json_out, ensure_ascii=False, indent="\t")
        return json_file

    def create_custom_resource_object(self, spark_app_json_path):

        config.load_incluster_config()
        configuration = client.Configuration()
        api_instance = client.CustomObjectsApi(client.ApiClient(configuration))
        group = 'sparkoperator.k8s.io'  # str | The custom resource's group name
        version = 'v1beta2'  # str | The custom resource's version
        namespace = 'pnda'  # str | The custom resource's namespace
        plural = 'sparkapplications'  # str | The custom resource's plural name. For TPRs this would be lowercase plural kind.
        pretty = 'true'
        with open(spark_app_json_path, 'r') as jfile:

            data = json.load(jfile)

        try:
            api_response = api_instance.create_namespaced_custom_object(group, version, namespace, plural, body=data, pretty=pretty)
            logging.info("CRD create response %s" % api_response)
        except ApiException as e:
            logging.debug("Exception when calling CustomObjectsApi->create_namespaced_custom_object: %s\n" % e)

    def delete_custom_resource_object(self, application_name):
        config.load_incluster_config()
        configuration = client.Configuration()
        api_instance = client.CustomObjectsApi(client.ApiClient(configuration))
        group = 'sparkoperator.k8s.io'  # str | The custom resource's group name
        version = 'v1beta2'  # str | The custom resource's version
        namespace = 'pnda'  # str | The custom resource's namespace
        plural = 'sparkapplications'  # str | The custom resource's plural name. For TPRs this would be lowercase plural kind.
        name = application_name  # str | the custom object's name
        body = client.V1DeleteOptions()
        grace_period_seconds = 56
        orphan_dependents = True

        try:
            api_response = api_instance.delete_namespaced_custom_object(
                group,
                version,
                namespace,
                plural,
                name,
                body,
                grace_period_seconds=grace_period_seconds,
                orphan_dependents=orphan_dependents)
            logging.info("CRD delete response %s" % api_response)
        except ApiException as e:
            logging.debug("Exception when calling CustomObjectsApi->delete_namespaced_custom_object: %s\n" % e)

    def create_component(self, staged_component_path, application_name, user_name, component, properties):
        logging.debug("create_component: %s %s %s %s %s", application_name, user_name, json.dumps(component), properties, staged_component_path)
        remote_component_tmp_path = '%s/%s/%s' % ('/tmp/%s' % self._namespace, application_name, component['component_name'])
        remote_component_install_path = '%s/%s/%s' % (
            '/opt/%s' % self._namespace, application_name, component['component_name'])
        service_name = '%s-%s-%s' % (self._namespace, application_name, component['component_name'])

        if 'component_spark_version' not in properties:
            properties['component_spark_version'] = '1'
        if 'component_spark_submit_args' not in properties:
            properties['component_spark_submit_args'] = ''
        if 'component_py_files' not in properties:
            properties['component_py_files'] = ''

        if 'upstart.conf' in component['component_detail']:
            # old style applications - reject these
            raise Exception('Support for user supplied upstart.conf files has been deprecated, ' +
                            'the deployment manager will supply one automatically. ' +
                            'Please see PNDA example-applications for usage.')
        else:
            # new style applications that don't need to provide upstart.conf or yarn-kill.py
            if 'component_main_jar' in properties and 'component_main_class' not in properties:
                raise Exception('properties.json must contain "main_class" for %s sparkStreaming %s' % (application_name, component['component_name']))

            java_app = None
            if 'component_main_jar' in properties:
                java_app = True
            elif 'component_main_py' in properties:
                java_app = False
            else:
                raise Exception('properties.json must contain "main_jar or main_py" for %s sparkStreaming %s' % (application_name, component['component_name']))

            this_dir = os.path.dirname(os.path.realpath(__file__))
            service_script = 'systemd.service.tpl' if java_app else 'systemd.service.py.tpl'
            service_script_install_path = '/usr/lib/systemd/system/%s.service' % service_name
            if 'component_respawn_type' not in properties:
                properties['component_respawn_type'] = 'always'
            if 'component_respawn_timeout_sec' not in properties:
                properties['component_respawn_timeout_sec'] = '2'
            copy(os.path.join(this_dir, service_script), staged_component_path)

        self._fill_properties(os.path.join(staged_component_path, service_script), properties)
        self._fill_properties(os.path.join(staged_component_path, 'log4j.properties'), properties)
        self._fill_properties(os.path.join(staged_component_path, 'application.properties'), properties)
        mkdircommands = []
        mkdircommands.append('mkdir -p %s' % remote_component_tmp_path)
        mkdircommands.append('mkdir -p %s' % remote_component_install_path)
        logging.debug("mkdircommands are %s", mkdircommands)
        deployer_utils.exec_cmds(mkdircommands)
        logging.debug("Staged Component Path is: %s", staged_component_path)

        os.system("cp %s %s" % (staged_component_path + '/*', remote_component_tmp_path))

        commands = []
        os.system('cp %s/%s %s' % (remote_component_tmp_path, service_script, service_script_install_path))
        os.system('cp %s %s' % (remote_component_tmp_path + '/*', remote_component_install_path))

        if 'component_main_jar' in properties:
            commands.append('cp %s && jar uf %s application.properties' % (remote_component_install_path, properties['component_main_jar']))
        logging.debug("commands are : %s", commands)
        deployer_utils.exec_cmds(commands)
        app_path = staged_component_path.split('/')[:5]
        jar_path = '/'.join(app_path)

        self.create_spark_yml(jar_path, properties)
        app_removal_path = staged_component_path.split('/')[:3]


        undo_commands = []
        undo_commands.append('rm -rf %s\n' % '/'.join(app_removal_path))
        undo_commands.append('rm -rf %s\n' % remote_component_tmp_path)
        undo_commands.append('rm -rf %s\n' % remote_component_install_path)
        undo_commands.append('rm  %s\n' % service_script_install_path)
        logging.debug("uninstall commands: %s", undo_commands)
        return {'ssh': undo_commands, 'crdjson': jar_path}

    def restart_component(self, application_name, create_data):
        logging.debug("deleting_component: %s %s", application_name, json.dumps(create_data))
        self.delete_custom_resource_object(application_name)
        logging.debug("restarting_component: %s %s", application_name, json.dumps(create_data))
        json_path = create_data['crdjson']
        self.create_custom_resource_object("%s/%s.json" % (json_path,application_name))

    def destroy_component(self, application_name, create_data):
        logging.debug("destroy_component: %s %s", application_name, json.dumps(create_data))
        self.delete_custom_resource_object(application_name)
        self._control_component(create_data['ssh'])

    def start_component(self, application_name, create_data):
        logging.debug("start_component: %s %s", application_name, json.dumps(create_data))
        #._control_component(create_data['start_cmds'])
        json_path = create_data['crdjson']
        self.create_custom_resource_object("%s/%s.json" % (json_path,application_name))

    def get_pod_logs(self, pod_name):
        namespace_id = 'pnda'
        config.load_incluster_config()
        try:
            configuration = client.Configuration()
            api_client = client.ApiClient(configuration)
            api_instance = client.CoreV1Api(api_client)
            api_response = api_instance.read_namespaced_pod_log(name=str(pod_name) + "-driver", namespace=namespace_id)
            return api_response

        except ApiException as e:
            logging.debug("Exception when calling CoreV1Api->read_namespaced_pod_log: %s\n" % e)

    def get_pod_state(self, pod_name):
        namespace_id = 'pnda'
        config.load_incluster_config()
        try:
            configuration = client.Configuration()
            api_client = client.ApiClient(configuration)
            api_instance = client.CoreV1Api(api_client)
            api_response_state = api_instance.read_namespaced_pod_status(name=str(pod_name) + "-driver", namespace=namespace_id)
            return api_response_state.status.phase
        except ApiException as e:
            logging.debug("Exception when calling CoreV1Api->read_namespaced_pod_status: %s\n" % e)

    def _control_component(self, cmds):
        deployer_utils.exec_cmds(cmds)
