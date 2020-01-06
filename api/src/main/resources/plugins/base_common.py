"""
Name:       base_common.py
Purpose:    Common methods for flink.py and sparkStreaming.py plugins.
Author:     PNDA team

Created:    20/04/2018

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
import logging
from kubernetes.client.rest import ApiException
from kubernetes import config, client
import deployer_utils
from plugins.base_creator import Creator


class Common(Creator):
    def destroy_component(self, application_name, create_data):
        logging.debug("destroy_component: %s %s", application_name, json.dumps(create_data))
        self.delete_custom_resource_object(application_name)
        self._control_component(create_data['ssh'])

    def start_component(self, application_name, create_data):
        logging.debug("start_component: %s %s", application_name, json.dumps(create_data))
        # self._control_component(create_data['start_cmds'])
        json_path = create_data['crdjson']
        self.create_custom_resource_object("%s/%s.json" % (json_path,application_name))

    def stop_component(self, application_name, create_data):
        logging.debug("stop_component: %s %s", application_name, json.dumps(create_data))
        # self._control_component(create_data['stop_cmds'])

    def _control_component(self, cmds):
        '''key_file = self._environment['cluster_private_key']
        root_user = self._environment['cluster_root_user']
        target_host = 'localhost'''
        deployer_utils.exec_cmds(cmds)

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
