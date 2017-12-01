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

import json
import logging
import datetime
import requests

import deployer_utils
from plugins.base_creator import Creator
from exceptiondef import FailedCreation


class OozieCreator(Creator):

    def validate_component(self, component):
        errors = []
        file_list = component['component_detail']
        if 'workflow.xml' not in file_list:
            errors.append('missing file workflow.xml')
        return errors

    def get_component_type(self):
        return 'oozie'

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

    def start_component(self, application_name, create_data):
        logging.debug("start_component: %s %s", application_name, json.dumps(create_data))
        self._start_oozie(create_data['job_handle'], create_data['application_user'])

    def stop_component(self, application_name, create_data):
        logging.debug("stop_component: %s %s", application_name, json.dumps(create_data))
        self._stop_oozie(create_data['job_handle'], create_data['application_user'])

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

        # insert required oozie properties
        properties['user.name'] = properties['application_user']
        # Oozie ShareLib - supports actions
        properties['oozie.use.system.libpath'] = 'true'
        # platform shared libs e.g. hbase
        properties['oozie.libpath'] = '/user/deployment/platform'

        # insert reference to coordinator xor workflow
        if 'coordinator.xml' in component['component_detail']:
            def_path = 'oozie.coord.application.path'
        else:
            def_path = 'oozie.wf.application.path'

        properties[def_path] = '%s/%s' % (self._environment['name_node'], remote_path)

        # deploy everything to various hadoop services
        undeploy = self._deploy_to_hadoop(properties, staged_component_path, remote_path, properties['application_user'])

        # return something that can be used to undeploy later
        return {'job_handle': undeploy['id'],
                'component_hdfs_root': properties['component_hdfs_root'],
                'application_user': properties['application_user']}

    def _deploy_to_hadoop(self, properties, staged_component_path, remote_path, application_user, exclude=None):
        if exclude is None:
            exclude = []
        exclude.extend(['hdfs.json',
                        'hbase.json',
                        'properties.json',
                        'application.properties'])

        # stage the component files to hdfs
        self._hdfs_client.recursive_copy(staged_component_path, remote_path, exclude=exclude)

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
        print oozie_url
        requests.put(oozie_url)
