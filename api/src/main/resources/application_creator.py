"""
Name:       application_creator.py
Purpose:    Creates applications from packages
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

import pwd
import tarfile
import os
import json
import re

import shutil
import logging
import uuid
from importlib import import_module
from exceptiondef import FailedValidation, FailedCreation
from deployer_utils import HDFS


class ApplicationCreator(object):

    def __init__(self, config, environment, service):
        self._config = config
        self._environment = environment
        self._service = service
        self._component_creators = {}
        self._name_regex = re.compile('')
        self._hdfs_client = HDFS(environment['webhdfs_host'],
                                 environment['webhdfs_port'],
                                 environment['webhdfs_user'])

    def assert_application_properties(self, override_properties, default_properties):
        for component_type, component_properties in default_properties.items():
            creator = self._load_creator(component_type)
            creator.assert_application_properties(override_properties.get(component_type, {}), component_properties)

    def create_application(self, package_data_path, package_metadata, application_name, property_overrides):

        logging.debug("create_application: %s", application_name)

        if not re.match('^[a-zA-Z0-9_-]+$', application_name):
            raise FailedCreation('Application name %s may only contain a-z A-Z 0-9 - _' % application_name)

        user_name = property_overrides['user']
        try:
            pwd.getpwnam(user_name)
        except KeyError:
            raise FailedCreation(
                'User %s does not exist. Verify that this user account exists on the machine running the deployment manager.' % user_name)

        stage_path = self._stage_package(package_data_path)

        # create each class of components in the package, aggregating any
        # component specific return data for destruction
        create_metadata = {}
        try:
            for component_type, components in package_metadata['component_types'].items():
                creator = self._load_creator(component_type)
                result = creator.create_components(stage_path,
                                                   application_name,
                                                   user_name,
                                                   components,
                                                   property_overrides.get(component_type))
                create_metadata[component_type] = result
        finally:
            #clean up staged package data
            #shutil.rmtree(stage_path)
            pass

        return create_metadata

    def destroy_application(self, application_name, application_create_data):

        logging.debug("destroy_application: %s %s", application_name, application_create_data)

        app_hdfs_root = None
        for component_type, component_create_data in application_create_data.items():
            creator = self._load_creator(component_type)
            creator.destroy_components(application_name, component_create_data)
            if component_create_data and 'application_hdfs_root' in component_create_data[0]:
                app_hdfs_root = component_create_data[0]['application_hdfs_root']

        local_path = '/opt/%s/%s/' % (self._service, application_name)
        if os.path.isdir(local_path):
            os.rmdir(local_path)

        if app_hdfs_root is not None:
            self._hdfs_client.remove(app_hdfs_root, recursive=False)

    def start_application(self, application_name, application_create_data):

        logging.debug("start_application: %s %s", application_name, application_create_data)

        for component_type, component_create_data in application_create_data.items():
            creator = self._load_creator(component_type)
            creator.start_components(application_name, component_create_data)

    def stop_application(self, application_name, application_create_data):

        logging.debug("stop_application: %s %s", application_name, application_create_data)

        for component_type, component_create_data in application_create_data.items():
            creator = self._load_creator(component_type)
            creator.stop_components(application_name, component_create_data)

    def validate_package(self, package_name, package_metadata):

        logging.debug("validate_package: %s", json.dumps(package_metadata))

        result = {}
        self._validate_name(package_name, package_metadata)
        for component_type, component_metadata in package_metadata['component_types'].items():
            creator = self._load_creator(component_type)
            validation_errors = creator.validate_components(component_metadata)
            if validation_errors:
                result[component_type] = validation_errors

        if result:
            raise FailedValidation(result)

    def _validate_name(self, package_name, package_metadata):

        parts = package_name.split('-')
        if len(parts) < 2:
            raise FailedValidation(
                "package name must be of the form name-version e.g. name-version.1.2.3 but found %s" % package_name)

        version_parts = parts[-1].split('.')
        if len(version_parts) < 3:
            raise FailedValidation("version must be a three part major.minor.patch e.g. 1.2.3 but found %s" % parts[-1])

        if package_name != package_metadata['package_name']:
            raise FailedValidation("package name must match name of enclosed folder but found %s and %s" % (
            package_name, package_metadata['package_name']))

    def get_application_runtime_details(self, application_name, application_create_data):

        logging.debug("get_application_runtime_details: %s %s", application_name, application_create_data)

        details = {}
        details['yarn_applications'] = {}
        for component_type, component_create_data in application_create_data.items():
            creator = self._load_creator(component_type)
            type_details = creator.get_component_runtime_details(component_create_data)
            details['yarn_applications'].update(type_details['yarn_applications'])
        return details

    def _load_creator(self, component_type):

        logging.debug("_load_creator %s", component_type)

        creator = self._component_creators.get(component_type)

        if creator is None:

            module = '%s.%s' % (self._config['plugins_path'], component_type)
            cls = '%s%sCreator' % (component_type[0].upper(), component_type[1:])
            try:
                module = import_module("plugins.%s" % component_type)
                self._component_creators[component_type] = getattr(
                    module, cls)(self._config, self._environment, self._service)
                creator = self._component_creators[component_type]

            except ImportError as exception:
                logging.error(
                    'Unable to load Creator for component type "%s" [%s]',
                    component_type,
                    exception)

        return creator

    def _stage_package(self, package_data_path):

        logging.debug("_stage_package")

        if not os.path.isdir(self._config['stage_root']):
            os.mkdir(self._config['stage_root'])

        tar = tarfile.open(package_data_path)
        stage_path = "%s/%s" % (self._config['stage_root'], uuid.uuid4())
        tar.extractall(path=stage_path)

        return stage_path
