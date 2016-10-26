"""
Name:       base_creator.py
Purpose:    Base Functionality for Creator classes
            Calls overridden methods of derived classes to create specific components
            The creator names must match the component type names in packages:
                For example, for a component type of 'oozie' it will look for a module 'oozie'
                and a class within that module called 'OozieCreator'.

            Each Creator must implement:
                - validate_component
                - create_component
                - destroy_component
                - start_component
                - stop_component
                - get_component_type
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
import string
import collections
import subprocess
import hbase_descriptor
import opentsdb_descriptor
from deployer_utils import HDFS


class Creator(object):
    '''
    Base Functionality for Creator classes
    '''

    def __init__(self, config, environment, namespace):
        '''
        The Creator will be passed the config and environment descriptors
        that are passed to the Deployment Manager when it is instantiated.
        '''
        self._config = config
        self._environment = environment
        self._namespace = namespace
        self._hdfs_client = HDFS(environment['webhdfs_host'],
                                 environment['webhdfs_port'],
                                 'hdfs')

    def validate_component(self, components):
        '''
        Validates components of the package of given component type

        components - a list of component maps reflecting the structure of the
                 components within the package and following this example.
                 component_detail, component_path and component_name are
                 guaranteed to be present, component_detail will vary according
                 to the contents of the package. The validation function can
                 do anything but it is sensible to check for presence of key
                 files, for example. Since the staging path is present in the
                 config passed to the Creator it is possible to load the actual
                 file data and execute more complex validation if desired.

        returns - Array of messages indication what is wrong, 0 length array for no errors

        '''
        pass

    def create_component(self, staged_component_path, application_name, component, properties):
        '''
        Creates component of the package of given component type

        application_name - name of the application
        components - a list of component maps following above example structure
        properties - a full map of properties that can be used as needed

        returns - create data, which can be anything but will be associated
              with this application creation by the Deployment Manager. Therefore
              it makes sense to include enough information to be able to
              implement destroy_components.
        '''
        pass

    def destroy_component(self, application_name, create_data):
        '''
        Destroys component of the package of given component type

        application_name - name of the application
        create_data - as per above explanation, the data returned from the
                      corresponding create operation, which should be
                      sufficient to execute a clean destruction.
        '''
        pass

    def start_component(self, application_name, start_data):
        '''
        starts component of the package of given component type

        application_name - name of the application
        create_data - as per above explanation, the data returned from the
                      corresponding create operation, which should be
                      sufficient to execute start command.
        '''
        pass

    def stop_component(self, application_name, stop_data):
        '''
        stops component of the package of given component type

        application_name - name of the application
        create_data - as per above explanation, the data returned from the
                      corresponding create operation, which should be
                      sufficient to execute stop command.
        '''
        pass

    def _instantiate_properties(self, application_name, component, property_overrides):
        logging.debug(
            "_instantiate_properties %s %s",
            component,
            property_overrides)

        component_properties = {}
        if 'properties.json' in component['component_detail']:
            component_properties = component['component_detail']['properties.json']

        props = collections.OrderedDict()
        for prop in self._environment:
            props['environment_' + prop] = self._environment[prop]
        for prop in component_properties:
            props['component_' + prop] = component_properties[prop]
        for prop in property_overrides:
            props['component_' + prop] = property_overrides[prop]

        props['component_application'] = application_name
        props['component_name'] = component['component_name']
        props['component_job_name'] = '%s-%s-job' % (props['component_application'], props['component_name'])
        props['component_hdfs_root'] = '/user/%s/%s' % (application_name, component['component_name'])
        return props

    def _fill_properties(self, local_file, props):
        with open(local_file, "r") as myfile:
            file_contents = myfile.read()

        new_file_contents = string.Template(
            file_contents).safe_substitute(props)

        with open(local_file, "w") as myfile:
            myfile.write(new_file_contents)

    def _auto_fill_app_properties(self, staged_component_path, props):
        app_properties_file_path = '%s/application.properties' % staged_component_path
        with open(app_properties_file_path, "a") as app_properties_file:
            if 'component_no_auto_props' not in props:
                app_properties_file.write('\n')
                for prop in props:
                    app_properties_file.write('%s=%s\n' % (
                        prop.replace('_', '.', 1), props[prop]))

    def _create_optional_descriptors(self, staged_component_path, component, properties):
        result = {}
        if 'hbase.json' in component['component_detail']:
            logging.debug("creating hbase.json")
            self._fill_properties('%s/%s' % (staged_component_path, 'hbase.json'), properties)
            hbase_descriptor.create('%s/%s' % (staged_component_path, 'hbase.json'), self._environment)

        if 'hdfs.json' in component['component_detail']:
            logging.debug("creating hdfs.json")
            descriptor_path = '%s/%s' % (staged_component_path, 'hdfs.json')
            self._fill_properties(descriptor_path, properties)
            with open(descriptor_path) as descriptor_file:
                contents = descriptor_file.read()
                hdfs_descriptor = json.loads(contents)
            for element in hdfs_descriptor:
                properties['hdfspath_' + element['name']] = element['path']
                self._hdfs_client.make_dir(element['path'])

            result['hdfs.json'] = hdfs_descriptor

        if 'opentsdb.json' in component['component_detail']:
            logging.debug("creating opentsdb.json")
            self._fill_properties('%s/%s' % (staged_component_path, 'opentsdb.json'), properties)
            opentsdb_descriptor.create('%s/%s' % (staged_component_path, 'opentsdb.json'), self._environment)

        return result

    def _destroy_optional_descriptors(self, descriptors):
        result = {}

        if 'hdfs.json' in descriptors:
            logging.debug("destroying hdfs.json")
            hdfs_descriptor = descriptors['hdfs.json']
            for element in hdfs_descriptor:
                if element['delete_on_undeploy'] > 0:
                    dir_count = element['delete_on_undeploy']
                    while dir_count > 0:
                        dir_count = dir_count - 1
                        element['path'] = element['path'][
                            :element['path'].rfind('/')]
                    self._hdfs_client.remove(element['path'], recursive=True)

        return result

    def create_components(self, stage_path, application_name, components,
                          components_overrides):
        results = []
        for component_name, component in components.iteritems():
            staged_component_path = '%s/%s' % (stage_path, component['component_path'])
            overrides = components_overrides.get(component_name) if components_overrides is not None else {}
            overrides = {} if overrides is None else overrides
            merged_props = self._instantiate_properties(application_name, component, overrides)
            descriptor_result = self._create_optional_descriptors(staged_component_path, component, merged_props)
            self._auto_fill_app_properties(staged_component_path, merged_props)
            result = self.create_component(staged_component_path, application_name, component, merged_props)
            result['component_name'] = component_name
            result['component_job_name'] = merged_props['component_job_name']
            result['descriptors'] = descriptor_result
            results.append(result)
        return results

    def destroy_components(self, application_name, create_data):
        for single_component_data in create_data:
            self._destroy_optional_descriptors(single_component_data['descriptors'])
            self.destroy_component(application_name, single_component_data)
        return None

    def start_components(self, application_name, start_data):
        for single_component_data in start_data:
            self.start_component(application_name, single_component_data)
        return None

    def stop_components(self, application_name, stop_data):
        for single_component_data in stop_data:
            self.stop_component(application_name, single_component_data)
        return None

    def validate_components(self, components):
        logging.debug("validate_components: %s", components)
        result = {}
        for component_name, component in components.iteritems():
            validation_errors = self.validate_component(component)
            if len(validation_errors) > 0:
                result[component_name] = validation_errors
        return result

    def get_component_runtime_details(self, create_data):
        logging.debug("get_component_runtime_details: %s", create_data)
        details = {}
        yarn_ids = []
        details['yarn_ids'] = yarn_ids
        for single_component_data in create_data:
            yarn_id = self.get_yarn_id(single_component_data['component_job_name'])
            if yarn_id is not None:
                yarn_ids.append({"component": single_component_data['component_name'],
                                 "type": self.get_component_type(),
                                 "yarn-id": yarn_id})
        return details

    def get_yarn_id(self, job_name):
        out = subprocess.check_output(['yarn', 'application', '-list'])
        for line in out.splitlines():
            fields = line.split('\t')
            if len(fields) >= 6:
                logging.debug(line)
                app = fields[1].strip()
                if app == job_name:
                    return fields[0].strip()
        return None
