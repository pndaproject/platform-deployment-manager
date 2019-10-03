"""
Name:       package_parser.py
Purpose:    Parses metadata from package structure
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

import json
import tarfile
import traceback
import logging

from exceptiondef import FailedValidation


class PackageParser(object):

    def __init__(self):
        pass

    def properties_from_metadata(self, metadata):
        properties = {}
        for component_type, components in metadata['component_types'].items():
            properties[component_type] = {}
            for component_name, component_detail in components.items():
                properties[component_type][component_name] = component_detail['component_detail']['properties.json']
        return properties

    def get_package_metadata(self, package_data_path):

        try:
            logging.debug("get_package_metadata")
            metadata = {}

            tar = tarfile.open(package_data_path)
            for name in sorted([member.name for member in tar.getmembers()]):
                name_parts = name.split('/')
                package_name = name_parts[0]
                if len(name_parts) == 1:
                    # there must be exactly one package in the archive
                    if 'package_name' in metadata:
                        raise FailedValidation("Expected to find a single directory inside the archive, but found more than one item at the top level")
                    metadata['package_name'] = package_name
                    metadata['component_types'] = {}
                elif len(name_parts) >= 4:
                    component_type = name_parts[1]
                    component_name = name_parts[2]
                    file_name = name_parts[3]
                    if component_type not in metadata['component_types']:
                        metadata['component_types'][component_type] = {}
                    if component_name not in metadata['component_types'][component_type]:
                        metadata['component_types'][component_type][component_name] = {
                            'component_name': component_name,
                            'component_detail': {},
                            'component_path': '%s/%s/%s' % (package_name, component_type, component_name)
                        }
                    file_contents = {}
                    if file_name == 'properties.json':
                        file_contents = json.load(tar.extractfile(name))
                    metadata['component_types'][component_type][component_name][
                        'component_detail']['/'.join(name_parts[3:])] = file_contents

            # there must be at least one component type in the package
            if 'component_types' not in metadata:
                raise FailedValidation("Expected to find a single package directory inside the archive, but found none")
            if len(metadata['component_types']) <= 0:
                raise FailedValidation("Expected to find at least one component within the package directory")

            for component_type, components in metadata['component_types'].items():
                for component_name, component_detail in components.items():
                    if 'properties.json' not in component_detail['component_detail']:
                        component_detail['component_detail']['properties.json'] = {}

            logging.debug(json.dumps(metadata))
            return metadata
        except FailedValidation as failure:
            logging.error(traceback.format_exc())
            raise failure
        except Exception:
            logging.error(traceback.format_exc())
            raise FailedValidation("Unexpected error parsing the package contents")
