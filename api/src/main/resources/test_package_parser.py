"""
Name:       test_package_parser.py
Purpose:    Unit tests for the package parser
            Run with main(), the easiest way is "nosetests test_*.py"
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

import unittest
from package_parser import PackageParser
from exceptiondef import FailedValidation


class PackageParserTests(unittest.TestCase):

    def test_generate_metadata(self):
        parser = PackageParser()
        expected_metadata = {
            "component_types": {
                "sparkStreaming": {
                    "componentC": {
                        "component_detail": {
                            "properties.json": {
                                "property1": "1",
                                "property2": "two"
                            }
                        },
                        "component_path": "test_package-1.0.2/sparkStreaming/componentC",
                        "component_name": "componentC"
                    }
                },
                "oozie": {
                    "componentA": {
                        "component_detail": {
                            "properties.json": {
                                "property3": "3",
                                "property4": "four"
                            }
                        },
                        "component_path": "test_package-1.0.2/oozie/componentA",
                        "component_name": "componentA"
                    },
                    "componentB": {
                        "component_detail": {
                            "hdfs.json": {},
                            "properties.json": {}
                        },
                        "component_path": "test_package-1.0.2/oozie/componentB",
                        "component_name": "componentB"
                    }
                }
            },
            "package_name": "test_package-1.0.2"
        }

        package_name = "test_package-1.0.2"
        self.assertEqual(parser.get_package_metadata("%s.tar.gz" % package_name), expected_metadata)

    def test_invalid_package(self):
        parser = PackageParser()
        package_name = "test_package-1.0.3"
        self.assertRaises(FailedValidation, parser.get_package_metadata, "%s.tar.gz" % package_name)

    def test_generate_properties(self):
        parser = PackageParser()
        metadata = {
            "component_types": {
                "sparkStreaming": {
                    "componentC": {
                        "component_detail": {
                            "properties.json": {
                                "property1": "1",
                                "property2": "two"
                            }
                        },
                        "component_path": "test_package-1.0.2/sparkStreaming/componentC",
                        "component_name": "componentC"
                    }
                },
                "oozie": {
                    "componentA": {
                        "component_detail": {
                            "properties.json": {
                                "property3": "3",
                                "property4": "four"
                            }
                        },
                        "component_path": "test_package-1.0.2/oozie/componentA",
                        "component_name": "componentA"
                    },
                    "componentB": {
                        "component_detail": {
                            "properties.json": {
                                "property5": "5",
                                "property6": "six"
                            }
                        },
                        "component_path": "test_package-1.0.2/oozie/componentB",
                        "component_name": "componentB"
                    }
                }
            },
            "package_name": "test_package-1.0.2"
        }

        expected_properties = {
            'oozie': {
                'componentA': {
                    'property3': '3',
                    'property4': 'four'
                },
                'componentB': {
                    'property5': '5',
                    'property6': 'six'
                }
            },
            'sparkStreaming': {
                'componentC': {
                    'property1': '1',
                    'property2': 'two'
                }
            }
        }
        self.assertEqual(parser.properties_from_metadata(metadata), expected_properties)
        