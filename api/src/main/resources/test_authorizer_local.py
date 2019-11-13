"""
Purpose:    Unit tests for the the authorizer_local module
            Run with "nosetests test_*.py"
Author:     PNDA team

Copyright (c) 2018 Cisco and/or its affiliates.

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
from authorizer_local import AuthorizerLocal

class AuthorizerLocalTesting(AuthorizerLocal):
    def add_rule(self, rule):
        self._rules['rules']['grant'].append(rule)

    def remove_from_set(self, set_name, action):
        print self._rules['sets'][set_name]
        self._rules['sets'][set_name].remove(action)
        print self._rules['sets'][set_name]

class TestAuthorizerLocal(unittest.TestCase):
    '''
    Tests for AuthorizerLocal
    '''

    def test_modify_own_application1(self):
        # Check that it is allowed to modify your own application
        auth = AuthorizerLocal()
        self.assertTrue(auth.authorize({'user': 'dave', 'groups': ['group1', 'group2']},
                                       {'type': 'deployment_manager:application', 'owner': 'dave'},
                                       {'name': 'deployment_manager:application:create'}))

    def test_modify_own_application2(self):
        # Check that it is allowed to modify your own application with a different action
        auth = AuthorizerLocal()
        self.assertTrue(auth.authorize({'user': 'dave', 'groups': ['group1', 'group2']},
                                       {'type': 'deployment_manager:application', 'owner': 'dave'},
                                       {'name': 'deployment_manager:application:start'}))

    def test_read_other_application(self):
        # Check that it is allowed to read someone else's application
        auth = AuthorizerLocal()
        self.assertTrue(auth.authorize({'user': 'dave', 'groups': ['group1', 'group2']},
                                       {'type': 'deployment_manager:application', 'owner': 'delia'},
                                       {'name': 'deployment_manager:application:read'}))

    def test_read_own_application(self):
        # Check that it is allowed to read your own application
        auth = AuthorizerLocal()
        self.assertTrue(auth.authorize({'user': 'dave', 'groups': ['group1', 'group2']},
                                       {'type': 'deployment_manager:application', 'owner': 'dave'},
                                       {'name': 'deployment_manager:application:read'}))

    def test_admin_other_application(self):
        # Check that it is allowed to modify someone else's application if you are admin
        auth = AuthorizerLocal()
        self.assertTrue(auth.authorize({'user': 'dave', 'groups': ['group1', 'admin']},
                                       {'type': 'deployment_manager:application', 'owner': 'delia'},
                                       {'name': 'deployment_manager:application:read'}))

    def test_modify_other_application(self):
        # Check that it is prohibited to modify someone else's application
        auth = AuthorizerLocal()
        self.assertFalse(auth.authorize({'user': 'dave', 'groups': ['group1', 'group1']},
                                        {'type': 'deployment_manager:application', 'owner': 'delia'},
                                        {'name': 'deployment_manager:application:stop'}))

    def test_undefined_action(self):
        # Check that it is prohibited to perform an undefined action
        auth = AuthorizerLocal()
        self.assertFalse(auth.authorize({'user': 'dave', 'groups': ['group1', 'admin']},
                                        {'type': 'deployment_manager:application', 'owner': 'dave'},
                                        {'name': 'deployment_manager:application:hack'}))

    def test_empty_groups(self):
        # Check that a user with no groups can modify their own application
        auth = AuthorizerLocal()
        self.assertTrue(auth.authorize({'user': 'dave', 'groups': []},
                                       {'type': 'deployment_manager:application', 'owner': 'dave'},
                                       {'name': 'deployment_manager:application:start'}))

    def test_missing_user(self):
        # Check that if insufficient attributes are supplied then that counts as failing a rule
        auth = AuthorizerLocal()
        self.assertFalse(auth.authorize({'groups': []},
                                        {'type': 'deployment_manager:application', 'owner': 'dave'},
                                        {'name': 'deployment_manager:application:stop'}))

    def test_modified_rules(self):
        # Test another rule added to the default ones
        auth = AuthorizerLocalTesting()
        auth.add_rule("action['name'] == 'deployment_manager:package:deploy' and 'authors' in identity['groups']")
        auth.remove_from_set('everyone_actions', "deployment_manager:package:deploy")
        #no deploy as non author
        self.assertFalse(auth.authorize({'user': 'dave', 'groups': ['users', 'sys22']},
                                        {'type': 'deployment_manager:package', 'owner': None},
                                        {'name': 'deployment_manager:package:deploy'}))
        #deploy as author
        self.assertTrue(auth.authorize({'user': 'delia', 'groups': ['authors', 'users', 'sys22']},
                                       {'type': 'deployment_manager:package', 'owner': None},
                                       {'name': 'deployment_manager:package:deploy'}))
        #create as non author
        self.assertTrue(auth.authorize({'user': 'dave', 'groups': ['users', 'sys22']},
                                       {'type': 'deployment_manager:application', 'owner': None},
                                       {'name': 'deployment_manager:application:create'}))
