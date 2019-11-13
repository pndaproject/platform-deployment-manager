"""
Name:       authorizer_local.py
Purpose:    Validates requests to access resources by comparing the attributes of an
            identity, a resource and an action against rules defined in authorizer_rules.yaml

Author:     PNDA team

Created:    17/05/2018

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
import logging
import json
import yaml
from authorizer import Authorizer

class AuthorizerLocal(Authorizer):
    '''
    Authorizer implementation that validates requests based on locally defined rules
    '''
    def __init__(self):
        '''
        Initialise the authorizer by loading the rules file
        '''
        with open('authorizer_rules.yaml') as rules_file:
            self._rules = yaml.load(rules_file)

    def authorize(self, identity, resource, action):
        '''
        Validate a request to access a resource
        Parameters:
         - identity: dictionary of attributes defining the user performing the action
         - resource: dictionary of attributes defining the resource that access is required for
         - action: dictionary of attributes defining the action being performed
        '''
        logging.debug("authorize: identity:%s, resource:%s, action:%s", json.dumps(identity), json.dumps(resource), json.dumps(action))
        sets = self._rules['sets']
        logging.debug("sets: %s", json.dumps(sets))
        authorize = False
        for grant_rule in self._rules['rules']['grant']:
            logging.debug("checking rule: %s", grant_rule)
            try:
                #pylint: disable=eval-used
                if eval(grant_rule):
                    authorize = True
                    logging.debug("authorize: %s for %s", authorize, grant_rule)
                    break
            except KeyError as ex:
                logging.warning("missing attribute %s", ex)

        if not authorize:
            logging.debug("authorize: %s", authorize)

        return authorize
