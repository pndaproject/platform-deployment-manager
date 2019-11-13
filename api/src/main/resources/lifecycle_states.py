"""
Name:       lifecycle_states.py
Purpose:    Constants for lifecycle state names
Author:     PNDA team

Created:    04/04/2016

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


class PackageDeploymentState(object):
    DEPLOYING = "DEPLOYING"
    DEPLOYED = "DEPLOYED"
    UNDEPLOYING = "UNDEPLOYING"
    NOTDEPLOYED = "NOTDEPLOYED"


class ApplicationState(object):
    CREATING = "CREATING"
    DESTROYING = "DESTROYING"
    CREATED = "CREATED"
    STARTING = "STARTING"
    STARTED = "STARTED"
    STOPPING = "STOPPING"
    NOTCREATED = "NOTCREATED"
    COMPLETED = "COMPLETED"
