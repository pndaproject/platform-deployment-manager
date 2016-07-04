# -------------------------------------------------------------------------------
# Name:         test_run_server_and_rest_test
# Purpose:      runs the server and then runs the rest tests.
#               Good  for debugging

#
# Author:       eshalev
#
# Created:      21/03/2016
# -------------------------------------------------------------------------------
"""
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
from threading import Thread
from time import sleep
from deployer_system_test import DeployerRestClientTester
from app import main


def start_server():
    # replace this sleep with a mutex. Sleep might have a race condition
    sleep(3)
    print "running api tests"
    deployer_tests = DeployerRestClientTester("http://localhost:5000")
    deployer_tests.run_tests()


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level="DEBUG")
    print "This file is only a test file to excersize the Deployer rest API"
    THREAD = Thread(target=start_server)
    THREAD.start()
    print "starting server"
    main()
