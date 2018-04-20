# -------------------------------------------------------------------------------
# Name:        DeployerRestClient
# Purpose:     A rest client to query the deployer via REST calls
#
# Author:      eshalev
#
# Created:     18/03/2016
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
import sys
import json
import time
import re
import traceback
import logging
import requests

from lifecycle_states import ApplicationState, PackageDeploymentState


class DeployerRestClient(object):
    """
    A rest client to query the deployer via REST calls
    """

    def __init__(self, rest_end_point):
        """
        :param rest_end_point: The url for the server to query
        """
        self.rest_end_point = rest_end_point

    def generic_get_request(self, url, ok_codes=None):
        """
        a convenience method for making get calls.
        :param url: The url to query (appended to the endpoint in the constructor)
        :return: a python http response
        """
        if ok_codes is None:
            ok_codes = [200]

        url = self.rest_end_point + url
        logging.debug("REST-GET: %s", url)
        response = requests.get(url)
        # check the response code
        logging.debug("RESPONSE: " + str(response.status_code) + ", " + str(response.content))
        assert response.status_code in ok_codes
        return response

    def request_available_packages(self):
        """
        :return: The packages available to be deployed
        """
        return self.generic_get_request("/repository/packages")

    def request_deployed_packages(self):
        """
        :return: packages that have already been deployed
        """
        return self.generic_get_request("/packages")

    def request_deployed_applications(self):
        """
        :return: applications that have already been deployed
        """
        return self.generic_get_request("/applications")

    def request_package_status(self, package_name):
        """
        :param package_name: the name of the package to check
        :return: status of the package deployment
        """
        return self.generic_get_request("/packages/" + package_name + "/status")

    def request_application_status(self, application_name):
        """
        :param application_name: the name of the application to check
        :return: status of the application deployment
        """
        return self.generic_get_request("/applications/" + application_name + "/status", [200, 404])

    def request_application_detail(self, application_name):
        """
        :param application_name: the name of the application to check
        :return: detail of the application deployment like yarn IDs
        """
        return self.generic_get_request("/applications/" + application_name + "/detail")

    def request_package_info(self, package_name):
        """
        :param package_name: the name of the package to check
        :return: extended information regarding package deployment.
        """
        return self.generic_get_request("/packages/" + package_name)

    def request_application_info(self, application_name):
        """
        :param application_name: the name of the application to check
        :return: extended information regarding application deployment.
        """
        return self.generic_get_request("/applications/" + application_name)

    def deploy_package(self, package_name):
        """
        :param package_name: The name of the packaged to deploy
        :return: response of package deployed
        """
        # deploy package:
        url = self.rest_end_point + '/packages/' + package_name
        logging.debug("\nREST-PUT: %s", url)
        response = requests.put(url)
        # check the response code
        logging.debug("RESPONSE: %s ", response.content)
        assert response.status_code == 202
        return response

    def create_application(self, package_name, application_name):
        """
        :param package_name: The name of the packaged to deploy
        :param application_name: The name of the application to create
        :return: response of application created
        """
        # create application:
        url = self.rest_end_point + '/applications/' + application_name
        logging.debug("\nREST-PUT: %s", url)
        response = requests.put(url, json={'package': package_name})
        # check the response code
        logging.debug("RESPONSE: %s ", response.content)
        assert response.status_code == 202
        return response

    def undeploy_application(self, application_name):
        """
        :param application_name: The name of the application to undeploy
        :return: response of application undeployed
        """
        # undeploy package:
        url = self.rest_end_point + '/applications/' + application_name
        logging.debug("\nREST-DELETE: %s", url)
        response = requests.delete(url)
        # check the response code
        logging.debug("RESPONSE: %s ", response.content)
        assert response.status_code == 202
        return response

    def start_application(self, application_name):
        """
        :param application_name: The name of the application to start
        :return: response of application started
        """
        # start application:
        url = self.rest_end_point + '/applications/%s/start' % application_name
        logging.debug("\nREST-POST: %s", url)
        response = requests.post(url)
        # check the response code
        logging.debug("RESPONSE: %s ", response.content)
        assert response.status_code == 202
        return response

    def stop_application(self, application_name):
        """
        :param application_name: The name of the application to stop
        :return: response of application stopped
        """
        # stop application:
        url = self.rest_end_point + '/applications/%s/stop' % application_name
        logging.debug("\nREST-POST: %s", url)
        response = requests.post(url)
        # check the response code
        logging.debug("RESPONSE: %s ", response.content)
        assert response.status_code == 202
        return response

    def destroy_application(self, application_name):
        """
        :param application_name: The name of the application to destroy
        :return: response of application destroyed
        """
        # undeploy application:
        url = self.rest_end_point + '/applications/' + application_name
        logging.debug("\nREST-DELETE: %s", url)
        response = requests.delete(url)
        # check the response code
        logging.debug("RESPONSE: %s ", response.content)
        assert response.status_code == 202
        return response

    def undeploy_package(self, package_name):
        """
        :param package_name: The name of the package to undeploy
        :return: response of package undeployed
        """
        # undeploy package:
        url = self.rest_end_point + '/packages/' + package_name
        logging.debug("\nREST-DELETE: %s", url)
        response = requests.delete(url)
        # check the response code
        debug_string = "RESPONSE: " + str(response.status_code) + ", " + response.content
        logging.debug(debug_string)
        assert response.status_code == 202, "202 expected got: " + debug_string
        return response


class DeployerRestClientTester(object):
    """
    Tests for the deployer test client
    """

    def __init__(self, rest_end_point="http://localhost:5000"):
        self.deployer_rest_client = DeployerRestClient(rest_end_point)

    def run_tests(self):
        status = "pass"

        try:
            logging.info("testing errors:")
            # this pacakge should have bad configuration issues. Starting the app should cause a delayed error
            self.test_deploy_bad_package(status)
            error_package = "ThisIsABadPackage"
            self.test_error_deploy_bad_package(error_package)
            self.test_error_undeploy_bad_package(error_package)
            self.test_error_undeploy_bad_package(error_package)

            logging.debug("Finding a package to use for testing...")
            test_application = 'testapp'
            test_package = self.find_a_test_package()
            logging.debug("Testing to see if package is allready deployed")
            if self.is_package_deployed(test_package):
                logging.debug("package has been previously deployed, Undepoying...")
                logging.debug("""WARNING this might be a bad on a production system,
                    but it's a convient cleanup method for incomplete tests""")
                self.test_undeploy(test_package)
            if self.is_application_deployed(test_application):
                logging.debug("application has been previously deployed, Undepoying...")
                logging.debug("""WARNING this might be a bad on a production system,
                    but it's a convient cleanup method for incomplete tests""")
                self.test_destroy_application(test_application)
            logging.debug("ensure that the workspace is clean before continuing test:")
            assert not self.is_package_deployed(test_package)
            self.test_deploy_package(test_package)
            package_info = self.get_package_info(test_package)
            logging.debug("checking extended package info interface")
            assert package_info["status"] == PackageDeploymentState.DEPLOYED
            assert package_info["defaults"]
            self.test_create_application(test_package, test_application)
            application_info = self.get_application_info(test_application)
            assert application_info["status"] == ApplicationState.CREATED
            self.test_start_application(test_application)
            self.test_get_application_detail(test_application)
            self.test_stop_application(test_application)
            self.test_destroy_application(test_application)
            logging.debug("undeploying package")
            self.test_undeploy(test_package)
            logging.info("!!! TESTS SUCCEEDED !!!")
        except Exception:
            logging.info("!!! TESTS FAILED!!!")
            logging.info(traceback.format_exc())
            status = "fail"

        return {"status": status}

    def test_deploy_bad_package(self, response):
        # deploy an erroneous package, and check for error reporting
        bad_application = "spark-batch-example-app-wf-0.0.1"
        # initial start should succeed, failure should be reported later
        self.test_deploy_package(bad_application,
                                 expected_state=PackageDeploymentState.NOTDEPLOYED,
                                 legal_values=[PackageDeploymentState.DEPLOYING,
                                               PackageDeploymentState.NOTDEPLOYED])
        # confirm that the information is in the status:
        response = self.deployer_rest_client.request_package_status(bad_application)
        status = json.loads(response.content)
        # check that error was reported in information
        assert "missing file" in json.dumps(status.get("information"))
        return response

    def test_destroy_application(self, test_application):
        logging.debug("Destroying test application...")
        self.deployer_rest_client.destroy_application(test_application)
        self.await_application_status(test_application,
                                      expected=ApplicationState.NOTCREATED,
                                      legal_values=[ApplicationState.DESTROYING, ApplicationState.NOTCREATED])

    def test_stop_application(self, test_application):
        logging.debug("Stopping test application...")
        self.deployer_rest_client.stop_application(test_application)
        self.await_application_status(test_application,
                                      expected=ApplicationState.CREATED,
                                      legal_values=[ApplicationState.STOPPING, ApplicationState.CREATED])

    def test_start_application(self, test_application, expected_state=ApplicationState.STARTED, legal_values=None):
        if not legal_values:
            legal_values = [ApplicationState.STARTING, ApplicationState.STARTED]
        logging.debug("Starting test application...")
        self.deployer_rest_client.start_application(test_application)
        self.await_application_status(test_application,
                                      expected=expected_state,
                                      legal_values=legal_values)

    def test_get_application_detail(self, test_application):
        logging.debug("Getting application detail...")

        logging.debug("waiting for yarn ID to become available")
        retries_performed = 0
        while not self.check_yarn_id(test_application):
            time.sleep(2)
            retries_performed += 1
            if retries_performed > 60:
                raise Exception("Exceeded retry limit, Is something wrong?")
            logging.debug("Retry #%s", str(retries_performed))

    def test_create_application(self, test_package, test_application):
        logging.debug("Creating test application...")
        self.deployer_rest_client.create_application(
            test_package, test_application)
        self.await_application_status(test_application,
                                      expected=ApplicationState.CREATED,
                                      legal_values=[ApplicationState.CREATING, ApplicationState.CREATED])

    def test_deploy_package(self, test_package, expected_state=PackageDeploymentState.DEPLOYED, legal_values=None):
        # set some default values for parameter:
        if not legal_values:
            legal_values = [PackageDeploymentState.DEPLOYING, PackageDeploymentState.DEPLOYED]
        logging.debug("Deploying test package...")
        self.deployer_rest_client.deploy_package(test_package)
        self.await_package_status(test_package,
                                  expected=expected_state,
                                  legal_values=legal_values)

    def test_error_deploy_bad_package(self, package_name):
        logging.debug("Deploying a bad package to test error handling...")
        self.deployer_rest_client.deploy_package(package_name)
        # wait for deploy to fail:
        self.await_package_status(package_name,
                                  expected=PackageDeploymentState.NOTDEPLOYED,
                                  legal_values=[PackageDeploymentState.DEPLOYING, PackageDeploymentState.NOTDEPLOYED])
        # check that we have the right error:
        package_info = self.get_package_info(package_name)
        assert package_name + " NotFound" in package_info["information"]

    def test_error_undeploy_bad_package(self, package_name):
        logging.debug("undeploying a bad package to test error handling...")
        try:
            self.deployer_rest_client.undeploy_package(package_name)
            # should never reach this statement:
            assert False
        except AssertionError as ex:
            assert "404" in ex.message

    def test_undeploy(self, test_package):
        logging.debug("Undeploying test package...")
        self.deployer_rest_client.undeploy_package(test_package)
        self.await_package_status(test_package,
                                  expected=PackageDeploymentState.NOTDEPLOYED,
                                  legal_values=[PackageDeploymentState.UNDEPLOYING, PackageDeploymentState.NOTDEPLOYED])

    def await_application_status(self, application_name, expected, legal_values):
        """
        Waits for asynch delpoy command to finish
        :param legal_values:
        :param expected:
        :param package_name:
        :return:
        """
        logging.debug("waiting for asynch operation to finish")
        retries_performed = 0
        while not self.check_application_state(application_name, expected, legal_values):
            time.sleep(2)
            retries_performed += 1
            if retries_performed > 60:
                raise Exception("Exceeded retry limit, Is something wrong?")
            logging.debug("Retry #%s", str(retries_performed))

    def await_package_status(self, package_name, expected, legal_values):
        """
        Waits for asynch delpoy command to finish
        :param legal_values:
        :param expected:
        :param package_name:
        :return:
        """
        logging.debug("waiting for asynch operation to finish")
        retries_performed = 0
        while not self.check_package_state(package_name, expected, legal_values):
            time.sleep(2)
            retries_performed += 1
            if retries_performed > 60:
                raise Exception("Exceeded retry limit, Is something wrong?")
            logging.debug("Retry #%s", str(retries_performed))

    def find_a_test_package(self):
        """
        :return: Find a hello-world type package to use for the rest of the test
        """
        response = self.deployer_rest_client.request_available_packages()
        # extract the package name from the file name:
        test_package = re.search('\"spark-batch-example-app-wf.*?\"',
                                 response.content).group(0).replace(".tar.gz", "").replace("\"", "")

        logging.debug("testing with package: %s", test_package)
        # verify that the name makes sense
        assert len(test_package) > 1
        return test_package

    def is_package_deployed(self, package_name):
        return self.deployer_rest_client.request_deployed_packages().content.find(package_name) != -1

    def is_application_deployed(self, application_name):
        return self.deployer_rest_client.request_deployed_applications().content.find(application_name) != -1

    def get_package_info(self, package_name):
        """
        Similar to status only
        :param package_name:
        :return:
        """
        response = self.deployer_rest_client.request_package_info(
            package_name).content
        json_response = json.loads(response)
        assert json_response["status"]
        return json_response

    def get_application_info(self, application_name):
        """
        Similar to status only
        :param application_name:
        :return:
        """
        response = self.deployer_rest_client.request_application_info(
            application_name).content
        json_response = json.loads(response)
        assert json_response["status"]
        return json_response

    def check_package_state(self, package_name, expected, legal_values):
        response = self.deployer_rest_client.request_package_status(
            package_name).content
        json_response = json.loads(response)
        # validate that response is valid:
        status = json_response["status"]
        assert status in legal_values
        return status == expected

    def check_application_state(self, application_name, expected, legal_values):
        response = self.deployer_rest_client.request_application_status(
            application_name).content
        json_response = json.loads(response)
        # validate that response is valid:
        status = json_response["status"]
        assert status in legal_values
        return status == expected

    def check_yarn_id(self, application_name):
        response = self.deployer_rest_client.request_application_detail(application_name).content
        json_response = json.loads(response)
        assert json_response["status"]
        logging.debug(json_response)
        return len(json_response["yarn_ids"]) > 0


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.getLevelName('DEBUG'),
                        stream=sys.stderr)
    logging.info("testing module on localhost")
    CLIENT_TESTER = DeployerRestClientTester()
    CLIENT_TESTER.run_tests()
