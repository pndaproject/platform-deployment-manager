"""
Purpose:    Unit tests for the the deployer manager
            Run with main(), the easiest way is "nosetests test_*.py"
Author:     PNDA team

Created:    23/03/2016

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
import traceback
from multiprocessing import Event
from mock import Mock, patch, mock_open
from deployment_manager import DeploymentManager
from exceptiondef import NotFound, ConflictingState, FailedValidation, Forbidden
from lifecycle_states import ApplicationState, PackageDeploymentState


def set_dictionary_value(dictionary, key, value):
    """
    convienience method to allow lambdas to set dictionaries
    """
    dictionary[key] = value  # setup mocks:


def throw_error_on_purpose(*_):
    raise Exception("throwing an error for test purposes")


class DeploymentManagerTest(unittest.TestCase):
    def setUp(self):
        """
        Create some global mocks
        """
        mock_repository = Mock()
        mock_package_registar = Mock()
        mock_package_registar.get_package_metadata = Mock(
            return_value={"name": Mock(), "version": Mock(), "metadata": {"component_types": {}, "user": "username"}})
        package_status = {}
        mock_package_registar.get_package_deploy_status = lambda package: package_status.get(package, None)
        mock_package_registar.set_package_deploy_status = \
            lambda package, status: set_dictionary_value(package_status, package, status)

        mock_package_registar.package_exists = lambda package_name: package_name in package_status and \
            package_status.get(package_name)["state"] == "DEPLOYED"
        self.mock_repository = mock_repository
        self.mock_package_registar = mock_package_registar
        self.mock_environment = {
            'webhdfs_host': 'webhdfshost',
            'webhdfs_port': 'webhdfsport',
            'name_node': 'namenode',
            'oozie_uri': 'oozie',
            'cluster_private_key': 'keyfile.pem',
            'cluster_root_user': 'root_user',
            'yarn_node_managers': 'nm1,nm2',
            'hbase_rest_server': 'hbasehost',
            'hbase_thrift_server': 'hbasehost',
            'hbase_rest_port': '123',
            'hive_server': 'hivehost',
            'hive_port': '124',
            'opentsdb': '1.2.3.5:1234',
            'queue_policy': 'echo dev',
            'namespace': 'mockspace'
        }
        self.mock_config = {"deployer_thread_limit": 1, 'stage_root': 'stage', 'plugins_path': 'plugins', 'oozie_spark_version': '1'}
        # mock app registrar:
        mock_application_registar = Mock()
        application_data = {}
        mock_application_registar.application_has_record = lambda app: app in application_data
        mock_application_registar.get_application = lambda app: application_data.get(app, None)
        mock_application_registar.set_application_status = \
            lambda app, status, info=None: set_dictionary_value(application_data, app,
                                                                {"status": status, "information": info, 'overrides':{'user':'username'}})
        self.mock_application_registar = mock_application_registar
        self.mock_summary_registar = Mock()

        self.test_package_name = "aPakageForTesting-1.0.0"
        self.test_app_name = "anAppForTesting"

    def _mock_get_groups(self, _):
        return []

    def test_undeploy_errors(self):
        self.mock_package_registar.package_exists = Mock(return_value=True)

        # mock the package registrar:
        self.mock_package_registar.delete_package = throw_error_on_purpose

        class DeploymentManagerWithLocalCallbacks(DeploymentManager):
            def _state_change_event_package(self, package_name):
                handle_package_state_change(package_name)

            def _assert_package_status(self, package, required_status):
                return True

            def _get_groups(self, user):
                return []

        deployment_manager = self._initialize_deployment_manager(DeploymentManagerWithLocalCallbacks)
        # mock status interface:
        on_complete = Event()
        # holds the result of the test, false if test has not finished yet:
        test_result = [None]

        # intercept callbacks from the asynch worker
        def handle_package_state_change(package_name):
            try:
                if package_name == self.test_package_name:
                    info = deployment_manager.get_package_info(package_name, 'username')
                    print "got async info: " + str(info)
                    # since we are testing for errors, the state needs to eventually change back to "deployed"
                    if info["status"] == PackageDeploymentState.DEPLOYED:
                        if not test_result[0]:
                            # report the final result to the test
                            test_result[0] = {
                                "info": info
                            }
                            on_complete.set()
            except Exception:
                traceback.print_exc()
                if not test_result:
                    on_complete.set()

        # launch the asynch test
        deployment_manager.undeploy_package(self.test_package_name, 'username')
        # wait for test to finsish
        on_complete.wait(5)
        self.assertIsNotNone(test_result[0], "execution completed test")
        info = test_result[0].get("info")
        self.assertTrue("Error undeploying" in info["information"], "expected error message in: " + str(info))

    @patch('os.remove')
    # pylint: disable=unused-argument
    def test_create_application_errors(self, os_mock_rem):

        self.mock_package_registar.package_exists = Mock(return_value=True)
        self.mock_package_registar.get_package_deploy_status = Mock(
            # return_value={"status":PackageDeploymentState.DEPLOYED}
            return_value=None
        )
        verify_app_state_changes = None

        # create manager for testing:
        class DeploymentManagerWithLocalCallbacks(DeploymentManager):
            def _state_change_event_application(self, app_name):
                verify_app_state_changes(app_name)

            def _assert_package_status(self, package, required_status):
                return True

            def _get_groups(self, user):
                return []

        deployment_manager = self._initialize_deployment_manager(DeploymentManagerWithLocalCallbacks)
        # mock status interface:
        on_complete = Event()
        # holds the result of the test, false if test has not finished yet:
        test_result = [None]
        expected_app_statuses = [ApplicationState.CREATING, ApplicationState.NOTCREATED]

        # intercept callbacks from the asynch worker
        verify_app_state_changes = self._create_app_state_verifier(expected_app_statuses, on_complete, test_result,
                                                                   deployment_manager)

        # launch the asynch test
        deployment_manager.create_application(self.test_package_name, self.test_app_name, {'user': 'root'}, 'root')
        # wait for test to finsish
        on_complete.wait(5)
        self.assertIsNotNone(test_result[0], "async task completed")
        info = test_result[0].get("info")
        self.assertTrue("Error creating" in info["information"], "expected error message in: " + str(info))

    @patch('starbase.Connection')
    @patch('pyhs2.connect')
    @patch('datetime.datetime')
    @patch('os.system')
    @patch('deployer_utils.exec_ssh')
    @patch('requests.put')
    @patch('requests.post')
    @patch('deployer_utils.HDFS')
    @patch('spur.ssh')
    @patch('application_creator.shutil')
    @patch('application_creator.os')
    @patch('application_creator.tarfile')
    @patch('os.remove')
    @patch('commands.getstatusoutput')
    # pylint: disable=unused-argument
    def test_create_oozie_error(self, cmd_mock, os_mock_rem, tar_mock, os_mock, shutil_mock, spur_ssh,
                                hdfs_client_mock, post_mock, put_mock, exec_ssh_mock,
                                os_sys_mock, dt_mock, hive_mock, hbase_mock):

        self.mock_package_registar.package_exists = Mock(return_value=True)
        self.mock_package_registar.get_package_deploy_status = Mock(
            # return_value={"status":PackageDeploymentState.DEPLOYED}
            return_value=None
        )
        verify_app_state_changes = None

        # create manager for testing:
        class DeploymentManagerWithLocalCallbacks(DeploymentManager):
            def _state_change_event_application(self, app_name):
                verify_app_state_changes(app_name)

            def _assert_package_status(self, package, required_status):
                return True

            def _get_groups(self, user):
                return []

        deployment_manager = self._initialize_deployment_manager(DeploymentManagerWithLocalCallbacks)
        # mock status interface:
        on_complete = Event()
        # holds the result of the test, false if test has not finished yet:
        test_result = [None]
        expected_app_statuses = [ApplicationState.CREATING, ApplicationState.NOTCREATED]

        # intercept callbacks from the asynch worker
        verify_app_state_changes = self._create_app_state_verifier(expected_app_statuses, on_complete, test_result,
                                                                   deployment_manager)

        class Resp(object):
            status_code = 400
            headers = {'oozie-error-message': 'oozie error!'}

            def json(self):
                return {'id': 'someid'}

        post_mock.return_value = Resp()
        cmd_mock.return_value = (0, 'dev')

        self.mock_package_registar.get_package_data.return_value = 'abcd'
        self.mock_package_registar.get_package_metadata.return_value = {
            'name': 'name',
            'version': '0.0.0',
            'metadata': {
                "component_types": {
                    "oozie": {
                        "componentA": {
                            "component_detail": {
                                "coordinator.xml": {},
                                "properties.json": {}
                            },
                            "component_path": "test_package-1.0.2/oozie/componentA",
                            "component_name": "componentA"
                        }
                    }
                },
                "package_name": "test_package-1.0.3",
                "user": "username"
            }
        }

        with patch("__builtin__.open", mock_open(read_data="[]")):
            deployment_manager.create_application(self.test_package_name, self.test_app_name, {'user': 'root'}, 'root')
            # wait for test to finsish
            on_complete.wait(5)
            self.assertIsNotNone(test_result[0], "async task completed")
            info = test_result[0].get("info")
            self.assertTrue("oozie error!" in info["information"], "expected error message in: " + str(info))

    def _initialize_deployment_manager(self, constructor):
        """
        :return: a deployment manager constructed with the default mocks
        """
        return constructor(
            repository=self.mock_repository,
            package_registrar=self.mock_package_registar,
            application_registrar=self.mock_application_registar,
            application_summary_registrar=self.mock_summary_registar,
            environment=self.mock_environment,
            config=self.mock_config)

    def _create_app_state_verifier(self, expected_app_statuses, on_complete, test_result, deployment_manager):
        """
        Constructs a state verifier to listen for app state changes
        :param expected_app_statuses: a list of in-order states to check for one after the other
        :param on_complete: an event that will be fired once the verification process is over
        :param test_result: a dictionary object which will be set with the application info if the test is succsefull
        :return:  a callback function to call from within the package deployment manager
        """
        next_expected_status_index = [0]

        def verify_app_state_changes(app_name):
            """
            intercept callbacks from the asynch worker
            and assert that they are correct
            """
            try:
                if app_name == self.test_app_name:
                    info = deployment_manager.get_application_info(app_name, 'username')
                    print "got async info2: " + str(info)
                    # since we are testing for errors, the state needs to eventually change back to "deployed"
                    app_status = info["status"]
                    # each time this function is called it expects a different status
                    current_expected_status = expected_app_statuses[next_expected_status_index[0]]
                    next_expected_status_index[0] += 1
                    assert app_status == current_expected_status
                    # if all statuses have been reported:
                    if next_expected_status_index[0] >= len(expected_app_statuses):
                        # end test:
                        test_result[0] = {
                            "info": info
                        }
                        on_complete.set()
            except Exception:
                # end test without reporting result, will cause aasertion error
                traceback.print_exc()
                if not test_result[0]:
                    on_complete.set()

        return verify_app_state_changes

    def test_start_application_errors(self):

        self.mock_package_registar.package_exists = Mock(return_value=True)
        self.mock_package_registar.get_package_deploy_status = Mock(
            # return_value={"status":PackageDeploymentState.DEPLOYED}
            return_value=None
        )
        verify_app_state_changes = None

        # create manager for testing:
        class DeploymentManagerWithLocalCallbacks(DeploymentManager):
            def _state_change_event_application(self, app_name):
                verify_app_state_changes(app_name)

            def _assert_package_status(self, package, required_status):
                return True

            def _get_groups(self, user):
                return []

        deployment_manager = self._initialize_deployment_manager(DeploymentManagerWithLocalCallbacks)
        # mock status interface:
        on_complete = Event()
        # holds the result of the test, false if test has not finished yet:
        test_result = [None]
        expected_app_statuses = [ApplicationState.STARTING, ApplicationState.CREATED]

        verify_app_state_changes = self._create_app_state_verifier(expected_app_statuses, on_complete, test_result,
                                                                   deployment_manager)

        self.mock_application_registar.set_application_status(self.test_app_name, ApplicationState.CREATED)
        # launch the asynch test
        deployment_manager.start_application(self.test_app_name, 'username')
        # wait for test to finsish
        on_complete.wait(5)
        self.assertIsNotNone(test_result[0], "async task completed")
        info = test_result[0].get("info")
        self.assertTrue("Error starting" in info["information"], "expected error message in: " + str(info))

    def test_delete_application_errors(self):
        self.mock_package_registar.package_exists = Mock(return_value=True)
        self.mock_package_registar.get_package_deploy_status = Mock(
            # return_value={"status":PackageDeploymentState.DEPLOYED}
            return_value=None
        )
        verify_app_state_changes = None

        # create manager for testing:
        class DeploymentManagerWithLocalCallbacks(DeploymentManager):
            def _state_change_event_application(self, app_name):
                verify_app_state_changes(app_name)

            def _assert_package_status(self, package, required_status):
                return True

            def _get_groups(self, user):
                return []

        deployment_manager = self._initialize_deployment_manager(DeploymentManagerWithLocalCallbacks)
        # mock status interface:
        on_complete = Event()
        # holds the result of the test, false if test has not finished yet:
        test_result = [None]
        expected_app_statuses = [ApplicationState.DESTROYING, ApplicationState.STARTED]

        verify_app_state_changes = self._create_app_state_verifier(expected_app_statuses, on_complete, test_result,
                                                                   deployment_manager)

        self.mock_application_registar.set_application_status(self.test_app_name, ApplicationState.STARTED)
        # launch the asynch test
        deployment_manager.delete_application(self.test_app_name, 'username')
        # wait for test to finsish
        on_complete.wait(5)
        self.assertIsNotNone(test_result[0], "async task completed")
        info = test_result[0].get("info")
        self.assertTrue("Error deleting" in info["information"], "expected error message in: " + str(info))

    def test_stop_application_errors(self):
        self.mock_package_registar.package_exists = Mock(return_value=True)
        self.mock_package_registar.get_package_deploy_status = Mock(
            # return_value={"status":PackageDeploymentState.DEPLOYED}
            return_value=None
        )
        verify_app_state_changes = None

        # create manager for testing:
        class DeploymentManagerWithLocalCallbacks(DeploymentManager):
            def _state_change_event_application(self, app_name):
                verify_app_state_changes(app_name)

            def _assert_package_status(self, package, required_status):
                return True

            def _get_groups(self, user):
                return []

        deployment_manager = self._initialize_deployment_manager(DeploymentManagerWithLocalCallbacks)
        # mock status interface:
        on_complete = Event()
        # holds the result of the test, false if test has not finished yet:
        test_result = [None]
        expected_app_statuses = [ApplicationState.STOPPING, ApplicationState.STARTED]

        verify_app_state_changes = self._create_app_state_verifier(expected_app_statuses, on_complete, test_result,
                                                                   deployment_manager)

        self.mock_application_registar.set_application_status(self.test_app_name, ApplicationState.STARTED)
        # launch the asynch test
        deployment_manager.stop_application(self.test_app_name, 'username')
        # wait for test to finsish
        on_complete.wait(5)
        self.assertIsNotNone(test_result[0], "async task completed")
        info = test_result[0].get("info")
        self.assertTrue("Error stopping" in info["information"], "expected error message in: " + str(info))

    def test_stop_application(self):
        self.mock_package_registar.package_exists = Mock(return_value=True)
        self.mock_package_registar.get_package_deploy_status = Mock(
            # return_value={"status":PackageDeploymentState.DEPLOYED}
            return_value=None
        )

        application_callback_name = "application_callback"
        self.mock_config[application_callback_name] = application_callback_name

        class MockDeploymentManager(DeploymentManager):
            def create_mocks(self):
                self._application_creator = Mock()

            def _get_groups(self, user):
                return []

        deployment_manager = self._initialize_deployment_manager(MockDeploymentManager)
        deployment_manager.create_mocks()
        deployment_manager.rest_client = Mock()
        # mock status interface:
        on_complete = Event()
        # holds the result of the test, false if test has not finished yet:
        test_result = [None]
        on_rest_callback = self._create_rest_callback_verifier(expected_statuses=["STOPPING", "CREATED"],
                                                               on_complete=on_complete, test_result=test_result)
        deployment_manager.rest_client.post = on_rest_callback

        # launch the asynch test
        self.mock_application_registar.set_application_status(self.test_app_name, "STARTED")
        deployment_manager.stop_application(self.test_app_name, 'username')
        # wait for test to finsish
        on_complete.wait(5)
        self.assertIsNotNone(test_result[0], "async task completed")
        info = test_result[0]
        self.assertEquals(info.get("data")[0]["state"], "CREATED")

    def _create_rest_callback_verifier(self, expected_statuses, on_complete, test_result):
        """
        Constructs a mock rest interface to verify states uppon rest callback
        :param expected_app_statuses: a list of in-order states to check for one after the other
        :param on_complete: an event that will be fired once the verification process is over
        :param test_result: a dictionary object which will be set with the application info if the test is succsefull
        :return:  a callback function to call from within the package deployment manager
        """
        next_expected_status_index = [0]

        def verify_app_state_changes(callback_url, json):
            """
            intercept callbacks from the asynch worker
            and assert that they are correct
            """
            self.assertIsNotNone(callback_url)
            try:
                info = json
                print "got async info1: " + str(info)
                # since we are testing for errors, the state needs to eventually change back to "deployed"
                status = json["data"][0]["state"]
                # each time this function is called it expects a different status
                current_expected_status = expected_statuses[next_expected_status_index[0]]
                next_expected_status_index[0] += 1
                assert status == current_expected_status
                # if all statuses have been reported:
                if next_expected_status_index[0] >= len(expected_statuses):
                    # end test:
                    test_result[0] = json
                    on_complete.set()
            except Exception:
                # end test without reporting result, will cause aasertion error
                traceback.print_exc()
                if not test_result[0]:
                    on_complete.set()

        return verify_app_state_changes

    @patch('os.remove')
    # pylint: disable=unused-argument
    def test_deploy_package_with_errors(self, os_mock):
        """
        Tests asynchronous error reporting in the deploy-package process.
        """

        package_callback_name = "package_callback"
        self.mock_config[package_callback_name] = package_callback_name
        self.mock_package_registar.self.package_exists = Mock(return_value=False)

        class MockDeploymentManager(DeploymentManager):
            def create_mocks(self):
                self._application_creator = Mock()
                self._package_parser = Mock()

                def throwerr(_):
                    raise FailedValidation("Failed validation")
                self._package_parser.get_package_metadata.side_effect = throwerr

            def _get_groups(self, user):
                return []

        deployment_manager = self._initialize_deployment_manager(MockDeploymentManager)
        deployment_manager.create_mocks()
        deployment_manager.rest_client = Mock()
        # mock status interface:
        on_complete = Event()
        # holds the result of the test, false if test has not finished yet:
        test_result = [None]
        # listen for callbacks from the deployer:
        on_rest_callback = self._create_rest_callback_verifier(expected_statuses=[PackageDeploymentState.DEPLOYING, PackageDeploymentState.NOTDEPLOYED],
                                                               on_complete=on_complete, test_result=test_result)
        deployment_manager.rest_client.post = on_rest_callback
        self.mock_application_registar.set_application_status(self.test_app_name, ApplicationState.STARTED)
        # launch the asynch test
        deployment_manager.deploy_package(self.test_package_name, 'username')
        # wait for test to finsish
        on_complete.wait(5)
        self.assertIsNotNone(test_result[0], "async task completed")
        info = test_result[0]
        # check that error  was reported:
        self.assertEquals(info.get("data")[0]["state"], PackageDeploymentState.NOTDEPLOYED)
        self.assertTrue("Failed validation" in info.get("data")[0]["information"])

    @patch('os.remove')
    # pylint: disable=unused-argument
    def test_deploy_package(self, os_mock):
        """
        Tests asynchronous deploy-package process.
        """

        package_callback_name = "package_callback"
        self.mock_config[package_callback_name] = package_callback_name
        self.mock_package_registar.self.package_exists = Mock(return_value=False)
        self.mock_repository.get_package.return_value = 'abcd'

        class MockDeploymentManager(DeploymentManager):
            def create_mocks(self):
                self._application_creator = Mock()
                self._package_parser = Mock()

            def _get_groups(self, user):
                return []

        deployment_manager = self._initialize_deployment_manager(MockDeploymentManager)
        deployment_manager.create_mocks()
        deployment_manager.rest_client = Mock()
        # mock status interface:
        on_complete = Event()
        # holds the result of the test, false if test has not finished yet:
        test_result = [None]
        # listen for callbacks from the deployer:
        on_rest_callback = self._create_rest_callback_verifier(expected_statuses=[PackageDeploymentState.DEPLOYING, PackageDeploymentState.DEPLOYED],
                                                               on_complete=on_complete, test_result=test_result)
        deployment_manager.rest_client.post = on_rest_callback
        self.mock_application_registar.set_application_status(self.test_app_name, ApplicationState.STARTED)
        # launch the asynch test
        deployment_manager.deploy_package(self.test_package_name, 'username')
        # wait for test to finsish
        on_complete.wait(5)
        self.assertIsNotNone(test_result[0], "async task completed")
        info = test_result[0]
        # check that error  was reported:
        self.assertEquals(info.get("data")[0]["state"], PackageDeploymentState.DEPLOYED)
        self.assertFalse("Error deploying" in info.get("data")[0]["information"])

    def test_get_environment(self):
        repository = Mock()
        package_registrar = Mock()
        application_registrar = Mock()
        application_summary_registrar = Mock()
        environment = {"namespace": "some_namespace", 'webhdfs_host': 'webhdfshost', 'webhdfs_port': 'webhdfsport'}
        config = {"deployer_thread_limit": 10}

        dmgr = DeploymentManager(repository,
                                 package_registrar,
                                 application_registrar,
                                 application_summary_registrar,
                                 environment,
                                 config)
        dmgr._get_groups = self._mock_get_groups #pylint: disable =protected-access

        self.assertEqual(dmgr.get_environment('username'), environment)

    def test_list_packages(self):
        expected_packages = ['package-1.0.0', 'another-2.1.2']

        repository = Mock()
        package_registrar = Mock()
        package_registrar.list_packages.return_value = expected_packages
        application_registrar = Mock()
        application_summary_registrar = Mock()
        environment = {"namespace": "some_namespace", 'webhdfs_host': 'webhdfshost', 'webhdfs_port': 'webhdfsport'}
        config = {"deployer_thread_limit": 10}

        dmgr = DeploymentManager(repository,
                                 package_registrar,
                                 application_registrar,
                                 application_summary_registrar,
                                 environment,
                                 config)
        dmgr._get_groups = self._mock_get_groups #pylint: disable =protected-access

        self.assertEqual(dmgr.list_packages('username'), expected_packages)

    def test_list_repository(self):
        expected_packages = [{
            'latest_versions': [{
                'version': '1.0.26',
                'file': 'spark-batch-example-app-1.0.26.tar.gz'
            }],
            'name': 'spark-batch-example-app'
        }, {
            'latest_versions': [{
                'version': '1.0.30',
                'file': 'spark-batch-example-app-c-1.0.30.tar.gz'
            }],
            'name': 'spark-batch-example-app-c'
        }]

        repository = Mock()
        repository.get_package_list.return_value = expected_packages

        package_registrar = Mock()
        application_registrar = Mock()
        application_summary_registrar = Mock()
        environment = {"namespace": "some_namespace", 'webhdfs_host': 'webhdfshost', 'webhdfs_port': 'webhdfsport'}
        config = {"deployer_thread_limit": 10}

        dmgr = DeploymentManager(repository,
                                 package_registrar,
                                 application_registrar,
                                 application_summary_registrar,
                                 environment,
                                 config)
        dmgr._get_groups = self._mock_get_groups #pylint: disable =protected-access

        self.assertEqual(dmgr.list_repository(1, 'username'), expected_packages)

    def test_get_unknown_package_info(self):
        repository = Mock()
        package_registrar = Mock()
        package_registrar.get_package_deploy_status.return_value = None
        package_registrar.package_exists.return_value = False
        application_registrar = Mock()
        application_summary_registrar = Mock()
        environment = {"namespace": "some_namespace", 'webhdfs_host': 'webhdfshost', 'webhdfs_port': 'webhdfsport'}
        config = {"deployer_thread_limit": 10}

        dmgr = DeploymentManager(repository,
                                 package_registrar,
                                 application_registrar,
                                 application_summary_registrar,
                                 environment,
                                 config)
        dmgr._get_groups = self._mock_get_groups #pylint: disable =protected-access

        self.assertEqual(dmgr.get_package_info("something-1.0.0", 'username'), {
            'defaults': None,
            'information': None,
            'user': None,
            'name': 'something',
            'status': PackageDeploymentState.NOTDEPLOYED,
            'version': '1.0.0'})

    def test_list_package_applications(self):
        expected_applications = ['app1', 'app2']

        repository = Mock()
        package_registrar = Mock()
        application_registrar = Mock()
        application_registrar.list_applications_for_package.return_value = expected_applications
        application_summary_registrar = Mock()
        environment = {"namespace": "some_namespace", 'webhdfs_host': 'webhdfshost', 'webhdfs_port': 'webhdfsport'}
        config = {"deployer_thread_limit": 10}

        dmgr = DeploymentManager(repository,
                                 package_registrar,
                                 application_registrar,
                                 application_summary_registrar,
                                 environment,
                                 config)
        dmgr._get_groups = self._mock_get_groups #pylint: disable =protected-access

        self.assertEqual(dmgr.list_package_applications("package", 'username'), expected_applications)

    def test_list_applications(self):
        expected_applications = ['app1', 'app2']

        repository = Mock()
        package_registrar = Mock()
        application_registrar = Mock()
        application_registrar.list_applications.return_value = expected_applications
        application_summary_registrar = Mock()
        environment = {"namespace": "some_namespace", 'webhdfs_host': 'webhdfshost', 'webhdfs_port': 'webhdfsport'}
        config = {"deployer_thread_limit": 10}

        dmgr = DeploymentManager(repository,
                                 package_registrar,
                                 application_registrar,
                                 application_summary_registrar,
                                 environment,
                                 config)
        dmgr._get_groups = self._mock_get_groups #pylint: disable =protected-access

        self.assertEqual(dmgr.list_applications('username'), expected_applications)

    def test_application_in_progress(self):
        repository = Mock()
        package_registrar = Mock()
        application_registrar = Mock()
        application_registrar.get_application.return_value = {
            'overrides': {'user': 'username'},
            'defaults': {},
            'name': 'name',
            'package_name': 'package_name',
            'status': ApplicationState.STARTING,
            'information': None}
        application_summary_registrar = Mock()
        environment = {"namespace": "some_namespace", 'webhdfs_host': 'webhdfshost', 'webhdfs_port': 'webhdfsport'}
        config = {"deployer_thread_limit": 10}

        dmgr = DeploymentManager(repository,
                                 package_registrar,
                                 application_registrar,
                                 application_summary_registrar,
                                 environment,
                                 config)
        dmgr._get_groups = self._mock_get_groups #pylint: disable =protected-access

        self.assertRaises(ConflictingState, dmgr.start_application, "name", 'username')

    def test_package_in_progress(self):
        repository = Mock()
        package_registrar = Mock()
        application_registrar = Mock()
        application_summary_registrar = Mock()
        package_registrar.package_exists.return_value = True
        package_registrar.get_package_metadata.return_value = {
            'metadata': {'user': 'username'}
        }
        environment = {"namespace": "some_namespace", 'webhdfs_host': 'webhdfshost', 'webhdfs_port': 'webhdfsport'}
        config = {"deployer_thread_limit": 10}

        class DeploymentManagerTester(DeploymentManager):
            def set_package_progress(self, package, state):
                self._set_package_progress(package, state)

        dmgr = DeploymentManagerTester(repository,
                                       package_registrar,
                                       application_registrar,
                                       application_summary_registrar,
                                       environment,
                                       config)
        dmgr.set_package_progress("name", PackageDeploymentState.DEPLOYING)
        dmgr._get_groups = self._mock_get_groups #pylint: disable =protected-access

        self.assertRaises(ConflictingState, dmgr.undeploy_package, "name", "username")

    def test_package_not_exists(self):
        repository = Mock()
        package_registrar = Mock()
        package_registrar.get_package_deploy_status.return_value = None
        package_registrar.package_exists.return_value = False
        application_registrar = Mock()
        application_summary_registrar = Mock()
        environment = {"namespace": "some_namespace", 'webhdfs_host': 'webhdfshost', 'webhdfs_port': 'webhdfsport'}
        config = {"deployer_thread_limit": 10}
        dmgr = DeploymentManager(repository,
                                 package_registrar,
                                 application_registrar,
                                 application_summary_registrar,
                                 environment,
                                 config)
        dmgr._get_groups = self._mock_get_groups #pylint: disable =protected-access

        self.assertRaises(NotFound, dmgr.undeploy_package, "name", "username")

    @patch('deployment_manager.application_creator.ApplicationCreator')
    def test_get_application_detail(self, app_mock):
        app_mock.return_value.get_application_runtime_details.return_value = {'yarn_ids': []}
        repository = Mock()
        package_registrar = Mock()
        application_registrar = Mock()
        application_registrar.get_create_data.return_value = {}
        application_registrar.application_has_record.return_value = True
        application_registrar.get_application.return_value = {
            'overrides': {'user': 'username'},
            'defaults': {},
            'name': 'name',
            'package_name': 'package_name',
            'status': ApplicationState.STARTED,
            'information': None}
        application_summary_registrar = Mock()
        environment = {"namespace": "some_namespace", 'webhdfs_host': 'webhdfshost', 'webhdfs_port': 'webhdfsport'}
        config = {"deployer_thread_limit": 10}

        dmgr = DeploymentManager(repository,
                                 package_registrar,
                                 application_registrar,
                                 application_summary_registrar,
                                 environment,
                                 config)
        dmgr._get_groups = self._mock_get_groups #pylint: disable =protected-access

        self.assertEqual(dmgr.get_application_detail('name', 'username'), {'name': 'name', 'status': ApplicationState.STARTED, 'yarn_ids': []})

    @patch('deployment_manager.application_creator.ApplicationCreator')
    def test_fail_get_detail(self, app_mock):
        app_mock.return_value.get_application_runtime_details.return_value = {'yarn_ids': []}
        repository = Mock()
        package_registrar = Mock()
        application_registrar = Mock()
        application_registrar.get_create_data.return_value = {}
        application_registrar.application_has_record.return_value = True
        application_registrar.get_application.return_value = {
            'overrides': {'user': 'username'},
            'defaults': {},
            'name': 'name',
            'package_name': 'package_name',
            'status': ApplicationState.NOTCREATED,
            'information': None}
        application_summary_registrar = Mock()
        environment = {"namespace": "some_namespace", 'webhdfs_host': 'webhdfshost', 'webhdfs_port': 'webhdfsport'}
        config = {"deployer_thread_limit": 10}

        dmgr = DeploymentManager(repository,
                                 package_registrar,
                                 application_registrar,
                                 application_summary_registrar,
                                 environment,
                                 config)
        dmgr._get_groups = self._mock_get_groups #pylint: disable =protected-access

        self.assertRaises(NotFound, dmgr.get_application_detail, 'name', 'username')

    def test_get_application_summary(self):
        repository = Mock()
        package_registrar = Mock()
        application_registrar = Mock()
        application_summary_registrar = Mock()
        application_summary_registrar.get_summary_data.return_value = {'name':{
            'aggregate_status': 'COMPLETED_WITH_NO_FAILURES',
            'component-1': {}
        }}
        application_registrar.application_has_record.return_value = True
        application_registrar.get_application.return_value = {
            'overrides': {'user': 'username'},
            'defaults': {},
            'name': 'name',
            'package_name': 'package_name',
            'status': ApplicationState.NOTCREATED,
            'information': None}
        environment = {"namespace": "some_namespace", 'webhdfs_host': 'webhdfshost', 'webhdfs_port': 'webhdfsport'}
        config = {"deployer_thread_limit": 10}

        dmgr = DeploymentManager(repository,
                                 package_registrar,
                                 application_registrar,
                                 application_summary_registrar,
                                 environment,
                                 config)
        dmgr._get_groups = self._mock_get_groups #pylint: disable =protected-access

        self.assertEqual(dmgr.get_application_summary('name', 'username'), {'name':{'aggregate_status': 'COMPLETED_WITH_NO_FAILURES', 'component-1': {}}})

    def test_unauthorized_user_start(self):
        self.mock_package_registar.package_exists = Mock(return_value=True)
        self.mock_package_registar.get_package_deploy_status = Mock(
            return_value=None
        )

        application_callback_name = "application_callback"
        self.mock_config[application_callback_name] = application_callback_name

        class MockDeploymentManager(DeploymentManager):
            def create_mocks(self):
                self._application_creator = Mock()

            def _get_groups(self, user):
                return []

        deployment_manager = self._initialize_deployment_manager(MockDeploymentManager)
        deployment_manager.create_mocks()
        deployment_manager.rest_client = Mock()
        # mock status interface:
        on_complete = Event()
        # holds the result of the test, false if test has not finished yet:
        test_result = [[]]
        on_rest_callback = self._create_rest_callback_verifier(expected_statuses=["STARTING", "CREATED"],
                                                               on_complete=on_complete, test_result=test_result)
        deployment_manager.rest_client.post = on_rest_callback

        # launch the asynch test
        self.mock_application_registar.set_application_status(self.test_app_name, "CREATED")

        def expect_exception():
            deployment_manager.start_application(self.test_app_name, 'username2')

        self.assertRaises(Forbidden, expect_exception)
