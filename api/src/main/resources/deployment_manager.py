"""
Name:       deployment_manager.py
Purpose:    Implements the packages API and applications API
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
import time
import datetime
import threading
import traceback
import requests

import application_creator
from exceptiondef import ConflictingState, NotFound, ExceptionThatShouldBeDisplayedToCaller
from package_parser import PackageParser
from async_dispatcher import AsyncDispatcher
from lifecycle_states import ApplicationState, PackageDeploymentState


def milli_time():
    return int(round(time.time() * 1000))


class DeploymentManager(object):
    def __init__(self, repository, package_registrar, application_registrar, environment, config):
        self._repository = repository
        self._package_registrar = package_registrar
        self._application_registrar = application_registrar
        self._environment = environment
        self._config = config
        self._application_creator = application_creator.ApplicationCreator(config, environment,
                                                                           environment['namespace'])
        self._package_parser = PackageParser()
        self._package_progress = {}
        self._lock = threading.RLock()
        # load number of threads from config file:
        number_of_threads = self._config["deployer_thread_limit"]
        assert isinstance(number_of_threads, (int))
        assert number_of_threads > 0
        self.dispatcher = AsyncDispatcher(num_threads=number_of_threads)
        self.rest_client = requests

    def get_environment(self):
        return self._environment

    def list_packages(self):
        logging.info('list_deployed')
        deployed = self._package_registrar.list_packages()
        return deployed

    def _assert_package_status(self, package, required_status):
        status = self.get_package_info(package)['status']
        if status != required_status:
            if status == PackageDeploymentState.NOTDEPLOYED:
                raise NotFound(json.dumps({'status': status}))
            else:
                raise ConflictingState(json.dumps({'status': status}))

    def list_repository(self, recency):
        logging.info("list_available: %s", recency)
        available = self._repository.list_packages(recency)
        return available

    def get_package_info(self, package):
        information = None
        progress_state = self._get_package_progress(package)
        if progress_state is not None:
            properties = None
            status = progress_state
            name = package.rpartition('-')[0]
            version = package.rpartition('-')[2]
        else:
            # package deploy is not in progress:
            # get last package status from database
            deploy_status = self._package_registrar.get_package_deploy_status(package)
            if deploy_status:
                status = deploy_status["state"]
                information = deploy_status["information"]
            # check if package data exists in database:
            if self._package_registrar.package_exists(package):
                metadata = self._package_registrar.get_package_metadata(package)
                properties = self._package_parser.properties_from_metadata(metadata['metadata'])
                status = PackageDeploymentState.DEPLOYED
                name = metadata['name']
                version = metadata['version']
            else:
                if not deploy_status:
                    status = PackageDeploymentState.NOTDEPLOYED
                properties = None
                name = package.rpartition('-')[0]
                version = package.rpartition('-')[2]

        ret = {"name": name,
               "version": version,
               "status": status,
               "defaults": properties,
               "information": information}

        return ret

    def _run_asynch_package_task(self, package_name, initial_state, working_state, task):
        """
        Manages locks and state reporting for async background operations on packages
        :param package_name: The name of the package to operate on
        :param initial_state: The state to check before beginning work on the package
        :param working_state: The state to set while the package operation is being carried out.
        :param task: The actual work to be carried out
        """
        with self._lock:
            # check that package is in the right state before starting operation:
            self._assert_package_status(package_name, initial_state)
            # set the operation state before starting:
            self._set_package_progress(package_name, working_state)

        # this will be run in the background while taking care to release all locks and intermediate states:
        def do_work_and_report_progress():
            try:
                # report beginning of work to external APIs:
                self._state_change_event_package(package_name)
                # do the actual work:
                task()
            finally:
                # release the lock on the package:
                self._clear_package_progress(package_name)
                # report completion to external APIs
                self._state_change_event_package(package_name)

        # run everything on a background thread:
        self.dispatcher.run_as_asynch(task=do_work_and_report_progress)

    def deploy_package(self, package):
        # this function will be executed in the background:
        def _do_deploy():
            # if this value is not changed, then it is assumed that the operation never completed
            deploy_status = {"state": PackageDeploymentState.NOTDEPLOYED, "information": "Error deploying " + package}
            try:
                package_file = package + '.tar.gz'
                logging.info("deploy: %s", package)
                # download package:
                package_data = self._repository.download_package(package_file)
                # put package in database:
                metadata = self._package_parser.get_package_metadata(package_data)
                self._application_creator.validate_package(package, metadata)
                self._package_registrar.set_package(package, package_data)
                # set the operation status as complete
                deploy_status = {"state": PackageDeploymentState.DEPLOYED,
                                 "information": "Deployed " + package + " at " + self.utc_string()}
                logging.info("deployed: %s", package)
            except ExceptionThatShouldBeDisplayedToCaller as ex:
                # log error to screen:
                logging.error(ex.msg)
                # prepare human readable message
                error_message = package + " " + str(type(ex).__name__) + ", details: " + json.dumps(ex.msg)
                # set the status:
                deploy_status = {"state": PackageDeploymentState.NOTDEPLOYED, "information": error_message}
                raise
            finally:
                # report final state of operation to database:
                self._package_registrar.set_package_deploy_status(package, deploy_status)

        # schedule work to be done in the background:
        self._run_asynch_package_task(package_name=package,
                                      initial_state=PackageDeploymentState.NOTDEPLOYED,
                                      working_state=PackageDeploymentState.DEPLOYING,
                                      task=_do_deploy)

    def utc_string(self):
        return datetime.datetime.utcnow().isoformat()

    def undeploy_package(self, package):
        # this function will be executed in the background:
        def do_undeploy():
            undeploy_failed = True
            # this will be the default error message if it is not clear what the error was:
            deploy_status = {"state": PackageDeploymentState.DEPLOYED, "information": "Error undeploying " + package}
            try:

                logging.info("undeploy: %s", package)
                self._package_registrar.delete_package(package)
                logging.info("undeployed: %s", package)
                deploy_status = None
                undeploy_failed = False
            except ExceptionThatShouldBeDisplayedToCaller as ex:
                # log error to screen:
                logging.error(ex.msg)
                # prepare human readable message
                error_message = package + " " + str(type(ex).__name__) + ", details: " + json.dumps(ex.msg)
                # set the status:
                deploy_status = {"state": PackageDeploymentState.NOTDEPLOYED, "information": error_message}
                raise
            finally:
                if undeploy_failed:
                    # persist any errors in the database, but still throw them:
                    self._package_registrar.set_package_deploy_status(package, deploy_status)

        # schedule work to be done in the background:
        self._run_asynch_package_task(package_name=package,
                                      initial_state=PackageDeploymentState.DEPLOYED,
                                      working_state=PackageDeploymentState.UNDEPLOYING,
                                      task=do_undeploy)

    def _set_package_progress(self, package_name, state):
        """
        Marks the progress of background operations being run on the app.
        :param package_name: the name of the package to be modified
        :param state: the state of the background operation
        """
        # currently we are using multiple threads, so this lock is added for thread saftey
        with self._lock:
            self._package_progress[package_name] = state

    def _get_package_progress(self, package_name):
        """
        :param package_name: The name of the package for which to query progress
        :return: the state of the package
        """
        with self._lock:
            if self._is_package_in_progress(package_name):
                return self._package_progress[package_name]
            return None

    def _is_package_in_progress(self, package_name):
        """
        checks if the current package has an operation in progress
        :param package_name: the name of the package to check
        :return: true if the package is currently being operated on
        """
        with self._lock:
            return package_name in self._package_progress

    def _clear_package_progress(self, package):
        with self._lock:
            self._package_progress.pop(package, None)

    def _mark_destroying(self, package):
        self._set_package_progress(package, ApplicationState.DESTROYING)

    def _mark_creating(self, package):
        self._set_package_progress(package, ApplicationState.CREATING)

    def _mark_starting(self, package):
        self._set_package_progress(package, ApplicationState.STARTING)

    def _mark_stopping(self, package):
        self._set_package_progress(package, ApplicationState.STOPPING)

    def list_package_applications(self, package):
        logging.info('list_package_applications')
        applications = self._application_registrar.list_applications_for_package(package)
        return applications

    def list_applications(self):
        logging.info('list_applications')
        applications = self._application_registrar.list_applications()
        return applications

    def _assert_application_status(self, application, required_status):
        app_info = self.get_application_info(application)
        status = app_info['status']

        if (isinstance(required_status, list) and status not in required_status) \
                or (not isinstance(required_status, list) and status != required_status):
            if status == ApplicationState.NOTCREATED:
                raise NotFound(json.dumps({'status': status}))
            else:
                raise ConflictingState(json.dumps({'status': status}))

    def _assert_application_exists(self, application):
        status = self.get_application_info(application)['status']
        if status == ApplicationState.NOTCREATED:
            raise NotFound(json.dumps({'status': status}))

    def start_application(self, application):
        logging.info('start_application')
        with self._lock:
            self._assert_application_status(application, ApplicationState.CREATED)
            self._mark_starting(application)

        def do_work():
            try:
                self._state_change_event_application(application)
                try:
                    create_data = self._application_registrar.get_create_data(application)
                    self._application_creator.start_application(application, create_data)
                    self._application_registrar.set_application_status(application, ApplicationState.STARTED)
                except ExceptionThatShouldBeDisplayedToCaller as ex:
                    self._handle_application_error(application, ex, ApplicationState.CREATED)
                    raise
                except Exception as ex:
                    # report the error:
                    self._application_registrar.set_application_status(application, ApplicationState.CREATED,
                                                                       "Error starting application: " + application)
                    # re-throw the exception after reporting it
                    raise Exception(ex)
            finally:
                self._clear_package_progress(application)
                self._state_change_event_application(application)

        self.dispatcher.run_as_asynch(task=do_work)

    def stop_application(self, application):
        logging.info('stop_application')
        with self._lock:
            self._assert_application_status(application, ApplicationState.STARTED)
            self._mark_stopping(application)

        def do_work():
            try:
                self._state_change_event_application(application)
                try:
                    create_data = self._application_registrar.get_create_data(application)
                    self._application_creator.stop_application(application, create_data)
                    self._application_registrar.set_application_status(application, ApplicationState.CREATED)
                except ExceptionThatShouldBeDisplayedToCaller as ex:
                    self._handle_application_error(application, ex, ApplicationState.STARTED)
                    raise
                except Exception as ex:
                    # report the error:
                    self._application_registrar.set_application_status(application, ApplicationState.STARTED,
                                                                       "Error stopping application: " + application)
                    # re-throw the exception after reporting it
                    raise Exception(ex)
            finally:
                self._clear_package_progress(application)
                self._state_change_event_application(application)

        self.dispatcher.run_as_asynch(task=do_work)

    def get_application_info(self, application):
        logging.info('get_application_info')

        if not self._application_registrar.application_has_record(application):
            record = {'status': ApplicationState.NOTCREATED, 'information': None}
        else:
            record = self._application_registrar.get_application(application)
        progress_state = self._get_package_progress(application)
        if progress_state is not None:
            record['status'] = progress_state

        return record

    def get_application_detail(self, application):
        logging.info('get_application_detail')
        self._assert_application_exists(application)
        create_data = self._application_registrar.get_create_data(application)
        record = self._application_creator.get_application_runtime_details(application, create_data)
        record['status'] = self.get_application_info(application)['status']
        record['name'] = application
        return record

    def create_application(self, package, application, overrides):
        logging.info('create_application')

        with self._lock:
            self._assert_application_status(application, ApplicationState.NOTCREATED)
            self._assert_package_status(package, PackageDeploymentState.DEPLOYED)
            defaults = self.get_package_info(package)['defaults']
            package_data = self._package_registrar.get_package_data(package)
            self._application_registrar.create_application(package, application, overrides, defaults)
            self._mark_creating(application)

        def do_work():
            try:
                self._state_change_event_application(application)
                try:
                    package_metadata = self._package_registrar.get_package_metadata(package)['metadata']
                    create_data = self._application_creator.create_application(
                        package_data, package_metadata, application, overrides)
                    self._application_registrar.set_create_data(application, create_data)
                    self._application_registrar.set_application_status(application, ApplicationState.CREATED)
                except ExceptionThatShouldBeDisplayedToCaller as ex:
                    self._handle_application_error(application, ex, ApplicationState.NOTCREATED)
                    raise
                except Exception as ex:
                    self._application_registrar.set_application_status(application, ApplicationState.NOTCREATED,
                                                                       "Error creating application: " + application)
                    logging.error(traceback.format_exc(ex))
                    raise ex
            finally:
                # clear inner locks:
                self._clear_package_progress(application)
                self._state_change_event_application(application)

        self.dispatcher.run_as_asynch(task=do_work)

    def _handle_application_error(self, application, ex, app_status):
        """
        Use to handle application exceptions which should be relayed back to the user
        Sets the application state to an error
        :param application: The app for which to set the error
        :param ex: The error
        :param app_status: The status the app should be at following the error.
        """
        # log error to screen:
        logging.error(ex.msg)
        # prepare human readable message
        error_message = application + " " + str(type(ex).__name__) + ", details: " + json.dumps(ex.msg)
        # set the status:
        self._application_registrar.set_application_status(application, app_status, error_message)

    def delete_application(self, application):
        logging.info('delete_application')
        with self._lock:
            self._assert_application_status(application, [ApplicationState.CREATED, ApplicationState.STARTED])
            self._mark_destroying(application)

        def do_work():
            try:
                self._state_change_event_application(application)
                try:
                    create_data = self._application_registrar.get_create_data(application)
                    self._application_creator.destroy_application(application, create_data)
                    self._application_registrar.delete_application(application)
                except ExceptionThatShouldBeDisplayedToCaller as ex:
                    self._handle_application_error(application, ex, ApplicationState.STARTED)
                    raise
                except Exception as ex:
                    # report the error:
                    self._application_registrar.set_application_status(application, ApplicationState.STARTED,
                                                                       "Error deleting application: " + application)
                    # re-throw the exception after reporting it
                    raise Exception(ex)
            finally:
                self._clear_package_progress(application)
                self._state_change_event_application(application)

        self.dispatcher.run_as_asynch(task=do_work)

    def _state_change_event_application(self, name):
        endpoint_type = "application_callback"
        info = self.get_application_info(name)
        self._state_change_event(name, endpoint_type, info['status'], info['information'])

    def _state_change_event_package(self, name):
        endpoint_type = "package_callback"
        info = self.get_package_info(name)
        self._state_change_event(name, endpoint_type, info['status'], info['information'])

    def _state_change_event(self, name, endpoint_type, state, information):
        callback_url = self._config[endpoint_type]
        if callback_url:
            logging.debug("callback: %s %s %s", endpoint_type, name, state)
            callback_payload = {
                "data": [
                    {
                        "id": name,
                        "state": state,
                        "timestamp": milli_time()
                    }
                ],
                "timestamp": milli_time()
            }
            # add additional optional information
            if information:
                callback_payload["data"][0]["information"] = information
            logging.debug(callback_payload)
            self.rest_client.post(callback_url, json=callback_payload)
