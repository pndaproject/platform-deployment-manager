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
import os
import time
import datetime
import threading
import traceback
import grp
import pwd
import requests

import application_creator
import authorizer_local
from exceptiondef import ConflictingState, NotFound, Forbidden
from package_parser import PackageParser
from async_dispatcher import AsyncDispatcher
from lifecycle_states import ApplicationState, PackageDeploymentState


def milli_time():
    return int(round(time.time() * 1000))

class Resources(object):
    ENVIRONMENT = "deployment_manager:environment"
    PACKAGE = "deployment_manager:package"
    PACKAGES = "deployment_manager:packages"
    APPLICATION = "deployment_manager:application"
    APPLICATIONS = "deployment_manager:applications"
    REPOSITORY = "deployment_manager:repository"


class Actions(object):
    DEPLOY = "deploy"
    UNDEPLOY = "undeploy"
    CREATE = "create"
    START = "start"
    STOP = "stop"
    DESTROY = "destroy"
    READ = "read"

class DeploymentManager(object):
    def __init__(self, repository, package_registrar, application_registrar, application_summary_registrar, environment, config):
        self._repository = repository
        self._package_registrar = package_registrar
        self._application_registrar = application_registrar
        self._environment = environment
        self._config = config
        self._application_creator = application_creator.ApplicationCreator(config, environment,
                                                                           environment['namespace'])
        self._application_summary_registrar = application_summary_registrar
        self._package_parser = PackageParser()
        self._package_progress = {}
        self._lock = threading.RLock()
        self._authorizer = authorizer_local.AuthorizerLocal()

        # load number of threads from config file:
        number_of_threads = self._config["deployer_thread_limit"]
        assert isinstance(number_of_threads, (int))
        assert number_of_threads > 0
        self.dispatcher = AsyncDispatcher(num_threads=number_of_threads)
        self.rest_client = requests

    def _get_groups(self, user):
        groups = []
        if user:
            try:
                groups = [g.gr_name for g in grp.getgrall() if user in g.gr_mem]
                gid = pwd.getpwnam(user).pw_gid
                groups.append(grp.getgrgid(gid).gr_name)
            except:
                raise Forbidden('Failed to find details for user "%s"' % user)
        return groups

    def _authorize(self, user_name, resource_type, resource_owner, action_name):
        qualified_action = '%s:%s' % (resource_type, action_name)
        identity = {'user': user_name, 'groups': self._get_groups(user_name)}
        resource = {'type': resource_type, 'owner': resource_owner}
        action = {'name': qualified_action}
        if not self._authorizer.authorize(identity, resource, action):
            raise Forbidden('User "%s" does not have authorization for "%s"' % (user_name, qualified_action))

    def get_environment(self, user_name):
        self._authorize(user_name, Resources.ENVIRONMENT, None, Actions.READ)
        return self._environment

    def list_packages(self, user_name):
        self._authorize(user_name, Resources.PACKAGES, None, Actions.READ)
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

    def list_repository(self, recency, user_name):
        self._authorize(user_name, Resources.REPOSITORY, None, Actions.READ)
        logging.info("list_available: %s", recency)
        available = self._repository.get_package_list(user_name, recency)
        return available

    def _get_saved_package_data(self, package):
        package_owner = None
        package_exists = False
        package_metadata = None
        if self._package_registrar.package_exists(package):
            package_metadata = self._package_registrar.get_package_metadata(package)
            logging.debug(package_metadata)
            package_owner = package_metadata['metadata']['user']
            package_exists = True
        return package_owner, package_exists, package_metadata

    def _get_package_owner(self, package):
        package_owner, _, _ = self._get_saved_package_data(package)
        return package_owner

    def _get_application_owner(self, application):
        application_owner = None
        if self._application_registrar.application_has_record(application):
            application_owner = self._application_registrar.get_application(application)['overrides']['user']
        return application_owner

    def get_package_info(self, package, user_name=None):
        package_owner, package_exists, metadata = self._get_saved_package_data(package)
        if user_name is not None:
            self._authorize(user_name, Resources.PACKAGES, package_owner, Actions.READ)
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
            if package_exists:
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
               "user": package_owner,
               "defaults": properties,
               "information": information}

        return ret

    def _run_asynch_package_task(self, package_name, initial_state, working_state, task, auth_check):
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
            auth_check()
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

    def deploy_package(self, package, user_name):
        def auth_check():
            self._authorize(user_name, Resources.PACKAGE, None, Actions.DEPLOY)

        # this function will be executed in the background:
        def _do_deploy():
            # if this value is not changed, then it is assumed that the operation never completed
            package_data_path = None
            try:
                package_file = package + '.tar.gz'
                logging.info("deploy: %s", package)
                # download package:
                package_data_path = self._repository.get_package(package_file, user_name)
                # put package in database:
                metadata = self._package_parser.get_package_metadata(package_data_path)
                self._application_creator.validate_package(package, metadata)
                self._package_registrar.set_package(package, package_data_path, user_name)
                # set the operation status as complete
                deploy_status = {"state": PackageDeploymentState.DEPLOYED,
                                 "information": "Deployed " + package + " at " + self.utc_string()}
                logging.info("deployed: %s", package)
            except Exception as ex:
                logging.error(str(ex))
                error_message = "Error deploying " + package + " " + str(type(ex).__name__) + ", details: " + json.dumps(str(ex))
                deploy_status = {"state": PackageDeploymentState.NOTDEPLOYED, "information": error_message}
                raise
            finally:
                # report final state of operation to database:
                self._package_registrar.set_package_deploy_status(package, deploy_status)
                if package_data_path is not None:
                    os.remove(package_data_path)

        # schedule work to be done in the background:
        self._run_asynch_package_task(package_name=package,
                                      initial_state=PackageDeploymentState.NOTDEPLOYED,
                                      working_state=PackageDeploymentState.DEPLOYING,
                                      task=_do_deploy,
                                      auth_check=auth_check)

    def utc_string(self):
        return datetime.datetime.utcnow().isoformat()

    def undeploy_package(self, package, user_name):
        def auth_check():
            package_owner = self._get_package_owner(package)
            self._authorize(user_name, Resources.PACKAGE, package_owner, Actions.UNDEPLOY)

        # this function will be executed in the background:
        def do_undeploy():
            deploy_status = None
            try:
                logging.info("undeploy: %s", package)
                self._package_registrar.delete_package(package)
                logging.info("undeployed: %s", package)
            except Exception as ex:
                # log error to screen:
                logging.error(str(ex))
                # prepare human readable message
                error_message = "Error undeploying " + package + " " + str(type(ex).__name__) + ", details: " + json.dumps(str(ex))
                # set the status:
                deploy_status = {"state": PackageDeploymentState.DEPLOYED, "information": error_message}
                raise
            finally:
                if deploy_status is not None:
                    # persist any errors in the database, but still throw them:
                    self._package_registrar.set_package_deploy_status(package, deploy_status)

        # schedule work to be done in the background:
        self._run_asynch_package_task(package_name=package,
                                      initial_state=PackageDeploymentState.DEPLOYED,
                                      working_state=PackageDeploymentState.UNDEPLOYING,
                                      task=do_undeploy,
                                      auth_check=auth_check)

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

    def list_package_applications(self, package, user_name):
        self._authorize(user_name, Resources.APPLICATIONS, None, Actions.READ)
        logging.info('list_package_applications')
        applications = self._application_registrar.list_applications_for_package(package)
        return applications

    def list_applications(self, user_name):
        self._authorize(user_name, Resources.APPLICATIONS, None, Actions.READ)
        logging.info('list_applications')
        applications = self._application_registrar.list_applications()
        return applications

    def _assert_application_status(self, application, required_status):
        logging.debug("Checking %s is %s", application, json.dumps(required_status))
        app_info = self.get_application_info(application)
        status = app_info['status']
        logging.debug("Found %s is %s", application, status)

        if (isinstance(required_status, list) and status not in required_status) \
                or (not isinstance(required_status, list) and status != required_status):
            if status == ApplicationState.NOTCREATED:
                raise NotFound(json.dumps({'status': status}))
            else:
                raise ConflictingState(json.dumps({'status': status}))

        logging.debug("Status for %s is OK", application)

    def _assert_application_exists(self, application):
        status = self.get_application_info(application)['status']
        if status == ApplicationState.NOTCREATED:
            raise NotFound(json.dumps({'status': status}))

    def start_application(self, application, user_name):
        logging.info('start_application')
        with self._lock:
            self._assert_application_status(application, ApplicationState.CREATED)
            application_owner = self._get_application_owner(application)
            self._authorize(user_name, Resources.APPLICATION, application_owner, Actions.START)
            self._mark_starting(application)

        def do_work_start():
            try:
                self._state_change_event_application(application)
                try:
                    create_data = self._application_registrar.get_create_data(application)
                    self._application_creator.start_application(application, create_data)
                    self._application_registrar.set_application_status(application, ApplicationState.STARTED)
                except Exception as ex:
                    self._handle_application_error(application, ex, ApplicationState.CREATED, "starting")
                    raise
            finally:
                self._clear_package_progress(application)
                self._state_change_event_application(application)

        self.dispatcher.run_as_asynch(task=do_work_start)

    def stop_application(self, application, user_name):
        logging.info('stop_application')
        with self._lock:
            self._assert_application_status(application, ApplicationState.STARTED)
            application_owner = self._get_application_owner(application)
            self._authorize(user_name, Resources.APPLICATION, application_owner, Actions.STOP)
            self._mark_stopping(application)

        def do_work_stop():
            try:
                self._state_change_event_application(application)
                try:
                    create_data = self._application_registrar.get_create_data(application)
                    self._application_creator.stop_application(application, create_data)
                    self._application_registrar.set_application_status(application, ApplicationState.CREATED)
                except Exception as ex:
                    self._handle_application_error(application, ex, ApplicationState.STARTED, "stopping")
                    raise
            finally:
                self._clear_package_progress(application)
                self._state_change_event_application(application)

        self.dispatcher.run_as_asynch(task=do_work_stop)

    def get_application_info(self, application, user_name=None):
        if user_name is not None:
            application_owner = self._get_application_owner(application)
            self._authorize(user_name, Resources.APPLICATION, application_owner, Actions.READ)

        logging.info('get_application_info')

        if not self._application_registrar.application_has_record(application):
            record = {'status': ApplicationState.NOTCREATED, 'information': None}
        else:
            record = self._application_registrar.get_application(application)
        progress_state = self._get_package_progress(application)
        if progress_state is not None:
            record['status'] = progress_state

        return record

    def get_application_detail(self, application, user_name):
        application_owner = self._get_application_owner(application)
        self._authorize(user_name, Resources.APPLICATION, application_owner, Actions.READ)

        logging.info('get_application_detail')
        self._assert_application_exists(application)
        create_data = self._application_registrar.get_create_data(application)
        record = self._application_creator.get_application_runtime_details(application, create_data)
        record['status'] = self.get_application_info(application)['status']
        record['name'] = application
        return record

    def get_application_summary(self, application, user_name):
        application_owner = self._get_application_owner(application)
        self._authorize(user_name, Resources.APPLICATION, application_owner, Actions.READ)

        logging.info('get_application_summary')
        record = self._application_summary_registrar.get_summary_data(application)
        return record

    def create_application(self, package, application, overrides, user_name):
        logging.info('create_application')
        package_data_path = None

        with self._lock:
            self._assert_application_status(application, ApplicationState.NOTCREATED)
            self._assert_package_status(package, PackageDeploymentState.DEPLOYED)
            package_owner = self._get_application_owner(package)
            self._authorize(user_name, Resources.PACKAGE, package_owner, Actions.READ)
            self._authorize(user_name, Resources.APPLICATION, None, Actions.CREATE)
            defaults = self.get_package_info(package)['defaults']
            self._application_creator.assert_application_properties(overrides, defaults)
            package_data_path = self._package_registrar.get_package_data(package)
            self._application_registrar.create_application(package, application, overrides, defaults)
            self._mark_creating(application)

        def do_work_create():
            try:
                self._state_change_event_application(application)
                try:
                    package_metadata = self._package_registrar.get_package_metadata(package)['metadata']
                    create_data = self._application_creator.create_application(
                        package_data_path, package_metadata, application, overrides)
                    self._application_registrar.set_create_data(application, create_data)
                    self._application_registrar.set_application_status(application, ApplicationState.CREATED)
                except Exception as ex:
                    self._handle_application_error(application, ex, ApplicationState.NOTCREATED, "creating")
                    logging.error(traceback.format_exc(ex))
                    raise
            finally:
                # clear inner locks:
                self._clear_package_progress(application)
                self._state_change_event_application(application)
                if package_data_path is not None:
                    os.remove(package_data_path)

        self.dispatcher.run_as_asynch(task=do_work_create)

    def _handle_application_error(self, application, ex, app_status, operation):
        """
        Use to handle application exceptions which should be relayed back to the user
        Sets the application state to an error
        :param application: The app for which to set the error
        :param ex: The error
        :param app_status: The status the app should be at following the error.
        """
        # log error to screen:
        logging.error(str(ex))
        # prepare human readable message
        error_message = "Error %s " % operation + application + " " + str(type(ex).__name__) + ", details: " + json.dumps(str(ex))
        # set the status:
        self._application_registrar.set_application_status(application, app_status, error_message)

    def delete_application(self, application, user_name):
        logging.info('delete_application')
        with self._lock:
            self._assert_application_status(application, [ApplicationState.CREATED, ApplicationState.STARTED])
            application_owner = self._get_application_owner(application)
            self._authorize(user_name, Resources.APPLICATION, application_owner, Actions.DESTROY)
            self._mark_destroying(application)

        def do_work_delete():
            try:
                self._state_change_event_application(application)
                try:
                    create_data = self._application_registrar.get_create_data(application)
                    self._application_creator.destroy_application(application, create_data)
                    self._application_registrar.delete_application(application)
                except Exception as ex:
                    self._handle_application_error(application, ex, ApplicationState.STARTED, "deleting")
                    raise
            finally:
                self._clear_package_progress(application)
                self._state_change_event_application(application)

        self.dispatcher.run_as_asynch(task=do_work_delete)

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
