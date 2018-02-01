"""
Name:       app.py
Purpose:    Runs the webserver for the Deployment Manager REST API.
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
import sys
import json
import logging
from tornado_cors import CorsMixin

import tornado.httpserver
import tornado.options
import tornado.web
from tornado.ioloop import IOLoop
from tornado.web import asynchronous
from tornado.options import define, options

import package_registrar
import application_registrar
import deployer_utils
import application_summary_registrar
import deployment_manager
from deployer_system_test import DeployerRestClientTester
from exceptiondef import NotFound, ConflictingState, FailedValidation, FailedCreation
from async_dispatcher import AsyncDispatcher
from package_repo_rest_client import PackageRepoRestClient

options.logging = None


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r'/repository/packages', RepositoryHandler),
            (r'/packages', PackagesHandler),
            (r'/packages/(.*)/applications', PackageApplicationsHandler),
            (r'/packages/(.*)/status', PackageStatusHandler),
            (r'/packages/(.*)', PackageHandler),
            (r'/applications/(.*)/(.*)', ApplicationDetailHandler),
            (r'/applications/(.*)', ApplicationHandler),
            (r'/applications', ApplicationsHandler),
            (r'/environment/endpoints', EnvironmentHandler),
            (r'/selftest/all', SelfTestHandler)
        ]
        tornado.web.Application.__init__(self, handlers)


class BaseHandler(CorsMixin, tornado.web.RequestHandler):
    CORS_ORIGIN = '*'

    def handle_error(self, ex):
        def finish():
            if isinstance(ex, NotFound):
                logging.info(ex.msg)
                self.set_status(404)
                self.finish(ex.msg)
            elif isinstance(ex, ConflictingState):
                logging.info(ex.msg)
                self.set_status(409)
                self.finish(ex.msg)
            elif isinstance(ex, FailedValidation):
                logging.info(ex.msg)
                self.set_status(400)
                self.finish(ex.msg)
            elif isinstance(ex, FailedCreation):
                logging.info(ex.msg)
                self.set_status(500)
                self.finish(ex.msg)
            else:
                self.set_status(500)
                if "information" in str(ex):
                    msg = str(ex)
                else:
                    msg = {"status": "UNKNOWN", "information": str(ex)}
                self.finish(msg)

        IOLoop.instance().add_callback(callback=finish)

    def send_result(self, ret_val):
        def finish():
            self.finish(json.dumps(ret_val))

        IOLoop.instance().add_callback(callback=finish)

    def send_accepted(self):
        def finish():
            self.set_status(202)
            self.finish()

        IOLoop.instance().add_callback(callback=finish)

    def send_client_error(self, msg):
        def finish():
            self.set_status(400)
            self.finish(msg)

        IOLoop.instance().add_callback(callback=finish)


DISPATCHER = AsyncDispatcher(num_threads=10)


class SelfTestHandler(BaseHandler):
    @asynchronous
    def get(self):
        def do_call():
            self.send_result(DeployerRestClientTester().run_tests())

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)


class EnvironmentHandler(BaseHandler):
    @asynchronous
    def get(self):
        def do_call():
            self.send_result(dm.get_environment())

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)


class RepositoryHandler(BaseHandler):
    @asynchronous
    def get(self):
        args = self.request.arguments

        def do_call():
            recency = 1
            if 'recency' in args:
                recency = int(args['recency'][0])
            self.send_result(dm.list_repository(recency))

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)


class PackagesHandler(BaseHandler):
    @asynchronous
    def get(self):
        def do_call():
            self.send_result(dm.list_packages())

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)


class PackageHandler(BaseHandler):
    @asynchronous
    def get(self, name):
        def do_call():
            self.send_result(dm.get_package_info(name))

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)

    @asynchronous
    def put(self, name):
        def do_call():
            dm.deploy_package(name)
            self.send_accepted()

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)

    @asynchronous
    def delete(self, name):
        def do_call():
            dm.undeploy_package(name)
            self.send_accepted()

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)


class PackageApplicationsHandler(BaseHandler):
    @asynchronous
    def get(self, name):
        def do_call():
            self.send_result(dm.list_package_applications(name))

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)


class PackageStatusHandler(BaseHandler):
    @asynchronous
    def get(self, name):
        def do_call():
            package_info = dm.get_package_info(name)
            self.send_result({
                "status": package_info.get("status"),
                "information": package_info.get("information", None)
            })

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)


class ApplicationsHandler(BaseHandler):
    @asynchronous
    def get(self):
        def do_call():
            self.send_result(dm.list_applications())

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)


class ApplicationDetailHandler(BaseHandler):
    @asynchronous
    def post(self, name, action):
        def do_call():
            if action == 'start':
                dm.start_application(name)
                self.send_accepted()
            elif action == 'stop':
                dm.stop_application(name)
                self.send_accepted()
            else:
                self.send_client_error("%s is not a valid action (start|stop)" % action)

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)

    @asynchronous
    def get(self, name, action):
        def do_call():
            if action == 'status':
                app_info = dm.get_application_info(name)
                ret = {
                    "status": app_info["status"],
                    "information": app_info.get("information", None)
                }
                self.send_result(ret)
            elif action == 'detail':
                self.send_result(dm.get_application_detail(name))
            elif action == 'summary':
                self.send_result(dm.get_application_summary(name))
            else:
                self.send_client_error("%s is not a valid query (status|detail|summary)" % action)

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)

class ApplicationHandler(BaseHandler):
    @asynchronous
    def put(self, aname):
        try:
            request_body = json.loads(self.request.body)
            pname = request_body['package']
        except:
            self.send_client_error("Invalid request body")
            return

        def do_call():
            dm.create_application(pname, aname, request_body)
            self.send_accepted()

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)

    @asynchronous
    def get(self, name):
        def do_call():
            self.send_result(dm.get_application_info(name))

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)

    @asynchronous
    def delete(self, name):
        def do_call():
            dm.delete_application(name)
            self.send_accepted()

        DISPATCHER.run_as_asynch(task=do_call, on_error=self.handle_error)


# pylint: disable=C0103
# pylint: disable=W0603
config = None
dm = None


def main():
    global config
    global dm

    with open('dm-config.json', 'r') as f:
        config = json.load(f)

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.getLevelName(config['config']['log_level']),
                        stream=sys.stderr)

    define("port", default=5000, help="run on the given port", type=int)
    tornado.options.parse_command_line()

    logging.info("Starting up...")

    deployer_utils.fill_hadoop_env(config['environment'], config['config'])

    package_repository = PackageRepoRestClient(config['config']["package_repository"], config['config']['stage_root'])
    dm = deployment_manager.DeploymentManager(package_repository,
                                              package_registrar.HbasePackageRegistrar(
                                                  config['environment']['hbase_thrift_server'],
                                                  config['environment']['webhdfs_host'],
                                                  'hdfs',
                                                  config['environment']['webhdfs_port'],
                                                  config['config']['stage_root']),
                                              application_registrar.HbaseApplicationRegistrar(
                                                  config['environment']['hbase_thrift_server']),
                                              application_summary_registrar.HBaseAppplicationSummary(
                                                  config['environment']['hbase_thrift_server']),
                                              config['environment'],
                                              config['config'])

    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)

    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
