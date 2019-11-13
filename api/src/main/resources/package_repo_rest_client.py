"""
Purpose:    A native python rest client package repository api
Author:     PNDA team

Created:    4/04/2016

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
import json
import logging
import re
import requests
from requests.exceptions import RequestException
from exceptiondef import FailedConnection


class PackageRepoRestClient(object):
    def __init__(self, api_url, package_local_dir_path):
        """
        A client implementation for the package repository API
        :param api_url: A url describing the location to make REST calls to
        """
        self.api_url = api_url
        self._package_local_dir_path = package_local_dir_path

    def put_package(self, package_name, package_data):
        """
        Adds a package to the  repository
        :param package_name: The name of the package to add
        :param package_data: The actual binary data of the package
        """
        url = self.api_url + "/packages/" + package_name
        logging.debug("PUT: %s", url)
        response = requests.put(url, data=package_data)
        logging.debug("response code: %s", str(response.status_code))
        assert response.status_code == 200

    def get_package(self, package_name, user_name, expected_codes=None):
        """
        gets a package from the repository
        :param package_nam:
        :return: local path to file
        """
        if not expected_codes:
            expected_codes = [200]
        response = self.make_rest_get_request("/packages/%s?user.name=%s" % (package_name, user_name), expected_codes)
        local_path = "%s/%s" % (self._package_local_dir_path, package_name)
        with open(local_path, 'wb') as local_file:
            local_file.write(response.content)
        return local_path

    def get_package_list(self, user_name, recency=None):
        """
        :return: a list of all packages in the repository
        """
        url = "/packages?user.name=%s" % user_name
        if recency:
            url = url + "&recency=" + str(recency)
        response = self.make_rest_get_request(url)
        return json.loads(response.content)

    @staticmethod
    def parse_error_msg_from_response(html_str):
        title_tag = re.search('<title>(.+?)<.*/title>', html_str)
        if title_tag:
            cause_msg = re.sub(r'<[A-Za-z\/][^>]*>', '', title_tag.group())
            return cause_msg
        return html_str

    def make_rest_get_request(self, path, expected_codes=None):
        if not expected_codes:
            expected_codes = [200]
        url = self.api_url + path
        logging.debug("GET: %s", url)

        try:
            response = requests.get(url, timeout=120)
        except RequestException as exc:
            logging.debug("Request error: %s", str(exc))
            error_msg = 'Unable to connect to the Package Repository Manager'
            raise FailedConnection(error_msg)

        logging.debug("response code: %s", str(response.status_code))

        if response.status_code not in expected_codes:
            error_msg = PackageRepoRestClient.parse_error_msg_from_response(response.text)
            error_msg = "Package Repository Manager - {} (request path = {})".format(error_msg, path)
            logging.debug("Server error: %s", str(error_msg))
        assert response.status_code in expected_codes, error_msg

        return response
