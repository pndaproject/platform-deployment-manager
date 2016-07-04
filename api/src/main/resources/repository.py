"""
Name:       repository.py
Purpose:    Retrieves packages and lists of available packages from the repository
            Implementations include
             - SwiftRepository: packges are stores in OpenStack Swift
             - FsRepository: Packages are stores in the local file system where
                             the deployment manager is running
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

import re
import os
import logging
from distutils.version import StrictVersion

class SwiftRepository(object):
    """
    All functionality for the swift repository has been moved to another service
    """

    def __init__(self, repo_rest_client):
        """
        :param repo_rest_client: A configured instance of the  repository rest client
        """
        self._repo_rest_client = repo_rest_client

    def list_packages(self, recency):
        return self._repo_rest_client.get_package_list(recency)

    def download_package(self, package):
        return self._repo_rest_client.get_package(package)


class FsRepository(object):
    def __init__(self, location):
        self._location = location

    def list_packages(self, recency):

        logging.debug("list_packages %s", recency)

        path = self._location['path']
        logging.debug(path)

        candidates = {}
        for item in os.listdir(path):
            item = "%s%s" % (path, item)
            logging.debug(item)
            grps = re.match(r'.*/((.*)-(\d+.\d+.\d+).*)', item)
            if grps is not None:
                groups = grps.groups()
                if len(groups) == 3:
                    fullname, shortname, version = groups
                entry = {'version': version, 'file': fullname}
                try:
                    candidates[shortname].append(entry)
                except KeyError:
                    candidates[shortname] = [entry]

        packages_list = []
        for can in candidates:
            last = sorted(
                candidates[can],
                key=lambda x: StrictVersion(
                    x['version']),
                reverse=True)[:recency]
            packages_list.append({'name': can, 'latest_versions': last})

        return packages_list

    def is_available(self, package):
        logging.debug("is_available %s", package)
        path = self._location['path']
        found = False
        for item in os.listdir(path):
            if '/' + package + '.' in "%s%s" % (path, item):
                found = True
                break
        logging.debug(found)
        return found

    def download_package(self, package):
        logging.debug("download_package %s", package)
        with open("%s.tar.gz" % package, "rb") as in_file:
            package_data = in_file.read()
        return package_data
