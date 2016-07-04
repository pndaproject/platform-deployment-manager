"""
Name:       Test cases for package repository client
Purpose:    Unit testing

Author:     PNDA team

Created:    30/03/2016

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
import mock
from mock import patch, mock_open, Mock

import repository
from exceptiondef import NotFound


class TestSwiftRepo(unittest.TestCase):

    swift_response = [
        {
            "content-length": "25011",
            "content-type": "application/json; charset=utf-8"
        }, [{
            "bytes": 9235,
            "last_modified": "2016-03-23T14:36:20.000Z",
            "hash": "2a43a0629a4df5033d70176b4e71a216",
            "name": "releases/spark-batch-example-app-1.0.25.tar.gz"
        }, {
            "bytes": 9247,
            "last_modified": "2016-03-23T14:44:06.000Z",
            "hash": "b54264c03eb088361f370bf0b84c18fe",
            "name": "releases/spark-batch-example-app-1.0.26.tar.gz"
        }, {
            "bytes": 9251,
            "last_modified": "2016-03-25T13:13:56.000Z",
            "hash": "cc3c56727e9b81943a6ee345040a2782",
            "name": "releases/spark-batch-example-app-c-1.0.30.tar.gz"
        }]
    ]

    @mock.patch('swiftclient.client.Connection')
    def test_list_packages(self, swift_mock):
        swift_mock.return_value.get_container.return_value = self.swift_response
        expected_packages_1 = [{
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

        mock_repo_client = Mock()
        mock_repo_client.get_package_list = Mock(return_value=expected_packages_1)
        repo = repository.SwiftRepository(mock_repo_client)
        packages = repo.list_packages(1)
        self.assertEqual(packages, expected_packages_1)

        expected_packages_2 = [{
            'latest_versions': [{
                'version': '1.0.26',
                'file': 'spark-batch-example-app-1.0.26.tar.gz'
            }, {
                'version': '1.0.25',
                'file': 'spark-batch-example-app-1.0.25.tar.gz'
            }],
            'name': 'spark-batch-example-app'
        }, {
            'latest_versions': [{
                'version': '1.0.30',
                'file': 'spark-batch-example-app-c-1.0.30.tar.gz'
            }],
            'name': 'spark-batch-example-app-c'
        }]

        mock_repo_client.get_package_list = Mock(return_value=expected_packages_2)
        packages = repo.list_packages(2)
        self.assertEqual(packages, expected_packages_2)

    @mock.patch('swiftclient.client.Connection')
    def test_download_package(self, swift_mock):
        swift_mock.return_value.get_container.return_value = self.swift_response
        swift_mock.return_value.get_object.return_value = ["", "abcd"]
        mock_repo_client = Mock()
        mock_repo_client.get_package = Mock(return_value="abcd")
        repo = repository.SwiftRepository(mock_repo_client)
        package_data = repo.download_package("spark-batch-example-app-c-1.0.30")
        self.assertEqual(package_data, "abcd")

    def test_download_package_error(self):
        mock_repo_client = Mock()
        repo = repository.SwiftRepository(mock_repo_client)

        def throw():
            raise NotFound("test Error")

        mock_repo_client.get_package = lambda _: throw()
        self.assertRaises(NotFound, repo.download_package, "spark-batch-example-app-c-1.0.31")


class TestFsRepo(unittest.TestCase):
    location = {
        "path": "/tmp/packages/"
    }

    fs_list = [
        'spark-batch-example-app-1.0.25.tar.gz',
        'spark-batch-example-app-1.0.26.tar.gz',
        'spark-batch-example-app-c-1.0.30.tar.gz']

    @mock.patch('os.listdir')
    def test_list_packages(self, fs_mock):
        fs_mock.return_value = self.fs_list

        expected_packages_1 = [{
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

        repo = repository.FsRepository(self.location)
        packages = repo.list_packages(1)
        self.assertEqual(packages, expected_packages_1)

        expected_packages_2 = [{
            'latest_versions': [{
                'version': '1.0.26',
                'file': 'spark-batch-example-app-1.0.26.tar.gz'
            }, {
                'version': '1.0.25',
                'file': 'spark-batch-example-app-1.0.25.tar.gz'
            }],
            'name': 'spark-batch-example-app'
        }, {
            'latest_versions': [{
                'version': '1.0.30',
                'file': 'spark-batch-example-app-c-1.0.30.tar.gz'
            }],
            'name': 'spark-batch-example-app-c'
        }]

        packages = repo.list_packages(2)
        self.assertEqual(packages, expected_packages_2)

    @mock.patch('os.listdir')
    def test_is_available(self, fs_mock):
        fs_mock.return_value = self.fs_list

        repo = repository.FsRepository(self.location)
        available = repo.is_available("spark-batch-example-app-c-1.0.30")
        self.assertTrue(available)
        available = repo.is_available("spark-batch-example-app-c-1.0.31")
        self.assertFalse(available)

    def test_download_package(self):
        my_text = 'abcd'
        mocked_open_function = mock_open(read_data=my_text)

        with patch("__builtin__.open", mocked_open_function):
            repo = repository.FsRepository(self.location)
            package_data = repo.download_package("spark-batch-example-app-c-1.0.30")
            self.assertEqual(package_data, "abcd")
