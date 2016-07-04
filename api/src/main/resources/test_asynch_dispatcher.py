"""
Purpose:    Unit tests for the the asynchronous dispatcher
            Run with main(), the easiest way is "nosetests test_*.py"
Author:     PNDA team


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
from threading import Event
from async_dispatcher import AsyncDispatcher


class GenerateRecord(unittest.TestCase):
    def test(self):
        asynch_dispatcher = AsyncDispatcher()
        asynch_result = [None]
        test_exception_message = "testing exception handling"
        wait_for_exception = Event()

        def test_work():
            print "sending " + " hello"
            return "hello"

        def handler(param):
            print "got asynch result: " + param
            asynch_result[0] = param

        def raise_exception():
            raise Exception(test_exception_message)

        def handle_exception(ex):
            asynch_result[0] = ex
            wait_for_exception.set()

        task = asynch_dispatcher.run_as_asynch(test_work, handler)
        self.assertEqual(task.get_result(), "hello")
        self.assertEqual(asynch_result[0], "hello")
        task = asynch_dispatcher.run_as_asynch(raise_exception, on_error=handle_exception)
        try:
            task.get_result()
            self.fail("should not reach this line")
        except Exception as ex:
            # check that the test exception has been thrown
            self.assertTrue(test_exception_message in ex.message)
        # wait for exception handler to fire
        wait_for_exception.wait(timeout=5)
        self.assertIsInstance(asynch_result[0], Exception)
        self.assertEquals(asynch_result[0].message, test_exception_message)
