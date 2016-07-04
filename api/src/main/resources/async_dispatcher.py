"""
Name:       asynch_dispatcher
Purpose:    A module responsible for running blocking tasks as asynchronous tasks
            For now, it simply wraps a Threadpool,
            Later this might be switched a more advanced library.

Author:     PNDA team

Created:    22/03/2016

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
import traceback
import logging
from multiprocessing.dummy import Pool as ThreadPool


class AsyncDispatcher(object):
    """
    Runs blockiong calls as synchronous tasks
    """

    def __init__(self, num_threads=50):
        self.pool = ThreadPool(processes=num_threads)

    def run_as_asynch(self, task, on_success=None, on_error=None, on_complete=None):
        """
        Transforms a blocking call into an asynchronous task
        :param task: a function to run
        :param on_complete: a function to call when the task has finished running.
            Said function should accept the return value of the synchrouns task.
        :return:
        """

        def task_wrapper():
            """
            encapsulates tasks to catch their errors,
            as threadpool does not contain asynch error reporting by default
            """
            try:
                return task()
            except Exception as ex:
                logging.error(traceback.format_exc())
                # report asynchronous exception:
                if on_error:
                    # call callback on result thread (this is the worker thread)
                    self.pool.apply_async(on_error, args=(ex,))
                # call the completion handler:
                if on_complete:
                    # call callback on result thread (this is the worker thread)
                    self.pool.apply_async(on_complete)
                # re-throw execution exeption to get function
                raise Exception(ex)

        def success_wrapper(result):
            """
            called asynchronously when the task has finished running successfully
            """
            # This handler is called on the result thread,
            # so there is no need to reschedule the callback
            #
            # report success:
            if on_success:
                on_success(result)
            # report that task is completed
            if on_complete:
                on_complete()

        # run the task on a different thread:
        result = self.pool.apply_async(task_wrapper, callback=success_wrapper)
        return ScheduledTask(result)


class ScheduledTask(object):
    """
    An interface to control a running task
    This attempts to decouple the task from any particular execution framework
    """

    def __init__(self, task):
        self.task = task

    def get_result(self):
        """
        Blocks until result is avaiable
        :return: the value returned by the worker task
        """
        return self.task.get()
