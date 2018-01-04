import json
import unittest
from mock import patch
import application_summary
from lifecycle_states import ApplicationState
COMPONENT_STATUS = dict([("green", "OK"), ("amber", "WARN"), ("red", "ERROR")])
ERROR_STATUS = dict([("OK", "WITH_NO_ERRORS"), ("ERROR", "WITH_ERRORS"), ("WARN", "WITH_ERRORS")])
FAILURE_STATUS = dict([("OK", "WITH_NO_FAILURES"), ("ERROR", "WITH_FAILURES"), \
("WARN", "WITH_FAILURES")])

class AppplicationSummaryTests(unittest.TestCase):
    def test_process_application_data(self):
        """
        Check building aggregate status works on application level from component level
        """
        result = application_summary.process_application_data({'component-1': \
        {'aggregate_status': 'STARTED_RUNNING_WITH_NO_ERRORS', 'yarnId': '1234'}})
        self.assertEqual(result, 'STARTED_RUNNING_WITH_NO_ERRORS')

    @patch('application_summary.check_in_yarn')
    @patch('application_summary.yarn_info')
    @patch('application_summary.spark_job_handler')
    def test_spark_application(self, spark_job_patch, yarn_job_patch, yarn_check_patch):
        """
        Tetsing Spark application
        """
        #Spark application in case of Killed
        yarn_check_patch.return_value = {'id': 'application_1234'}
        yarn_job_patch.return_value = {'yarnFinalStatus': 'KILLED'}
        result = application_summary.spark_application('app1-example-job')
        self.assertEqual(result['aggregate_status'], '%s_%s_%s' % (ApplicationState.COMPLETED, \
        'KILLED', FAILURE_STATUS['OK']))

        #Spark application in case of Failed
        yarn_check_patch.return_value = {'id': 'application_1234'}
        yarn_job_patch.return_value = {'yarnFinalStatus': 'FAILED'}
        result = application_summary.spark_application('app1-example-job')
        self.assertEqual(result['aggregate_status'], '%s_%s_%s' % (ApplicationState.COMPLETED, \
        'FAILED', FAILURE_STATUS['ERROR']))

        #Spark application in case of Running with No errors
        yarn_check_patch.return_value = {'id': 'application_1234'}
        yarn_job_patch.return_value = {'yarnStatus': 'RUNNING', \
        'yarnFinalStatus': 'UNDEFINED'}
        spark_job_patch.return_value = {'state': 'OK', 'information': 'job_stage data'}
        result = application_summary.spark_application('app1-example-job')
        self.assertEqual(result['aggregate_status'], '%s_%s_%s' % (ApplicationState.STARTED, \
        'RUNNING', ERROR_STATUS['OK']))

        #Spark application in case of Running with errors
        yarn_check_patch.return_value = {'id': 'application_1234'}
        yarn_job_patch.return_value = {'yarnStatus': 'RUNNING', \
        'yarnFinalStatus': 'UNDEFINED'}
        spark_job_patch.return_value = {'state': 'ERROR', 'information': 'job_stage data'}
        result = application_summary.spark_application('app1-example-job')
        self.assertEqual(result['aggregate_status'], '%s_%s_%s' % (ApplicationState.STARTED, \
        'RUNNING', ERROR_STATUS['ERROR']))

        #Spark application in other states than above states
        yarn_check_patch.return_value = {'id': 'application_1234'}
        yarn_job_patch.return_value = {'yarnStatus': 'ACCEPTED', \
        'yarnFinalStatus': 'UNDEFINED'}
        result = application_summary.spark_application('app1-example-job')
        self.assertEqual(result['aggregate_status'], '%s_%s_%s' % (ApplicationState.STARTED, \
        'ACCEPTED', ERROR_STATUS['OK']))

    @patch('requests.get')
    def test_check_in_yarn(self, mock_req):
        """
        Check recent application data provided or not based on started time
        """
        mock_req.return_value = type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
            "apps": {
                "app": [
                    {
                        "name": "app1-example-job",
                        "id": "application_1234",
                        "startedTime": 1512647193214
                    },
                    {
                        "name": "app1-example-job",
                        "id": "application_1235",
                        "startedTime": 1512647212310
                    },
                    {
                        "name": "app1-example-job",
                        "id": "application_1236",
                        "startedTime": 1512647185041
                    }
                ]
            }
        })})
        result = application_summary.check_in_yarn('app1-example-job')
        self.assertEqual(result['id'], 'application_1235')

    @patch('application_summary.process_component_data')
    @patch('application_summary.get_oozie_workflow_actions')
    def test_oozie_coordinator_handler(self, oozie_worfklow_patch, process_component_data_patch):
        """
        Testing Oozie Coordinator
        """
        #Oozie Coordinator in case of Prepsuspended
        result = application_summary.oozie_coordinator_handler({
            'actions': [],
            'coordJobId': '0123-oozie-oozi-C',
            'coordJobName': 'o1-coordinator',
            'status': 'PREPSUSPENDED'
        })
        self.assertEqual(result['aggregate_status'], ApplicationState.CREATED)

        #Oozie Coordinator in case of Prep
        result = application_summary.oozie_coordinator_handler({
            'actions': [],
            'coordJobId': '0123-oozie-oozi-C',
            'coordJobName': 'o1-coordinator',
            'status': 'PREP'
        })
        self.assertEqual(result['aggregate_status'], ApplicationState.STARTING)

        #Oozie Coordinator in case of Running and one workflow in WARN state
        oozie_worfklow_patch.return_value = {
            'workflow-1': {
                'status': 'WARN',
                'oozieId': '0123-oozie-oozi-W',
                'actions': {},
                'name': 'o1-workflow'
            }
            }
        process_component_data_patch.return_value = 'ERROR'
        result = application_summary.oozie_coordinator_handler({
            'actions': [{
                'externalId': '0123-oozi-W',
                'coordJobId': '0123-oozi-C'
            }],
            'coordJobId': '0123-oozie-oozi-C',
            'coordJobName': 'o1-coordinator',
            'status': 'RUNNING'
        })
        self.assertEqual(result['aggregate_status'], '%s_%s_%s' % \
        (ApplicationState.STARTED, 'RUNNING', ERROR_STATUS['WARN']))

        #Oozie Coordinator in case of Suspended and Workflow in OK state
        oozie_worfklow_patch.return_value = {
            'workflow-1': {
                'status': 'OK',
                'oozieId': '0123-oozie-oozi-W',
                'actions': {},
                'name': 'o1-workflow'
            }
            }
        process_component_data_patch.return_value = 'OK'
        result = application_summary.oozie_coordinator_handler({
            'actions': [{
                'externalId': '0123-oozie-oozi-W',
                'coordJobId': '0123-oozie-oozi-C'
            }],
            'coordJobId': '0123-oozie-oozi-C',
            'coordJobName': 'o1-coordinator',
            'status': 'SUSPENDED'
        })
        self.assertEqual(result['aggregate_status'], '%s_%s_%s' \
        % (ApplicationState.COMPLETED, 'SUSPENDED', FAILURE_STATUS['OK']))

        #Oozie Coordinator in case of Killed and one workflow in ERROR state
        oozie_worfklow_patch.return_value = {
            'workflow-1': {
                'status': 'ERROR',
                'oozieId': '0123-oozie-oozi-W',
                'actions': {},
                'name': 'o1-workflow'
            }
            }
        process_component_data_patch.return_value = 'ERROR'
        result = application_summary.oozie_coordinator_handler({
            'actions': [{
                'externalId': '0123-oozie-oozi-W',
                'coordJobId': '0123-oozie-oozi-C'
            }],
            'coordJobId': '0123-oozie-oozi-C',
            'coordJobName': 'o1-coordinator',
            'status': 'KILLED'
        })
        self.assertEqual(result['aggregate_status'], '%s_%s_%s' \
        % (ApplicationState.COMPLETED, 'KILLED', FAILURE_STATUS['ERROR']))

    @patch('application_summary.process_component_data')
    @patch('application_summary.get_oozie_workflow_actions')
    def test_oozie_workflow_handler(self, oozie_worfklow_patch, process_component_data_patch):
        """
        Testing Oozie Workflow
        """
        #Oozie Workflow in case of Prep
        result = application_summary.oozie_workflow_handler({
            'actions': [],
            'id': '0123-oozie-oozi-W',
            'appName': 'o2-workflow',
            'status': 'PREP'
        })
        self.assertEqual(result['aggregate_status'], ApplicationState.CREATED)

        #Oozie Workflow in case of Running and one job in ERROR state
        oozie_worfklow_patch.return_value = {
            'job-1': {
                'status': 'ERROR',
                'yarnId': 'application_1234',
                'name': 'process'
            }
            }
        process_component_data_patch.return_value = 'ERROR'
        result = application_summary.oozie_workflow_handler({
            'actions': [{
                'externalId': 'job_1234',
                'name': 'process'
            }],
            'id': '0123-oozie-oozi-C',
            'appName': 'o2-workflow',
            'status': 'RUNNING'
        })
        self.assertEqual(result['aggregate_status'], '%s_%s_%s' % \
        (ApplicationState.STARTED, 'RUNNING', ERROR_STATUS['ERROR']))

        #Oozie Workflow in case of Suspended and job in OK state
        oozie_worfklow_patch.return_value = {
            'job-1': {
                'status': 'OK',
                'yarnId': 'application_1234',
                'name': 'process'
            }
            }
        process_component_data_patch.return_value = 'OK'
        result = application_summary.oozie_workflow_handler({
            'actions': [{
                'externalId': 'job_1234',
                'name': 'process'
            }],
            'id': '0123-oozie-oozi-C',
            'appName': 'o2-workflow',
            'status': 'SUSPENDED'
        })
        self.assertEqual(result['aggregate_status'], '%s_%s_%s' % \
        (ApplicationState.COMPLETED, 'SUSPENDED', FAILURE_STATUS['OK']))

        #Oozie Workflow in case of Killed and job in OK state
        oozie_worfklow_patch.return_value = {
            'job-1': {
                'status': 'OK',
                'yarnId': 'application_1234',
                'name': 'process'
            }
            }
        process_component_data_patch.return_value = 'OK'
        result = application_summary.oozie_workflow_handler({
            'actions': [{
                'externalId': 'job_1234',
                'name': 'process'
            }],
            'id': '0123-oozie-oozi-C',
            'appName': 'o2-workflow',
            'status': 'KILLED'
        })
        self.assertEqual(result['aggregate_status'], '%s_%s_%s' % \
        (ApplicationState.COMPLETED, 'KILLED', FAILURE_STATUS['OK']))

    @patch('requests.get')
    def test_oozie_api_request(self, mock_req):
        """
        Check Oozie API request returns a dict object
        """
        mock_req.return_value = type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
            'id': '01234-oozie-oozi-w',
            'appName': 'o2-workflow',
            'status': 'PREP'
        })})
        result = application_summary.oozie_api_request('01234-oozie-oozi-w')
        self.assertIsInstance(result, dict)

    @patch('application_summary.oozie_api_request')
    @patch('application_summary.yarn_info')
    @patch('application_summary.spark_job_handler')
    def test_get_oozie_workflow_actions(self, spark_job_patch, yarn_job_patch, oozie_api_patch):
        """
        Testing Oozie component's action handling for both Coordinator and Workflow
        """
        #In case of Oozie workflow with a Mapreduce job whose Yarn status's Failed
        yarn_job_patch.return_value = {'yarnStatus': 'FAILED', \
        'yarnFinalStatus': 'FAILED', 'type': 'MAPREDUCE'}
        result = application_summary.get_oozie_workflow_actions([
            {
                'status': 'RUNNING',
                'externalId': 'job_1235',
                'name': 'download',
                'type': 'shell',
                'externalChildIDs': None
            }])
        self.assertEqual(result, {
            'job-1': {
                'status': 'ERROR',
                'information': None,
                'applicationType': 'MAPREDUCE',
                'name': 'download',
                'yarnId': 'application_1235'
            }})

        #In case of Oozie workflow with a Spark job whose Yarn status's Running
        yarn_job_patch.return_value = {'yarnStatus': 'RUNNING', \
        'yarnFinalStatus': 'UNDEFINED', 'type': 'MAPREDUCE'}
        yarn_job_patch.return_value = {'yarnStatus': 'RUNNING', \
        'yarnFinalStatus': 'UNDEFINED', 'type': 'SPARK'}
        spark_job_patch.return_value = {'state': 'OK', 'information': {
            "stageSummary": {
                "active": 0,
                "number_of_stages": 448,
                "complete": 448,
                "pending": 0,
                "failed": 0
            },
            "jobSummary": {
                "unknown": 0,
                "number_of_jobs": 112,
                "running": 0,
                "succeeded": 112,
                "failed": 0
            }}}
        result = application_summary.get_oozie_workflow_actions([
            {
                'status': 'RUNNING',
                'externalId': 'job_1235',
                'name': 'process',
                'type': 'spark',
                'externalChildIDs': 'job_1236'
            }])
        self.assertEqual(result, {
            'job-1': {
                'status': 'OK',
                'information': {
                    "stageSummary": {
                        "active": 0,
                        "number_of_stages": 448,
                        "complete": 448,
                        "pending": 0,
                        "failed": 0
                    },
                    "jobSummary": {
                        "unknown": 0,
                        "number_of_jobs": 112,
                        "running": 0,
                        "succeeded": 112,
                        "failed": 0
                    }},
                'applicationType': 'SPARK',
                'name': 'process',
                'yarnId': 'application_1236'
            }})

        #In case of Oozie Coordinator with a worklfow have one subworkflow which have one Mapreduce job
        yarn_job_patch.return_value = {'yarnStatus': 'RUNNING', \
        'yarnFinalStatus': 'UNDEFINED', 'type': 'MAPREDUCE'}
        oozie_api_patch.side_effect = [{
            'status': 'SUCCEEDED',
            'appName': 'o1-workflow',
            'actions': [{
                'status': 'OK',
                'externalId': '0124-oozie-oozi-W',
                'name': 'download',
                'type': 'sub-workflow'
                }],
            'id': '0123-oozie-oozi-W',
        }, {
            'status': 'SUCCEEDED',
            'appName': 'o1-subworkflow',
            'actions': [
                {
                    'status': 'OK',
                    'externalId': 'job_123',
                    'name': 'download',
                    'type': 'shell',
                    'externalChildIDs': None
                }
            ], 'id': '0124-oozie-oozi-W'}]
        result = application_summary.get_oozie_workflow_actions([
            {
                'status': 'SUCCEEDED',
                'externalId': '0123-oozie-oozi-W',
                'type': None
            }])
        self.assertEqual(result, {
            "workflow-1": {
                "status": "OK",
                "oozieId": "0123-oozie-oozi-W",
                "actions": {
                    "subworkflow-1": {
                        "status": "OK",
                        "oozieId": "0124-oozie-oozi-W",
                        "actions": {
                            "job-1": {
                                "status": "OK",
                                "information": None,
                                "applicationType": "MAPREDUCE",
                                "name": "download",
                                "yarnId": "application_123"
                            }
                        },
                        "name": "o1-subworkflow"
                    }
                    },
                "name": "o1-workflow"
            }})

    @patch('requests.get')
    def test_spark_job_handler(self, spark_mock_req):
        """
        Testing Spark API for jobs and stage information
        """
        spark_mock_req.side_effect = [type('obj', (object,), {'status_code' : 200, 'text': json.dumps([
            {
                'status': 'RUNNING',
                'stageIds': [
                    5,
                    6
                ],
                'jobId': 1
            },
            {
                'status': 'SUCCEEDED',
                'stageIds': [
                    0,
                    1
                ],
                'jobId': 0
            }])}), type('obj', (object,), {'status_code' : 200, 'text': json.dumps([
                {
                    'status': 'COMPLETE',
                    'stageId': 23
                },
                {
                    'status': 'COMPLETE',
                    'stageId': 22
                }])})]
        result = application_summary.spark_job_handler('application_123')
        self.assertEqual(result, {
            'information': {
                'stageSummary': {
                    'active': 0,
                    'number_of_stages': 2,
                    'complete': 2,
                    'pending': 0,
                    'failed': 0
                },
                'jobSummary': {
                    'unknown': 0,
                    'number_of_jobs': 2,
                    'running': 1,
                    'succeeded': 1,
                    'failed': 0
                }
            },
            'state': 'OK'
            })

    @patch('requests.get')
    def test_yarn_info(self, yarn_mock_req):
        """
        Tetsing Yarn API provides application data by providing it's application id
        """
        yarn_mock_req.side_effect = [type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
            'app': {
                'state': 'KILLED',
                'name': 'app1-example-job',
                'applicationType': 'SPARK',
                'startedTime': 1513311261486,
                'finalStatus': 'KILLED'
            }})}), type('obj', (object,), {'status_code' : 200, 'text': json.dumps({
                'RemoteException': {
                    'message': 'java.lang.Exception: app with id: application_123 not found'}})})]

        result = application_summary.yarn_info('application_123')
        self.assertEqual(result, {
            'yarnStatus': 'KILLED',
            'yarnFinalStatus': 'KILLED',
            'type': 'SPARK',
            'startedTime': 1513311261486
        })

        result = application_summary.yarn_info('application_123')
        self.assertEqual(result, {
            'yarnStatus': 'NOT FOUND',
            'yarnFinalStatus': 'UNKNOWN',
            'type': 'UNKNOWN',
            'information': 'app with id application_123 not found'
        })
