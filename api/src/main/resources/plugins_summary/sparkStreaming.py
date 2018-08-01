import json
import requests

from plugins_summary.component_summary import ComponentSummary

class SparkStreamingComponentSummary(ComponentSummary):

    def get_component_type(self):
        return 'sparkStreaming'

    def yarn_handler(self, yarn_data, application):
        '''
        Handling Spark YARN data
        '''
        del application
        aggregate_status = ''
        yarnid = yarn_data['id']
        tracking_url = ''
        information = ''

        if yarn_data['state'] == 'SUBMITTED' or yarn_data['state'] == 'ACCEPTED':
            aggregate_status = yarn_data['state']
            information = yarn_data['diagnostics'].split('Details :')[0].strip()
        elif yarn_data['state'] == 'RUNNING':
            spark_data = self._job_handler(yarn_data['id'])
            if spark_data['state'] == 'OK':
                aggregate_status = 'RUNNING'
            else:
                aggregate_status = 'RUNNING_WITH_ERRORS'
            information = spark_data['information']
            tracking_url = yarn_data['trackingUrl']
        elif yarn_data['state'] == 'FINISHED':
            aggregate_status = '%s_%s' % (yarn_data['state'], yarn_data['finalStatus'])
            information = yarn_data['diagnostics'].split('Details :')[0].strip()
        elif yarn_data['state'] == 'FAILED' or yarn_data['state'] == 'KILLED':
            aggregate_status = yarn_data['state']
            information = yarn_data['diagnostics'].split('Details :')[0].strip()
        else:
            aggregate_status = 'NOT_FOUND'
            message = yarn_data.get('RemoteException', {'message': ['']}).\
            get('message').split(':')
            message[0] = ''

        return aggregate_status, yarnid, tracking_url, information

    def _job_handler(self, app_id):
        """
        Find Job and Stage status of Spark Application
        """
        ret = {}
        state = None
        information = None

        url = 'http://%s:%s%s%s%s%s%s' % (self._yarn_connection.yarn_host, \
            self._yarn_connection.yarn_port, '/proxy/', app_id, '/api/v1/applications/',\
                app_id, '/jobs')
        spark_jobs = requests.get(url, timeout=self.environment['rest_api_req_timeout'])
        spark_jobs = json.loads(spark_jobs.text)
        if spark_jobs:
            information = {}
            job_count = spark_jobs[0]['jobId'] + 1
            job_run_c, job_suc_c, job_unknown_c, job_fail_c = \
            self._get_status_count(spark_jobs, 'RUNNING', 'SUCCEEDED', 'UNKNOWN', 'FAILED')

            url = 'http://%s:%s%s%s%s%s%s' % (self._yarn_connection.yarn_host, \
                self._yarn_connection.yarn_port, '/proxy/', \
                app_id, '/api/v1/applications/', app_id, '/stages')
            spark_stages = requests.get(url, timeout=self.environment['rest_api_req_timeout'])
            spark_stages = json.loads(spark_stages.text)
            stage_count = spark_stages[0]['stageId'] + 1
            stage_active_c, stage_complete_c, stage_pending_c, stage_failed_c = \
            self._get_status_count(spark_stages, 'ACTIVE', 'COMPLETE', 'PENDING', 'FAILED')
            information = {
                'jobSummary': {'number_of_jobs': job_count, 'unknown': job_unknown_c, \
                    'succeeded': job_suc_c, 'failed': job_fail_c, 'running': job_run_c},
                'stageSummary': {'number_of_stages': stage_count, 'active': stage_active_c, \
                    'complete': stage_complete_c, 'pending': stage_pending_c, \
                    'failed': stage_failed_c}
                }

            if job_fail_c >= 1:
                state = self.component_status['red']
            else:
                state = self.component_status['green']
        else:
            state = self.component_status['green']
            information = 'No Jobs available'

        ret.update({
            'state': state,
            'information': information
        })

        return ret

    def _get_status_count(self, status_data, status1, status2, status3, status4):
        status1_c, status2_c, status3_c, status4_c = (0, 0, 0, 0)
        for ele in status_data:
            if ele['status'] == status1:
                status1_c += 1
            if ele['status'] == status2:
                status2_c += 1
            if ele['status'] == status3:
                status3_c += 1
            if ele['status'] == status4:
                status4_c += 1
        return status1_c, status2_c, status3_c, status4_c
