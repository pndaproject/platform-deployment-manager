import json
import requests

from plugins_summary.component_summary import ComponentSummary

class FlinkComponentSummary(ComponentSummary):

    def get_component_type(self):
        return 'flink'

    def yarn_handler(self, yarn_data, application):
        '''
        Handling Flink YARN data
        '''

        (aggregate_status, tracking_url) = ('', '')
        information = ''
        yarnid = yarn_data['id']

        if yarn_data['state'] == 'SUBMITTED' or yarn_data['state'] == 'ACCEPTED':
            aggregate_status = yarn_data['state']
            information = yarn_data['diagnostics'].split('Details :')[0].strip()
        elif yarn_data['state'] == 'RUNNING':
            data = self._job_handler(yarn_data['trackingUrl'])
            if data['state'] == 'OK':
                aggregate_status = 'RUNNING'
            else:
                aggregate_status = 'RUNNING_WITH_ERRORS'
            information = {}
            information = data
            tracking_url = '%s#/jobs/%s' % (yarn_data['trackingUrl'], data['flinkJid']) \
            if len(data['flinkJid']) > 1 else yarn_data['trackingUrl']
        elif yarn_data['state'] == 'FINISHED':
            aggregate_status = '%s_%s' % (yarn_data['state'], yarn_data['finalStatus'])
            flink_job_id = self._application_summary_registrar.get_flink_job_id\
            (application).strip('/')
            if len(flink_job_id) < 1:
                tracking_url = 'http://%s/#/completed-jobs' % \
                (self.environment['flink_history_server'])
            else:
                tracking_url = 'http://%s/#/jobs/%s' % \
                (self.environment['flink_history_server'], flink_job_id)
            information = yarn_data['diagnostics'].split('Details :')[0].strip()
        elif yarn_data['finalStatus'] == 'FAILED' or yarn_data['finalStatus'] == 'KILLED':
            aggregate_status = yarn_data['finalStatus']
            flink_job_id = self._application_summary_registrar.get_flink_job_id\
            (application).strip('/')
            if len(flink_job_id) < 1:
                tracking_url = 'http://%s/#/completed-jobs' % \
                (self.environment['flink_history_server'])
            else:
                tracking_url = 'http://%s/#/jobs/%s' % \
                (self.environment['flink_history_server'], flink_job_id)
            information = yarn_data['diagnostics'].split('Details :')[0].strip()
        else:
            aggregate_status = 'NOT_FOUND'
            message = yarn_data.get('RemoteException', {'message': ['']}).\
            get('message').split(':')
            message[0] = ''
            information = ''.join(message).strip()
        return aggregate_status, yarnid, tracking_url, information

    def _job_handler(self, url):
        ret_data = {'state': 'OK', 'flinkJid': ''}
        positive_status_list = ['CREATED', 'SCHEDULED', 'DEPLOYING', 'RUNNING', \
        'FINISHED', 'RECONCILING']

        url = '%s%s' % (url, 'jobs')
        job_list_resp = requests.get(url, timeout=self.environment['rest_api_req_timeout'])
        job_list_resp = json.loads(job_list_resp.text)

        if job_list_resp['jobs-running']:
            url = '%s/%s' % (url, job_list_resp['jobs-running'][0])
            job_data = requests.get(url, timeout=self.environment['rest_api_req_timeout'])
            job_data = json.loads(job_data.text)
            ret_data['flinkJid'] = job_data['jid']
            ret_data['vertices'] = []

            for vertice in job_data['vertices']:
                if vertice['status'] not in positive_status_list:
                    ret_data['state'] = 'ERROR'
                ret_data['vertices'].append({'name': vertice['name'].split(' ')[0]\
                    .split(':')[0], 'status': vertice['status']})

        return ret_data
