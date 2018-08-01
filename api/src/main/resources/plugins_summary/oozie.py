import json
import requests

from plugins_summary.component_summary import ComponentSummary

class OozieComponentSummary(ComponentSummary):

    def get_component_type(self):
        return 'oozie'

    def get_component_summary(self, component, application):
        ret_data = {}
        data = self._oozie_api_request(component['job_handle'])

        if 'coordJobName' in data:
            name = 'coordJobName'
            oozie_id = 'coordJobId'
        else:
            name = 'appName'
            oozie_id = 'id'

        if data['status'] == 'PREP' or data['status'] == 'PREPSUSPENDED':
            ret_data.update({'aggregate_status': "CREATED", \
                'oozieId': data[oozie_id], 'name': data[name]})
        elif data['status'] == 'RUNNING':
            oozie_data = self._oozie_action_handler(data['actions'])
            aggregate_status = self._process_data(oozie_data)
            if aggregate_status == 'OK':
                status = 'RUNNING'
            else:
                status = 'RUNNING_WITH_ERRORS'
            ret_data.update({'actions': oozie_data, 'aggregate_status': status, \
                'oozieId': data[oozie_id], 'name': data[name], 'status': aggregate_status})
        elif data['status'] == 'SUSPENDED' or data['status'] == 'KILLED':
            oozie_data = self._oozie_action_handler(data['actions'])
            aggregate_status = self._process_data(oozie_data)
            if aggregate_status == 'OK':
                status = '%s' % (data['status'])
            else:
                status = '%s_%s' % (data['status'], 'WITH_FAILURES')
            ret_data.update({'actions': oozie_data, 'aggregate_status': status, \
                'oozieId': data[oozie_id], 'name': data[name], 'status': aggregate_status})
        elif data['status'] == 'SUCCEEDED':
            oozie_data = self._oozie_action_handler(data['actions'])
            aggregate_status = self._process_data(oozie_data)
            ret_data.update({'actions': oozie_data, 'aggregate_status': 'COMPLETED', \
                'oozieId': data[oozie_id], 'name': data[name], 'status': aggregate_status})
        elif data['status'] == 'DONEWITHERROR':
            oozie_data = self._oozie_action_handler(data['actions'])
            aggregate_status = self._process_data(oozie_data)
            ret_data.update({'actions': oozie_data, 'aggregate_status': 'COMPLETED_WITH_FAILURES', \
                'oozieId': data[oozie_id], 'name': data[name], 'status': aggregate_status})
        ret_data.update({"componentType": "Oozie"})
        return ret_data

    def _action_yarn_handler(self, yarn_id):
        yarn_data = self._yarn_connection.yarn_info(yarn_id)
        (status, information) = ("", "")
        if yarn_data['yarnStatus'] == 'FAILED' or yarn_data['yarnStatus'] == 'KILLED':
            status = self.component_status['red']
            information = yarn_data['diagnostics']
        elif yarn_data['yarnStatus'] == 'SUBMITTED' or yarn_data['yarnStatus'] == 'ACCEPTED' or\
             yarn_data['yarnStatus'] == 'RUNNING' or yarn_data['yarnFinalStatus'] == 'SUCCEEDED':
            status = self.component_status['green']
        else:
            status = self.component_status['amber']
            information = yarn_data['diagnostics']
        return status, information

    def _oozie_action_handler(self, actions):
        """
        Handling OOZIE actions both Workflow and Coordinator
        """
        count = 1
        ret = {}
        for action in actions:
            if action['externalId'] is not None:
                if 'job_' in action.get('externalId', ''):
                    yarn_id = self._convert_job_id(action['externalChildIDs']) \
                    if action['externalChildIDs'] is not None \
                    else self._convert_job_id(action['externalId'])
                    applicationtype = action['type']
                    status, information = self._action_yarn_handler(yarn_id)
                    if action['status'] == 'ERROR':
                        status = self.component_status['red']
                        information = '%s, %s' % (information, action['errorMessage'])
                    ret.update({"%s-%d" % ("job", count):{
                        'status': status,
                        'yarnId': yarn_id,
                        'information': information,
                        'name': action['name'],
                        'applicationType': applicationtype
                    }})
                    count += 1
                if 'oozie-oozi-W' in action.get('externalId', ''):
                    type_name = self._find_workflow_type(action)
                    key = '%s-%d' % (type_name, count)
                    count += 1
                    oozie_info = self._oozie_api_request(action['externalId'])
                    oozie_data = self._oozie_action_handler(oozie_info['actions'])
                    job_status = self._process_data(oozie_data)
                    ret.update({key: {'actions': oozie_data, 'oozieId': action['externalId'], \
                    'status': job_status, 'name': oozie_info['appName']}})
        return ret

    def _process_data(self, data):
        """
        Process multiple Jobs or Subworkflows or Workflows data to find aggregate status
        """
        error_count, action_count, warn_count = (0, 0, 0)
        for ele in data:
            action_count += 1
            if data[ele]['status'] == 'ERROR':
                error_count += 1
            if data[ele]['status'] == 'WARN':
                warn_count += 1

        if error_count == action_count:
            status = self.component_status['red']
        elif error_count >= 1 or warn_count >= 1:
            status = self.component_status['amber']
        else:
            status = self.component_status['green']
        return status

    def _oozie_api_request(self, job_id):
        """
        Get OOZIE information for OOZIE Job Id
        """
        oozie_info = {}
        url = '%s%s%s' % (self.environment['oozie_uri'], '/v1/job/', job_id)
        oozie_info = requests.get(url, timeout=self.environment['rest_api_req_timeout'])
        oozie_info = json.loads(oozie_info.text)
        return oozie_info

    def _convert_job_id(self, job_id):
        """
        Replace keyword job with application to pass it to yarn and spark server
        """
        job_id = job_id.split('_')
        job_id[0] = 'application'
        job_id = '_'.join(job_id)
        return job_id

    def _find_workflow_type(self, action):
        """
        To find workflow type
        """
        type_name = ''
        if action['type'] == 'sub-workflow':
            type_name = 'subworkflow'
        else:
            type_name = 'workflow'
        return type_name
