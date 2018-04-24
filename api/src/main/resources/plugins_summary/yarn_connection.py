import json
import requests

class YarnConnection(object):
    def __init__(self, environment):
        self.yarn_host = environment['yarn_resource_manager_host']
        self.yarn_port = environment['yarn_resource_manager_port']
        self.rest_api_req_timeout = environment['rest_api_req_timeout']

    def _get_yarn_start_time(self, app_info):
        try:
            return int(app_info['startedTime'])
        except:
            return 0

    def check_in_yarn(self, job_name):
        """
        Check in YARN list of Jobs with Job name provided and return latest application
        """
        url = 'http://%s:%s%s' % (self.yarn_host, self.yarn_port, '/ws/v1/cluster/apps')
        run_app_info = None
        yarn_list = requests.get(url, timeout=self.rest_api_req_timeout)
        yarn_list = json.loads(yarn_list.text)
        if yarn_list['apps'] != None:
            for app in yarn_list['apps']['app']:
                if job_name == app['name']:
                    if run_app_info is None or self._get_yarn_start_time(app) > self._get_yarn_start_time(run_app_info):
                        run_app_info = app

        return run_app_info

    def yarn_info(self, app_id):
        """
        Get YARN information for a Job Id
        """
        url = 'http://%s:%s%s/%s' % (self.yarn_host, self.yarn_port, '/ws/v1/cluster/apps', app_id)
        ret = {}
        yarn_app_info = requests.get(url, timeout=self.rest_api_req_timeout)
        yarn_app_info = json.loads(yarn_app_info.text)
        if 'app' in yarn_app_info:
            ret.update({
                'yarnStatus': yarn_app_info['app']['state'],
                'yarnFinalStatus': yarn_app_info['app']['finalStatus'],
                'startedTime': yarn_app_info['app']['startedTime'],
                'diagnostics': yarn_app_info['app']['diagnostics'],
                'type': yarn_app_info['app']['applicationType']
            })
        else:
            message = yarn_app_info['RemoteException']['message'].split(':')
            message[0] = ''
            message = ''.join(message).strip()
            ret.update({'information': message, 'yarnStatus': 'NOT FOUND', \
                'yarnFinalStatus': 'UNKNOWN', 'type': 'UNKNOWN'})
        return ret
