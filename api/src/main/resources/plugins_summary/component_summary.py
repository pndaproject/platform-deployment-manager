import commands

class ComponentSummary(object):

    def __init__(self, environment, yarn_con, app_registrar):
        self.environment = environment
        self._yarn_connection = yarn_con
        self._application_summary_registrar = app_registrar
        self.component_status = dict([("green", "OK"), ("amber", "WARN"), ("red", "ERROR")])
        self._component_type = ''

    def get_components_summary(self, application, component_data):
        self._component_type = self.get_component_type()
        ret_data = {}

        for count, component in enumerate(component_data):
            ret_data.update({"%s-%d" % (self._component_type, count+1): self.get_component_summary(component, application)})

        return ret_data

    def get_component_summary(self, component, application):

        ret_data = {}
        dm_status = self._application_summary_registrar.get_dm_status(application)

        job_name = component['component_job_name']
        component_name = component['component_name']
        (aggregate_status, yarnid, tracking_url, information) = ('', '', '', '')
        yarn_data = self._yarn_connection.check_in_yarn(job_name)
        if dm_status == 'CREATED':
            if yarn_data != None:
                aggregate_status, yarnid, tracking_url, information = self.yarn_handler(yarn_data, application)
            else:
                aggregate_status = "CREATED"
        else:
            if yarn_data != None:
                aggregate_status, yarnid, tracking_url, information = self.yarn_handler(yarn_data, application)
            else:
                aggregate_status, information = self.check_in_service_log(self.environment['namespace'], application, component_name)

        ret_data = {
            'aggregate_status': aggregate_status,
            'yarnId': yarnid,
            'tracking_url': tracking_url,
            'information': information,
            'name': job_name,
            'componentType': "%s%s" % (self._component_type[0].upper(), self._component_type[1:])
        }

        return ret_data

    def check_in_service_log(self, namespace, application, component_name):
        '''
        Check in service log in case of application failed to submit to YARN
        '''
        service_name = '%s-%s-%s' % (namespace, application, component_name)
        (command, message, more_detail) = ('', '', '')
        command = 'sudo journalctl -u %s.service' % service_name
        out = commands.getoutput('%s -n 50' % command).split('\n')
        more_detail = 'More details: execute "journalctl -u %s"' % service_name

        for line in out:
            if 'Exception:' in line:
                message = '%s%s' % (line.split('Exception:')[0].split(' ')[-1], 'Exception')
                break

        if message == '':
            message = '%s' % (more_detail)
        else:
            message = '%s. %s' % (message, more_detail)

        return 'FAILED_TO_SUBMIT_TO_YARN', message
