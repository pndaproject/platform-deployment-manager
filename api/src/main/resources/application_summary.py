import json
import commands
import multiprocessing
import time
import logging
import sys
import requests

import application_registrar
import deployer_utils
import application_summary_registrar
from lifecycle_states import ApplicationState

CONFIG = None
YARN_RESOURCE_MANAGER = None
YARN_PORT = None
OOZIE_URI = None
_APPLICATION_REGISTRAR = None
_HBASE = None
_MAX_PROCESS_COUNT = 4
_MAX_TIME_BOUND = 60
_MAX_PROCESS_TIME = 6
COMPONENT_STATUS = dict([("green", "OK"), ("amber", "WARN"), ("red", "ERROR")])

def spark_job_handler(app_id):
    """
    Find Job and Stage status of Spark Application
    """
    ret = {}
    state = None
    information = None
    try:
        url = 'http://%s:%s%s%s%s%s%s' % (YARN_RESOURCE_MANAGER, YARN_PORT, \
        '/proxy/', app_id, '/api/v1/applications/', app_id, '/jobs')
        spark_jobs = requests.get(url, timeout=_MAX_PROCESS_TIME)
        try:
            spark_jobs = json.loads(spark_jobs.text)
            if spark_jobs:
                information = {}
                job_count = spark_jobs[0]['jobId'] + 1
                job_suc_c, job_unknown_c, job_fail_c, job_run_c = (0, 0, 0, 0)
                for ele in spark_jobs:
                    if ele['status'] == 'RUNNING':
                        job_run_c += 1
                    if ele['status'] == 'SUCCEEDED':
                        job_suc_c += 1
                    if ele['status'] == 'UNKNOWN':
                        job_unknown_c += 1
                    if ele['status'] == 'FAILED':
                        job_fail_c += 1
                url = 'http://%s:%s%s%s%s%s%s' % (YARN_RESOURCE_MANAGER, YARN_PORT, '/proxy/', \
                app_id, '/api/v1/applications/', app_id, '/stages')
                spark_stages = requests.get(url)
                spark_stages = json.loads(spark_stages.text)
                stage_count = spark_stages[0]['stageId'] + 1
                stage_complete_c, stage_active_c, stage_pending_c, stage_failed_c = (0, 0, 0, 0)
                for ele in spark_stages:
                    if ele['status'] == 'COMPLETE':
                        stage_complete_c += 1
                    if ele['status'] == 'ACTIVE':
                        stage_active_c += 1
                    if ele['status'] == 'PENDING':
                        stage_pending_c += 1
                    if ele['status'] == 'FAILED':
                        stage_failed_c += 1
                information = {
                    'jobSummary': {'number_of_jobs': job_count, 'unknown': job_unknown_c, \
                    'succeeded': job_suc_c, 'failed': job_fail_c, 'running': job_run_c},
                    'stageSummary': {'number_of_stages': stage_count, 'active': stage_active_c, \
                    'complete': stage_complete_c, 'pending': stage_pending_c, \
                    'failed': stage_failed_c}
                }
                if job_fail_c >= 1:
                    state = COMPONENT_STATUS['red']
                else:
                    state = COMPONENT_STATUS['green']
            else:
                state = COMPONENT_STATUS['red']
                information = 'No Jobs available'
        except ValueError as error_message:
            state = COMPONENT_STATUS['red']
            information = str(error_message)
    except requests.exceptions.Timeout as error_message:
        state = COMPONENT_STATUS['red']
        information = str(error_message)
    ret.update({
        'state': state,
        'information': information
    })
    return ret

def convert_job_id(job_id):
    """
    Replace keyword job with application to pass it to yarn and spark server
    """
    job_id = job_id.split('_')
    job_id[0] = 'application'
    job_id = '_'.join(job_id)
    return job_id

def yarn_info(app_id):
    """
    Get YARN information for a Job Id
    """
    url = 'http://%s:%s%s/%s' % (YARN_RESOURCE_MANAGER, YARN_PORT, '/ws/v1/cluster/apps', app_id)
    ret = {}
    try:
        yarn_app_info = requests.get(url, timeout=_MAX_PROCESS_TIME)
        try:
            yarn_app_info = json.loads(yarn_app_info.text)
            if 'app' in yarn_app_info:
                ret.update({
                    'yarnStatus': yarn_app_info['app']['state'],
                    'yarnFinalStatus': yarn_app_info['app']['finalStatus'],
                    'startedTime': yarn_app_info['app']['startedTime'],
                    'type': yarn_app_info['app']['applicationType']
                })
            else:
                message = yarn_app_info['RemoteException']['message'].split(':')
                message[0] = ''
                message = ''.join(message).strip()
                ret.update({'information': message, 'yarnStatus': 'NOT FOUND', \
                'yarnFinalStatus': 'UNKNOWN', 'type': 'UNKNOWN'})
        except ValueError as error_message:
            logging.debug(str(error_message))
    except requests.exceptions.Timeout as error_message:
        logging.debug(str(error_message))
    return ret

def find_workflow_type(actions):
    """
    To find workflow type
    """
    type_name = 'workflow'
    for action in actions:
        if action['type'] == 'sub-workflow':
            type_name = 'subworkflow'
            break
    return type_name

# pylint: disable=R0101

def oozie_action_handler(actions):
    """
    Handling OOZIE actions both Workflow and Coordinator
    """
    count = 1
    ret = {}
    for action in actions:
        if action['externalId'] != None:
            if 'job_' in action['externalId']:
                launcher_id = convert_job_id(action['externalId'])
                launcher_job = yarn_info(launcher_id)
                yarnid = launcher_id
                information = None
                applicationtype = launcher_job['type']
                if launcher_job['yarnStatus'] == 'KILLED' or \
                launcher_job['yarnFinalStatus'] == 'FAILED':
                    status = COMPONENT_STATUS['red']
                elif launcher_job['yarnStatus'] == 'RUNNING' or launcher_job['yarnStatus'] \
                == 'FINISHED' or launcher_job['yarnStatus'] == 'ACCEPTED':
                    status = COMPONENT_STATUS['green']
                    if action['externalChildIDs']:
                        actual_job_id = convert_job_id(action['externalChildIDs'])
                        actual_job = yarn_info(actual_job_id)
                        yarnid = actual_job_id
                        applicationtype = actual_job['type']
                        if actual_job['yarnStatus'] == 'KILLED' or \
                        actual_job['yarnFinalStatus'] == 'FAILED':
                            status = COMPONENT_STATUS['red']
                        elif action['type'] == 'spark':
                            spark_job = spark_job_handler(actual_job_id)
                            if spark_job['state'] == 'ERROR':
                                status = COMPONENT_STATUS['red']
                                information = spark_job['information']
                            else:
                                status = COMPONENT_STATUS['green']
                                information = spark_job['information']
                        else:
                            status = COMPONENT_STATUS['green']
                else:
                    status = COMPONENT_STATUS['red']
                    information = launcher_job['information']
                ret.update({"%s-%d" % ("job", count):{
                    'status': status,
                    'yarnId': yarnid,
                    'information': information,
                    'name': action['name'],
                    'applicationType': applicationtype
                }})
                count += 1
            if 'oozie-oozi-W' in action['externalId']:
                type_name = find_workflow_type(actions)
                key = '%s-%d' % (type_name, count)
                count += 1
                oozie_info = oozie_api_request(action['externalId'])
                oozie_data = oozie_action_handler(oozie_info['actions'])
                workflowstatus = process_component_data(oozie_data)
                if action['status'] == 'ERROR' or action['status'] == 'FAILED' \
                or action['status'] == 'KILLED':
                    action_status = COMPONENT_STATUS['red']
                else:
                    action_status = COMPONENT_STATUS['green']
                if action_status == 'OK' and workflowstatus == 'OK':
                    status = COMPONENT_STATUS['green']
                elif action_status == 'OK' and workflowstatus == 'WARN':
                    status = COMPONENT_STATUS['amber']
                else:
                    status = COMPONENT_STATUS['red']
                ret.update({key: {'actions': oozie_data, 'oozieId': action['externalId'], \
                'status': status, 'name': oozie_info['appName']}})
    return ret

def process_component_data(data):
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
    if error_count == 0:
        if warn_count >= 1:
            status = COMPONENT_STATUS['amber']
        else:
            status = COMPONENT_STATUS['green']
    elif error_count == action_count:
        status = COMPONENT_STATUS['red']
    else:
        status = COMPONENT_STATUS['amber']
    return status

def oozie_api_request(job_id):
    """
    Get OOZIE information for OOZIE Job Id
    """
    oozie_info = {}
    url = '%s%s%s' % (OOZIE_URI, '/v1/job/', job_id)
    try:
        oozie_info = requests.get(url, timeout=_MAX_PROCESS_TIME)
        try:
            oozie_info = json.loads(oozie_info.text)
        except ValueError as error_message:
            logging.debug(str(error_message))
    except requests.exceptions.Timeout as error_message:
        logging.debug(str(error_message))
    return oozie_info

def oozie_coordinator_handler(data):
    """
    Handling OOZIE-COORDINATOR
    """
    coord_status = {}
    if data['status'] == 'PREPSUSPENDED':
        coord_status.update({'aggregate_status': ApplicationState.CREATED, \
        'oozieId': data['coordJobId'], 'name': data['coordJobName']})
    elif data['status'] == 'PREP':
        coord_status.update({'aggregate_status': ApplicationState.STARTING, \
        'oozieId': data['coordJobId'], 'name': data['coordJobName']})
    elif data['status'] == 'RUNNING':
        oozie_data = oozie_action_handler(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        if aggregate_status == 'OK':
            status = 'RUNNING'
        else:
            status = 'RUNNING_WITH_ERRORS'
        coord_status.update({'actions': oozie_data, 'status': aggregate_status, 'aggregate_status':status, \
        'oozieId': data['coordJobId'], 'name': data['coordJobName']})
    elif data['status'] == 'SUSPENDED' or data['status'] == 'KILLED':
        oozie_data = oozie_action_handler(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        if aggregate_status == 'OK':
            status = '%s' % (data['status'])
        else:
            status = '%s_%s' % (data['status'], 'WITH_FAILURES')
        coord_status.update({'actions': oozie_data, 'status': aggregate_status, 'aggregate_status': status, \
        'oozieId': data['coordJobId'], 'name': data['coordJobName']})
    elif data['status'] == 'SUCCEEDED':
        oozie_data = oozie_action_handler(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        coord_status.update({'actions': oozie_data, 'status': aggregate_status, 'aggregate_status': 'COMPLETED', \
        'oozieId': data['coordJobId'], 'name': data['coordJobName']})
    elif data['status'] == 'DONEWITHERROR':
        oozie_data = oozie_action_handler(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        coord_status.update({'actions': oozie_data, 'status': aggregate_status, 'aggregate_status': 'COMPLETED_WITH_FAILURES', \
        'oozieId': data['coordJobId'], 'name': data['coordJobName']})
    coord_status.update({"componentType": "Oozie"})
    return coord_status

def oozie_workflow_handler(data):
    """
    Handling OOZIE-WORKFLOW
    """
    workflow_status = {}
    if data['status'] == 'PREP':
        workflow_status.update({'aggregate_status': ApplicationState.CREATED, \
        'oozieId': data['id'], 'name': data['appName']})
    elif data['status'] == 'RUNNING':
        oozie_data = oozie_action_handler(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        if aggregate_status == 'OK':
            status = 'RUNNING'
        else:
            status = 'RUNNING_WITH_ERRORS'
        workflow_status.update({'actions': oozie_data, 'aggregate_status': status, \
        'oozieId': data['id'], 'name': data['appName'], 'status': aggregate_status})
    elif data['status'] == 'SUSPENDED' or data['status'] == 'KILLED':
        oozie_data = oozie_action_handler(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        if aggregate_status == 'OK':
            status = '%s' % (data['status'])
        else:
            status = '%s_%s' % (data['status'], 'WITH_FAILURES')
        workflow_status.update({'actions': oozie_data, 'aggregate_status': status, \
        'oozieId': data['id'], 'name': data['appName'], 'status': aggregate_status})
    elif data['status'] == 'SUCCEEDED':
        oozie_data = oozie_action_handler(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        workflow_status.update({'actions': oozie_data, 'aggregate_status': 'COMPLETED', \
        'oozieId': data['id'], 'name': data['appName'], 'status': aggregate_status})
    elif data['status'] == 'DONEWITHERROR':
        oozie_data = oozie_action_handler(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        workflow_status.update({'actions': oozie_data, 'aggregate_status': 'COMPLETED_WITH_FAILURES', \
        'oozieId': data['id'], 'name': data['appName'], 'status': aggregate_status})
    workflow_status.update({"componentType": "Oozie"})
    return workflow_status

def oozie_application(job_handle):
    """
    Handling OOZIE Application
    """
    oozie_info = oozie_api_request(job_handle)
    if 'coordJobName' in oozie_info:
        return oozie_coordinator_handler(oozie_info)
    if 'appName' in oozie_info:
        return oozie_workflow_handler(oozie_info)
    return None

def compare_yarn_start_time(app_info):
    """
    returns startedTime
    """
    return app_info['startedTime']

def check_in_yarn(job_name):
    """
    Check in YARN list of Jobs with Job name provided and return latest application
    """
    url = 'http://%s:%s%s' % (YARN_RESOURCE_MANAGER, YARN_PORT, '/ws/v1/cluster/apps')
    run_app_info = None
    try:
        yarn_list = requests.get(url, timeout=_MAX_PROCESS_TIME)
        try:
            yarn_list = json.loads(yarn_list.text)
            if yarn_list['apps'] != None:
                for app in yarn_list['apps']['app']:
                    if job_name == app['name']:
                        if run_app_info is None or compare_yarn_start_time(app) > \
                        compare_yarn_start_time(run_app_info):
                            run_app_info = app
        except ValueError as error_message:
            logging.debug(str(error_message))
    except requests.exceptions.Timeout as error_message:
        logging.debug(str(error_message))
    return run_app_info

# pylint: disable=C0103

def check_in_service_log(namespace, application, component_name):
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

def spark_yarn_handler(yarn_data):
    '''
    Handling Spark YARN data
    '''
    information = ''
    aggregate_status = ''
    yarnid = yarn_data['id']
    if yarn_data['state'] == 'SUBMITTED' or yarn_data['state'] == 'ACCEPTED':
        aggregate_status = yarn_data['state']
        message = yarn_data['diagnostics'].split('Details :')[0].strip()
        information = message
    elif yarn_data['state'] == 'RUNNING':
        spark_data = spark_job_handler(yarn_data['id'])
        if spark_data['state'] == 'OK':
            aggregate_status = 'RUNNING'
        else:
            aggregate_status = 'RUNNING_WITH_ERRORS'
        information = spark_data['information']
    elif yarn_data['finalStatus'] == 'SUCCEEDED':
        aggregate_status = '%s_%s' % (yarn_data['state'], yarn_data['finalStatus'])
    elif yarn_data['state'] == 'FINISHED' and (yarn_data['finalStatus'] == 'FAILED' or yarn_data['finalStatus'] == 'KILLED'):
        aggregate_status = '%s_%s' % (yarn_data['state'], yarn_data['finalStatus'])
        information = yarn_data['diagnostics']
    elif yarn_data['finalStatus'] == 'FAILED' or yarn_data['finalStatus'] == 'KILLED':
        aggregate_status = yarn_data['finalStatus']
        information = yarn_data['diagnostics']
    else:
        aggregate_status = 'NOT_FOUND'
        message = yarn_data.get('RemoteException', {'message': ['']}).\
        get('message').split(':')
        message[0] = ''
        information = ''.join(message).strip()
    return aggregate_status, yarnid, information

def spark_application(job_name, application, component_name):
    """
    Handling SPARK Application
    """
    ret = {}
    check_in_service = False
    (aggregate_status, yarnid, information) = ('', '', '')
    status, timestamp = _HBASE.get_status_with_timestamp(application)
    yarn_data = check_in_yarn(job_name)
    if status == 'CREATED':
        if yarn_data != None:
            aggregate_status, yarnid, information = spark_yarn_handler(yarn_data)
        else:
            aggregate_status = ApplicationState.CREATED
    else:
        if yarn_data != None:
            if timestamp < yarn_data['startedTime']:
                aggregate_status, yarnid, information = spark_yarn_handler(yarn_data)
            else:
                check_in_service = True
        else:
            check_in_service = True
    if check_in_service:
        aggregate_status, information = check_in_service_log(CONFIG['environment']['namespace'], application, component_name)
    ret = {
        'aggregate_status': aggregate_status,
        'yarnId': yarnid,
        'information': information,
        'name': job_name
    }
    ret.update({"componentType": "SparkStreaming"})
    return ret

def flink_job_handler(flink_url):
    flink_data = {'state': 'ERROR', 'flinkJid': ''}
    positive_status_list = ['CREATED', 'SCHEDULED', 'DEPLOYING', 'RUNNING', 'FINISHED', 'RECONCILING']
    try:
        url = '%s%s' % (flink_url, 'jobs')
        flink_job_list_resp = requests.get(url, timeout=_MAX_PROCESS_TIME)
        flink_job_list_resp = json.loads(flink_job_list_resp.text)
        if flink_job_list_resp['jobs-running']:
            url = '%s/%s' % (url, flink_job_list_resp['jobs-running'][0])
            flink_resp_data = requests.get(url, timeout=_MAX_PROCESS_TIME)
            flink_resp_data = json.loads(flink_resp_data.text)
            flink_data['flinkJid'] = flink_resp_data['jid']
            flink_data['vertices'] = []
            for vertice in flink_resp_data['vertices']:
                if vertice['status'] in positive_status_list:
                    flink_data['state'] = 'OK'
                flink_data['vertices'].append({'name': vertice['name'].split(' ')[0].split(':')[0], \
                'status': vertice['status']})
    except ValueError as error_message:
        logging.debug(str(error_message))
    except requests.exceptions.Timeout as error_message:
        logging.debug(str(error_message))
    return flink_data

def flink_yarn_handler(yarn_data, application):
    '''
    Handling Flink YARN data
    '''
    information = {}
    (aggregate_status, trackingUrl) = ('', '')
    yarnid = yarn_data['id']
    if yarn_data['state'] == 'SUBMITTED' or yarn_data['state'] == 'ACCEPTED':
        aggregate_status = yarn_data['state']
        message = yarn_data['diagnostics'].split('Details :')[0].strip()
        information = {'yarn_diagnostics': message}
    elif yarn_data['state'] == 'RUNNING':
        flink_data = flink_job_handler(yarn_data['trackingUrl'])
        if flink_data['state'] == 'OK':
            aggregate_status = 'RUNNING'
        else:
            aggregate_status = 'RUNNING_WITH_ERRORS'
        information = flink_data
        trackingUrl = '%s#/jobs/%s' % (yarn_data['trackingUrl'], flink_data['flinkJid'])
    elif yarn_data['finalStatus'] == 'SUCCEEDED':
        aggregate_status = 'SUCCEEDED'
        flink_job_id = _HBASE.get_flink_job_id(application)
        if len(flink_job_id) < 1:
            trackingUrl = 'http://%s/#/completed-jobs' % (CONFIG['environment']['flink_history_server'])
        else:
            trackingUrl = 'http://%s/#/jobs/%s' % (CONFIG['environment']['flink_history_server'], flink_job_id)
        information = {'flinkJid': flink_job_id}
    elif yarn_data['finalStatus'] == 'FAILED' or yarn_data['finalStatus'] == 'KILLED':
        aggregate_status = yarn_data['finalStatus']
        flink_job_id = _HBASE.get_flink_job_id(application)
        if len(flink_job_id) < 1:
            trackingUrl = 'http://%s/#/completed-jobs' % (CONFIG['environment']['flink_history_server'])
        else:
            trackingUrl = 'http://%s/#/jobs/%s' % (CONFIG['environment']['flink_history_server'], flink_job_id)
        message = yarn_data['diagnostics'].split('Details :')[0].strip()
        information = {'yarn_diagnostics': message, 'flinkJid': flink_job_id}
    else:
        aggregate_status = 'NOT_FOUND'
        message = yarn_data.get('RemoteException', {'message': ['']}).\
        get('message').split(':')
        message[0] = ''
        information = {'yarn_diagnostics': ''.join(message).strip()}
    return aggregate_status, yarnid, trackingUrl, information

def flink_application(job_name, application, component_name):
    """
    Handling Flink Application
    """
    (ret, information) = ({}, {})
    (aggregate_status, yarnid, trackingUrl) = ('', '', '')
    check_in_service = False
    status, timestamp = _HBASE.get_status_with_timestamp(application)
    yarn_data = check_in_yarn(job_name)
    if status == 'CREATED':
        if yarn_data != None:
            aggregate_status, yarnid, trackingUrl, information = flink_yarn_handler(yarn_data, application)
        else:
            aggregate_status = ApplicationState.CREATED
    else:
        if yarn_data != None:
            if timestamp < yarn_data['startedTime']:
                aggregate_status, yarnid, trackingUrl, information = flink_yarn_handler(yarn_data, application)
            else:
                check_in_service = True
        else:
            check_in_service = True
    if check_in_service:
        aggregate_status, message = check_in_service_log(CONFIG['environment']['namespace'], application, component_name)
        information.update({'service_script_log': message})
    ret = {
        'aggregate_status': aggregate_status,
        'yarnId': yarnid,
        'information': information,
        'trackingUrl': trackingUrl,
        'name': job_name
    }
    ret.update({"componentType": "Flink"})
    return ret

def get_json(component_list, queue_obj):
    """
    Processing Components and return generated Component data
    """
    process_name = multiprocessing.current_process().name
    logging.info('%s %s', 'Starting', process_name)
    ret = {}
    for component in component_list:
        for application in component:
            if application not in ret.keys():
                ret.update({application: {}})
            for component_name in component[application]:
                if 'oozie' in component_name:
                    ret[application].update({component_name: oozie_application\
                    (component[application][component_name]['job_handle'])})
                if 'sparkStreaming' in component_name:
                    ret[application].update({component_name: spark_application\
                    (component[application][component_name]['component_job_name'], application,\
                     component[application][component_name]['component_name'])})
                if 'flink' in component_name:
                    ret[application].update({component_name: flink_application\
                    (component[application][component_name]['component_job_name'], application,\
                     component[application][component_name]['component_name'])})
    queue_obj.put([{process_name: ret}])
    logging.info('%s %s', 'Finished', process_name)

def combine_data(summarydata):
    """
    Combine components into application level
    """
    application_data = {}
    for process in summarydata:
        for app in summarydata[process]:
            if app not in application_data.keys():
                application_data.update({app: {}})
            for component in summarydata[process][app]:
                application_data[app].update({component: summarydata[process][app][component]})
    return application_data

def split_application_components(alist, split_c):
    """
    Split Components of application across processes based on process count
    """
    applist = [[] for _ in range(split_c)]
    index = 0
    for app in alist:
        create_data = _APPLICATION_REGISTRAR.get_create_data(app)
        if 'oozie' in create_data:
            for count, oozie_component in enumerate(create_data['oozie']):
                applist[index].append({app: {
                    '%s-%d' % ('oozie', (count+1)): oozie_component
                }})
                index = (index + 1) % split_c
        if 'sparkStreaming' in create_data:
            for count, spark_component in enumerate(create_data['sparkStreaming']):
                applist[index].append({app: {
                    '%s-%d' % ('sparkStreaming', (count+1)): spark_component
                }})
                index = (index + 1) % split_c
        if 'flink' in create_data:
            for count, flink_component in enumerate(create_data['flink']):
                applist[index].append({app: {
                    '%s-%d' % ('flink', (count+1)): flink_component
                }})
                index = (index + 1) % split_c
    return applist

def find_split_count(applist):
    """
    Find process count based on all applications component's count
    """
    (component_count, oozie_component_count, spark_component_count) = (0, 0, 0)
    split_count = 1
    for application in  applist:
        create_data = _APPLICATION_REGISTRAR.get_create_data(application)
        if 'oozie' in create_data:
            oozie_component_count = len(create_data['oozie'])
        if 'sparkStreaming' in create_data:
            spark_component_count = len(create_data['sparkStreaming'])
        if 'flink' in create_data:
            flink_component_count = len(create_data['flink'])
        component_count += oozie_component_count + spark_component_count + flink_component_count
        (oozie_component_count, spark_component_count, flink_component_count) = (0, 0, 0)
    process_count = (component_count * _MAX_PROCESS_TIME)/_MAX_TIME_BOUND
    if process_count > _MAX_PROCESS_COUNT:
        split_count = _MAX_PROCESS_COUNT
    elif process_count >= 1:
        split_count = process_count
    return split_count

def process_application_data(application):
    """
    Find Application level aggregated status based on it's component's status
    """
    aggregated_status_priority = dict([(1, ApplicationState.CREATED), \
    (2, ApplicationState.STARTING), (3, "RUNNING"), (4, 'RUNNING_WITH_ERRORS'), \
    (5, 'STOPPED'), (6, 'STOPPED_WITH_FAILURES'), (7, 'KILLED'), (8, 'KILLED_WITH_FAILURES'), \
    (9, 'COMPLETED'), (10, 'COMPLETED_WITH_FAILURES'), (11, 'NOT_FOUND')])
    current_agg_status_priority = 99
    for component in application:
        temp_comp_status = application[component].get('aggregate_status', '')
        temp_status_priority = 0
        if temp_comp_status == 'CREATED':
            temp_status_priority = 1
        elif temp_comp_status == 'STARTING' or temp_comp_status == 'SUBMITTED' \
            or temp_comp_status == 'ACCEPTED':
            temp_status_priority = 2
        elif temp_comp_status == 'RUNNING':
            temp_status_priority = 3
        elif temp_comp_status == 'RUNNING_WITH_ERRORS':
            temp_status_priority = 4
        elif temp_comp_status == 'SUSPENDED':
            temp_status_priority = 5
        elif temp_comp_status == 'SUSPENDED_WITH_FAILURES':
            temp_status_priority = 6
        elif temp_comp_status == 'KILLED' or temp_comp_status == 'FINISHED_KILLED':
            temp_status_priority = 7
        elif temp_comp_status == 'KILLED_WITH_FAILURES':
            temp_status_priority = 8
        elif temp_comp_status == 'COMPLETED' or temp_comp_status == 'FINISHED_SUCCEEDED' \
            or temp_comp_status == 'SUCCEEDED':
            temp_status_priority = 9
        elif temp_comp_status == 'COMPLETED_WITH_FAILURES' or temp_comp_status == 'FAILED' \
            or temp_comp_status == 'FINISHED_FAILED':
            temp_status_priority = 10
        else:
            temp_status_priority = 11
        if temp_status_priority < current_agg_status_priority:
            current_agg_status_priority = temp_status_priority
    return aggregated_status_priority[current_agg_status_priority]

def application_summary(app_list):
    """
    application_summary
    """
    _HBASE.remove_app_entry(app_list)
    if app_list:
        logging.info('%d-%s', len(app_list), 'Applications found')
        processes = []
        summary_data = {}
        split_count = find_split_count(app_list)
        logging.info('%d %s', split_count, 'Processes will be created and they will be \
processed in multiprocess environment')
        split_app_list = split_application_components(app_list, split_count)
        queue_obj = multiprocessing.Queue()
        for i, app in enumerate(split_app_list):
            process = multiprocessing.Process(name='%s-%d' %('process', i), \
            target=get_json, args=(app, queue_obj,))
            processes.append(process)
            process.start()
        total_time = _MAX_TIME_BOUND
        for process in processes:
            start_time = time.time()
            process.join(timeout=total_time)
            try:
                summary_data.update(queue_obj.get(block=False)[0])
            except multiprocessing.queues.Empty:
                pass
            total_time = total_time - int(time.time() - start_time)
        applications = combine_data(summary_data)
        for application in applications:
            aggregate_status = process_application_data(applications[application])
            applications[application].update({'aggregate_status': aggregate_status})
            logging.info("%s%s%s", application, "'s aggregate status is ", aggregate_status)
        _HBASE.post_to_hbase(applications)
        for process in processes:
            if process.is_alive():
                process.terminate()
        logging.info('All processes finished')
    else:
        logging.info('No application found')

# pylint: disable=C0103
# pylint: disable=W0603

def main():
    """
    main
    """
    global CONFIG
    global YARN_RESOURCE_MANAGER
    global YARN_PORT
    global OOZIE_URI
    global _APPLICATION_REGISTRAR
    global _HBASE

    with open('dm-config.json', 'r') as conf:
        CONFIG = json.load(conf)

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.getLevelName(CONFIG['config']['log_level']),
                        stream=sys.stderr)
    logging.info('Starting... Building actual status for applications')
    deployer_utils.fill_hadoop_env(CONFIG['environment'], CONFIG['config'])
    _APPLICATION_REGISTRAR = application_registrar.HbaseApplicationRegistrar\
    (CONFIG['environment']['hbase_thrift_server'])
    _HBASE = application_summary_registrar.HBaseAppplicationSummary\
    (CONFIG['environment']['hbase_thrift_server'])
    YARN_RESOURCE_MANAGER = CONFIG['environment']['yarn_resource_manager_host']
    YARN_PORT = CONFIG['environment']['yarn_resource_manager_port']
    OOZIE_URI = CONFIG['environment']['oozie_uri']
    prev_run_app_list = []
    while True:
        start_time = time.time()
        app_list = _APPLICATION_REGISTRAR.list_applications()
        application_summary(app_list)
        prev_run_app_list = app_list
        wait_time = _MAX_TIME_BOUND - int(time.time() - start_time)
        app_list = _APPLICATION_REGISTRAR.list_applications()
        if set(app_list).difference(prev_run_app_list) == set([]) \
        and set(prev_run_app_list).difference(app_list) == set([]):
            time.sleep(wait_time)
        else:
            continue

if __name__ == "__main__":
    main()
