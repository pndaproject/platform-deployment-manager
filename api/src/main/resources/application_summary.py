import json
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
ERROR_STATUS = dict([("OK", "WITH_NO_ERRORS"), ("ERROR", "WITH_ERRORS"), ("WARN", "WITH_ERRORS")])
FAILURE_STATUS = dict([("OK", "WITH_NO_FAILURES"), ("ERROR", "WITH_FAILURES"), \
("WARN", "WITH_FAILURES")])

# pylint: disable=R0204
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
                job_count = len(spark_jobs)
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
                stage_count = len(spark_stages)
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

def get_oozie_workflow_actions(actions):
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
                oozie_data = get_oozie_workflow_actions(oozie_info['actions'])
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
        'name': data['coordJobName']})
    elif data['status'] == 'PREP':
        coord_status.update({'aggregate_status': ApplicationState.STARTING, \
        'name': data['coordJobName']})
    elif data['status'] == 'RUNNING':
        oozie_data = get_oozie_workflow_actions(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        coord_status.update({'actions': oozie_data, 'status': aggregate_status, 'aggregate_status':\
        '%s_%s_%s' % (ApplicationState.STARTED, 'RUNNING', ERROR_STATUS[aggregate_status]), \
        'oozieId': data['coordJobId'], 'name': data['coordJobName']})
    elif data['status'] == 'SUSPENDED':
        oozie_data = get_oozie_workflow_actions(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        coord_status.update({'actions': oozie_data, 'status': aggregate_status, 'aggregate_status':\
        '%s_%s_%s' % (ApplicationState.COMPLETED, 'SUSPENDED', FAILURE_STATUS[aggregate_status]), \
        'oozieId': data['coordJobId'], 'name': data['coordJobName']})
    elif data['status'] == 'KILLED':
        oozie_data = get_oozie_workflow_actions(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        coord_status.update({'actions': oozie_data, 'status': aggregate_status, 'aggregate_status':\
        '%s_%s_%s' % (ApplicationState.COMPLETED, 'KILLED', FAILURE_STATUS[aggregate_status]), \
        'oozieId': data['coordJobId'], 'name': data['coordJobName']})
    return coord_status

def oozie_workflow_handler(data):
    """
    Handling OOZIE-WORKFLOW
    """
    workflow_status = {}
    if data['status'] == 'PREP':
        workflow_status.update({'aggregate_status': ApplicationState.CREATED, \
        'name': data['appName']})
    elif data['status'] == 'RUNNING':
        oozie_data = get_oozie_workflow_actions(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        workflow_status.update({'actions': oozie_data, 'aggregate_status': '%s_%s_%s' % \
        (ApplicationState.STARTED, 'RUNNING', ERROR_STATUS[aggregate_status]), \
        'oozieId': data['id'], 'name': data['appName'], 'status': aggregate_status})
    elif data['status'] == 'SUSPENDED':
        oozie_data = get_oozie_workflow_actions(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        workflow_status.update({'actions': oozie_data, 'aggregate_status': '%s_%s_%s' % \
        (ApplicationState.COMPLETED, 'SUSPENDED', FAILURE_STATUS[aggregate_status]), \
        'oozieId': data['id'], 'name': data['appName'], 'status': aggregate_status})
    elif data['status'] == 'KILLED':
        oozie_data = get_oozie_workflow_actions(data['actions'])
        aggregate_status = process_component_data(oozie_data)
        workflow_status.update({'actions': oozie_data, 'aggregate_status': '%s_%s_%s' % \
        (ApplicationState.COMPLETED, 'KILLED', FAILURE_STATUS[aggregate_status]), \
        'oozieId': data['id'], 'name': data['appName'], 'status': aggregate_status})
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

def spark_application(job_name):
    """
    Handling SPARK Application
    """
    new_app_flag = False
    ret = {}
    information = None
    app_id = check_in_yarn(job_name)
    if app_id != None:
        yarn_data = yarn_info(app_id['id'])
        if yarn_data['yarnFinalStatus'] == 'FAILED':
            aggregate_status = '%s_%s_%s' % (ApplicationState.COMPLETED, \
            'FAILED', FAILURE_STATUS['ERROR'])
        elif yarn_data['yarnFinalStatus'] == 'KILLED':
            aggregate_status = '%s_%s_%s' % (ApplicationState.COMPLETED, \
            'KILLED', FAILURE_STATUS['OK'])
        elif yarn_data['yarnStatus'] == 'RUNNING':
            spark_data = spark_job_handler(app_id['id'])
            aggregate_status = '%s_%s_%s' % (ApplicationState.STARTED, \
            yarn_data['yarnStatus'], ERROR_STATUS[spark_data['state']])
            information = spark_data['information']
        elif yarn_data['yarnStatus'] == 'FINISHED' and yarn_data['yarnFinalStatus'] == 'SUCCEEDED':
            aggregate_status = '%s_%s_%s' % (ApplicationState.COMPLETED, \
            yarn_data['yarnStatus'], ERROR_STATUS['OK'])
        elif yarn_data['yarnStatus'] == 'NOT FOUND':
            aggregate_status = '%s_%s_%s' % (ApplicationState.COMPLETED, \
            yarn_data['yarnStatus'], ERROR_STATUS['ERROR'])
            information = yarn_data['information']
        else:
            aggregate_status = '%s_%s_%s' % (ApplicationState.STARTED, \
            yarn_data['yarnStatus'], ERROR_STATUS['OK'])
        ret = {
            'aggregate_status': aggregate_status,
            'yarnId': app_id['id'],
            'information': information,
            'name': job_name
        }
    else:
        new_app_flag = True
    if new_app_flag:
        ret = {
            'aggregate_status': ApplicationState.CREATED,
            'information': information,
            'name': job_name
        }
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
                    (component[application][component_name]['component_job_name'])})
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
        component_count += oozie_component_count + spark_component_count
        (oozie_component_count, spark_component_count) = (0, 0)
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
    (current_agg_status_priority, current_error_status_priority, \
    current_failure_status_priority) = (99, 99, 99)
    aggregated_status_priority = dict([(ApplicationState.CREATED, 1), \
    (ApplicationState.STARTING, 2), (ApplicationState.STARTED, 3), (ApplicationState.COMPLETED, 4)])
    error_status_priority = dict([('ERRORS', 1), ('NO_ERRORS', 2)])
    failure_status_priority = dict([('FAILURES', 1), ('NO_FAILURES', 2)])
    for component in application:
        if current_agg_status_priority >= aggregated_status_priority[application[component]\
        ['aggregate_status'].split('_')[0]]:
            current_agg_status_priority = aggregated_status_priority[application[component]\
            ['aggregate_status'].split('_')[0]]
        temp = application[component]['aggregate_status'].split('WITH_')[-1]
        if 'ERRORS' in temp:
            if current_error_status_priority >= error_status_priority[temp]:
                current_error_status_priority = error_status_priority[temp]
        if 'FAILURES' in temp:
            if current_failure_status_priority >= failure_status_priority[temp]:
                current_failure_status_priority = failure_status_priority[temp]

    if current_agg_status_priority == 3:
        if current_error_status_priority == 2:
            if current_failure_status_priority == 1:
                error_status = 'ERRORS'
            else:
                error_status = 'NO_ERRORS'
        else:
            error_status = 'ERRORS'
        aggregated_status = '%s_%s_WITH_%s' % (ApplicationState.STARTED, 'RUNNING', error_status)
    elif current_agg_status_priority == 4:
        if current_failure_status_priority == 1:
            failure_status = 'FAILURES'
        else:
            failure_status = 'NO_FAILURES'
        aggregated_status = '%s_WITH_%s' % (ApplicationState.COMPLETED, failure_status)
    elif current_agg_status_priority == 1:
        aggregated_status = ApplicationState.CREATED
    else:
        aggregated_status = ApplicationState.STARTING
    return aggregated_status

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
