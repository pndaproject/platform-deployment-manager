import time
import json
import logging
import sys
from importlib import import_module
from multiprocessing import TimeoutError as ThreadTimeoutError

from summary_aggregator import ComponentSummaryAggregator
from plugins_summary.yarn_connection import YarnConnection
from async_dispatcher import AsyncDispatcher
import application_registrar
import application_summary_registrar
import deployer_utils


# constants
SUMMARY_INTERVAL = 30
STATUS_INTERVAL = 0.1
REST_API_REQ_TIMEOUT = 5
MAX_APP_SUMMARY_TIMEOUT = 60

def milli_time():
    return int(round(time.time() * 1000))

class ApplicationDetailedSummary(object):

    def __init__(self, environment, config):
        self._environment = environment
        self._environment.update({'rest_api_req_timeout': REST_API_REQ_TIMEOUT})
        self._config = config
        self._application_registrar = application_registrar.HbaseApplicationRegistrar(environment['hbase_thrift_server'])
        self._application_summary_registrar = application_summary_registrar.HBaseAppplicationSummary(environment['hbase_thrift_server'])
        self._yarn_connection = YarnConnection(self._environment)
        self._summary_aggregator = ComponentSummaryAggregator()
        self._component_creators = {}
        self.dispatcher = AsyncDispatcher(num_threads=4)

    def generate(self):
        """
        Update applications detailed summary
        """
        applist = self._application_registrar.list_applications()
        logging.info("List of applications: %s", ', '.join(applist))
        self._application_summary_registrar.sync_with_dm(applist)
        apps_to_be_processed = {}

        for app in applist:
            apps_to_be_processed.update({app: self.generate_summary(app)})

        wait_time = 0

        # waiting block for all the application to get completed
        while len(apps_to_be_processed) != 0:
            for app_name in apps_to_be_processed.keys():
                try:
                    apps_to_be_processed[app_name].task.get(STATUS_INTERVAL) #
                    del apps_to_be_processed[app_name]
                except ThreadTimeoutError:
                    wait_time += STATUS_INTERVAL # increasing the wait time by status interval
                    if round(wait_time, 1) % MAX_APP_SUMMARY_TIMEOUT == 0:
                        # logging out list of applications whose wait time exceeds the max app summary timeout, on the interval of same max app summary timeout
                        # i.e. every 60 seconds as per current max app summary timeout
                        logging.error("Timeout exceeded, %s applications waiting for %d seconds", (',').join(apps_to_be_processed.keys()), int(wait_time))

    def generate_summary(self, application):
        """
        Update HBase wih recent application summary
        """
        def _do_generate():

            try:
                create_data = self._application_registrar.get_create_data(application)
                input_data = {}
                for component_name, component_data in create_data.items():
                    input_data[component_name] = {}
                    input_data[component_name]["component_ref"] = self._load_creator(component_name)
                    input_data[component_name]["component_data"] = component_data
                app_data = self._summary_aggregator.get_application_summary(application, input_data)
                self._application_summary_registrar.post_to_hbase(app_data, application)
                logging.debug("Application: %s, Status: %s", application, app_data[application]['aggregate_status'])
            except Exception as ex:
                logging.error('%s while trying to get status of application "%s"', str(ex), application)

        return self.dispatcher.run_as_asynch(task=_do_generate)

    def _load_creator(self, component_type):

        creator = self._component_creators.get(component_type)

        if creator is None:

            cls = '%s%sComponentSummary' % (component_type[0].upper(), component_type[1:])
            try:
                module = import_module("plugins_summary.%s" % component_type)
                self._component_creators[component_type] = getattr(module, cls)\
                (self._environment, self._yarn_connection, self._application_summary_registrar)
                creator = self._component_creators[component_type]
            except ImportError as exception:
                logging.error(
                    'Unable to load Creator for component type "%s" [%s]',
                    component_type,
                    exception)

        return creator

def main():
    """
    main
    """
    config = None
    with open('dm-config.json', 'r') as con:
        config = json.load(con)

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.getLevelName(config['config']['log_level']),
                        stream=sys.stderr)

    deployer_utils.fill_hadoop_env(config['environment'], config['config'])

    summary = ApplicationDetailedSummary(config['environment'], config['config'])

    logging.info('Starting... Building actual status for applications')

    while True:
        # making sure every 30 seconds generate summary initiated
        start_time_on_cur_round = milli_time()

        summary.generate()

        finish_time_on_cur_round = (milli_time() - start_time_on_cur_round)/1000.0
        logging.info("Finished generating summary, time taken %s seconds", str(finish_time_on_cur_round))

        if finish_time_on_cur_round >= SUMMARY_INTERVAL:
            continue
        else:
            # putting sleep only for the remainig time from the current round's time
            time.sleep(SUMMARY_INTERVAL - finish_time_on_cur_round)

if __name__ == "__main__":
    main()
