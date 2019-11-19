import json
import logging
import happybase
from thriftpy2.transport import TTransportException
from Hbase_thrift import AlreadyExists

#pylint: disable=E0602

class HBaseAppplicationSummary(object):
    def __init__(self, hbase_host):
        self._hbase_host = hbase_host
        self._table_name = 'platform_application_summary'
        if self._hbase_host is not None:
            try:
                connection = happybase.Connection(self._hbase_host)
                connection.create_table(self._table_name, {'cf': dict()})
                logging.debug("applications summary table created")
            except AlreadyExists as error_message:
                logging.debug("applications summary table already exists")
            except TTransportException as error_message:
                logging.error(str(error_message))
            finally:
                connection.close()

    def sync_with_dm(self, app_list):
        connection = None
        try:
            connection = happybase.Connection(self._hbase_host)
            table = connection.table(self._table_name)
            for application, _ in table.scan():
                if application not in app_list:
                    table.delete(application)
        except TTransportException as error_message:
            logging.error(str(error_message))
        finally:
            connection.close()

    def write_to_hbase(self, application, summary):
        connection = None
        try:
            connection = happybase.Connection(self._hbase_host)
            table = connection.table(self._table_name)
            table.put(application, summary)
        except TTransportException as error_message:
            logging.error(str(error_message))
        finally:
            connection.close()

    def post_to_hbase(self, summary, application):
        data = {}
        for component in summary[application]:
            if component != 'aggregate_status':
                data.update({component: summary[application][component]})
        data = {
            '%s:%s' % ('cf', 'component_data'): json.dumps(data),
            '%s:%s' % ('cf', 'aggregate_status'): summary[application]['aggregate_status']
        }
        self.write_to_hbase(application, data)

    def _read_from_db(self, key):
        connection = None
        try:
            connection = happybase.Connection(self._hbase_host)
            table = connection.table(self._table_name)
            data = table.row(key)
        except TTransportException as error_message:
            logging.error(str(error_message))
        finally:
            connection.close()
        return data

    def get_dm_data(self, key):
        connection = None
        try:
            connection = happybase.Connection(self._hbase_host)
            table = connection.table("platform_applications")
            data = table.row(key)
        except TTransportException as error_message:
            logging.error(str(error_message))
        finally:
            connection.close()
        return data

    def get_dm_status(self, key):
        try:
            connection = happybase.Connection(self._hbase_host)
            table = connection.table("platform_applications")
            row = table.row(key, columns=[b'cf:status'])
            status = row[b'cf:status']
        except TTransportException as error_message:
            logging.error(str(error_message))
        finally:
            connection.close()
        return status.decode()

    def get_flink_job_id(self, key):
        jid = ''
        data = self._read_from_db(key)
        if data:
            data = json.loads(data[b'cf:component_data'])
            for component in data:
                if 'flink' in component:
                    tracking_url = data[component]['tracking_url']
                    jid = tracking_url.split("jobs")[-1]
        return jid

    def get_summary_data(self, application):
        record = {application: {}}
        dm_data = self.get_dm_data(application)
        if dm_data:
            summary_data = self._read_from_db(application)
            if summary_data:
                record[application].update({
                    'aggregate_status': summary_data[b'cf:aggregate_status']
                })
                record[application].update(json.loads(summary_data[b'cf:component_data']))
            else:
                record[application].update({
                    'status': 'Not Available'
                })
        else:
            record[application].update({
                'status': 'Not Created'
            })
        return record
