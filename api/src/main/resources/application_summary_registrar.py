import json
import logging
import happybase
from Hbase_thrift import AlreadyExists
from thriftpy.transport import TTransportException

#pylint: disable=E0602

class HBaseAppplicationSummary(object):
    def __init__(self, hbase_host):
        self._hbase_host = hbase_host
        self._table_name = 'platform_application_summary'
        if self._hbase_host is not None:
            try:
                connection = happybase.Connection(self._hbase_host)
                connection.create_table(self._table_name, {'cf': dict()})
            except AlreadyExists as error_message:
                logging.info(str(error_message))
            except TTransportException as error_message:
                logging.debug(str(error_message))
            finally:
                connection.close()

    def remove_app_entry(self, app_list):
        connection = None
        try:
            connection = happybase.Connection(self._hbase_host)
            table = connection.table(self._table_name)
            for application, _ in table.scan():
                if application not in app_list:
                    table.delete(application)
        except TTransportException as error_message:
            logging.debug(str(error_message))
        finally:
            connection.close()

    def write_to_hbase(self, application, summary):
        connection = None
        try:
            connection = happybase.Connection(self._hbase_host)
            table = connection.table(self._table_name)
            table.put(application, summary)
        except TTransportException as error_message:
            logging.debug(str(error_message))
        finally:
            connection.close()

    def post_to_hbase(self, app_summary):
        for application in app_summary:
            data = {}
            for component in app_summary[application]:
                if 'aggregate_status' not in component:
                    data.update({component: app_summary[application][component]})
            data = {
                '%s:%s' % ('cf', 'component_data'): json.dumps(data),
                '%s:%s' % ('cf', 'aggregate_status'): app_summary[application]['aggregate_status']
            }
            self.write_to_hbase(application, data)

    def _read_from_db(self, key):
        connection = None
        try:
            connection = happybase.Connection(self._hbase_host)
            table = connection.table(self._table_name)
            data = table.row(key)
        except TTransportException as error_message:
            logging.debug(str(error_message))
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
            logging.debug(str(error_message))
        finally:
            connection.close()
        return data

    def get_summary_data(self, application):
        record = {application: {}}
        dm_data = self.get_dm_data(application)
        if dm_data:
            summary_data = self._read_from_db(application)
            if summary_data:
                record[application].update({
                    'aggregate_status': summary_data['cf:aggregate_status']
                })
                record[application].update(json.loads(summary_data['cf:component_data']))
            else:
                record[application].update({
                    'status': 'Not Available'
                })
        else:
            record[application].update({
                'status': 'Not Created'
            })
        return record
