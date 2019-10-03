import json
import unittest
from mock import Mock, patch
from application_summary_registrar import HBaseAppplicationSummary

class AppplicationSummaryRegistrarTests(unittest.TestCase):
    @patch('happybase.Connection.table.scan', return_value=['app1', 'app2', 'app3'])
    @patch('happybase.Connection')
    def test_sync_with_dm(self, hbase_mock, dm_app_list):
        """
        Testing deleted aplication get removed from Hbase
        """
        registrar = HBaseAppplicationSummary('1.2.3.4')
        application_list = ['app1', 'app2']
        registrar.sync_with_dm(application_list)
        for application, _ in dm_app_list:
            if application not in application_list:
                hbase_mock.return_value.table.return_value.delete.assert_called_once_with(application)

    @patch('happybase.Connection')
    def test_post_to_hbase(self, hbase_mock):
        """
        Testing Summary data ets posted to Hbase
        """
        registrar = HBaseAppplicationSummary('1.2.3.4')
        registrar.post_to_hbase({'aname': {'aggregate_status': 'status', 'component-1': 'data'}}, 'aname')
        hbase_mock.return_value.table.return_value.put.assert_called_once_with('aname', \
        {b'cf:component_data': json.dumps({'component-1': 'data'}), b'cf:aggregate_status': 'status'})

    @patch('happybase.Connection')
    def test_get_summary_data(self, hbase_mock):
        """
        Test get summary data
        """
        #In case of application have data in platform_application_summary and platform_applications
        registrar = HBaseAppplicationSummary('1.2.3.4')
        registrar.get_dm_data = Mock(return_value={b'cf:create_data': '{"create": "data"}', })
        hbase_mock.return_value.table.return_value.row.return_value = {b'cf:component_data': \
        json.dumps({"component-1": "data"}), b'cf:aggregate_status': 'status'}
        result = registrar.get_summary_data('name')
        self.assertEqual(result, {'name': {'aggregate_status': 'status', 'component-1': 'data'}})

        #In case of application have data only in platform_applications not in platform_application_summary
        registrar.get_dm_data = Mock(return_value={b'cf:create_data': '{"create": "data"}', })
        hbase_mock.return_value.table.return_value.row.return_value = {}
        result = registrar.get_summary_data('name')
        self.assertEqual(result, {'name': {'status': 'Not Available'}})

        #In case of application have data not in platform_applications itself
        registrar.get_dm_data = Mock(return_value={})
        result = registrar.get_summary_data('name')
        self.assertEqual(result, {'name': {'status': 'Not Created'}})
