class ComponentSummaryAggregator(object):
    def get_application_summary(self, app_name, component_details):
        (ret_data, comp_data) = ({}, {})
        for _, component_data in component_details.items():
            comp_data.update(component_data['component_ref'].get_components_summary(app_name, \
                component_data['component_data']))
        ret_data[app_name] = comp_data
        ret_data[app_name].update({'aggregate_status': self.process_application_data(comp_data)})
        return ret_data

    def process_application_data(self, application):
        """
        Find Application level aggregated status based on it's component's status
        """
        aggregated_status_priority = dict([(1, 'CREATED'), \
        (2, 'STARTING'), (3, "RUNNING"), (4, 'RUNNING_WITH_ERRORS'), \
        (5, 'STOPPED'), (6, 'STOPPED_WITH_FAILURES'), (7, 'KILLED'), (8, 'KILLED_WITH_FAILURES'), \
        (9, 'FAILED'), (10, 'COMPLETED'), (11, 'COMPLETED_WITH_FAILURES'), (12, 'NOT_FOUND')])
        current_agg_status_priority = 0

        for component in application:
            temp_status_priority = self._get_component_priority(application, component)
            if temp_status_priority > current_agg_status_priority:
                current_agg_status_priority = temp_status_priority

        return aggregated_status_priority[current_agg_status_priority]

    def _get_component_priority(self, application, component):
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
        elif temp_comp_status == 'FAILED':
            temp_status_priority = 9
        elif temp_comp_status == 'COMPLETED' or temp_comp_status == 'FINISHED_SUCCEEDED' \
                or temp_comp_status == 'SUCCEEDED':
            temp_status_priority = 10
        elif temp_comp_status == 'COMPLETED_WITH_FAILURES' or temp_comp_status == 'FINISHED_FAILED':
            temp_status_priority = 11
        else:
            temp_status_priority = 12
        return temp_status_priority
