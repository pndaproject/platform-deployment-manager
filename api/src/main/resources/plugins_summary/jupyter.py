from plugins_summary.component_summary import ComponentSummary

class JupyterComponentSummary(ComponentSummary):

    def get_component_type(self):
        return 'jupyter'

    def get_component_summary(self, component, application):
        return {}
