from .configuration import PREFIX

class ClickHouseError(Exception):
    pass


class ConfigurationError(Exception):
    def __init__(self, param_name):
        param_name = PREFIX + param_name
        super(ConfigurationError, self).__init__("Config parameter '%s' is not set properly" % param_name)
