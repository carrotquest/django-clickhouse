from .configuration import PREFIX


class ConfigurationError(Exception):
    def __init__(self, param_name):
        param_name = PREFIX + param_name
        super(ConfigurationError, self).__init__("Config parameter '%s' is not set properly" % param_name)


class DBAliasError(Exception):
    def __init__(self, alias):
        super(DBAliasError, self).__init__(
            "Database alias `%s` is not found. Check %s parameter" % (alias, PREFIX + 'DATABASES'))


class RedisLockTimeoutError(Exception):
    pass
