@receiver(post_migrate)
def clickhouse_migrate(sender, **kwargs):
    if getattr(settings, 'UNIT_TEST', False):
        # Не надо мигрировать ClickHouse при каждом UnitTest
        # Это сделает один раз система тестирования
        return

    if kwargs.get('using', 'default') != 'default':
        # Не надо выполнять синхронизацию для каждого шарда. Только один раз.
        return

    app_name = kwargs['app_config'].name

    package_name = "%s.%s" % (app_name, 'ch_migrations')
    if importlib.util.find_spec(package_name):
        settings.CLICKHOUSE_DB.migrate(package_name)
        print('\033[94mMigrated ClickHouse models for app "%s"\033[0m' % app_name)