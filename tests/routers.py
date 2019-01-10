
class SecondaryRouter:
    def db_for_read(self, model, **hints):
        if model.__name__.lower().startswith('secondary'):
            return 'secondary'

    def db_for_write(self, model, **hints):
        if model.__name__.lower().startswith('secondary'):
            return 'secondary'

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_migrate(self, db, app_label, model=None, **hints):
        if model and model.__name__.lower().startswith('secondary'):
            return db == 'secondary'
        else:
            return False if db == 'secondary' else None
