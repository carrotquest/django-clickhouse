from infi import clickhouse_orm


class HintParamMixin:
    def __init__(self, *args, hints=None, **kwargs):
        if hints is None:
            hints = dict()
        self.hints = hints
        super(HintParamMixin, self).__init__(*args, **kwargs)


class CreateTable(HintParamMixin, clickhouse_orm.CreateTable):
    pass


class AlterTable(HintParamMixin, clickhouse_orm.AlterTable):
    pass


class AlterTableWithBuffer(HintParamMixin, clickhouse_orm.AlterTableWithBuffer):
    pass


class DropTable(HintParamMixin, clickhouse_orm.DropTable):
    pass


class AlterConstraints(HintParamMixin, clickhouse_orm.AlterConstraints):
    pass


class AlterIndexes(HintParamMixin, clickhouse_orm.AlterIndexes):
    pass


class RunPython(HintParamMixin, clickhouse_orm.RunPython):
    pass


class RunSQL(HintParamMixin, clickhouse_orm.RunSQL):
    pass
