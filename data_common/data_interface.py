import abc


class DataInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def save(self, table, obj):
        pass

    @abc.abstractmethod
    def get(self, table, obj):
        pass

    @abc.abstractmethod
    def get_items(self, table, query):
        pass

    @abc.abstractmethod
    def save_minimal(self, table, obj):
        pass

    @abc.abstractmethod
    def atomic_update(self, table, key, update_expression, express_attr_values):
        pass

    @abc.abstractmethod
    def get_by_user_id(self, table, user_id):
        pass

    @abc.abstractmethod
    def get_all_items(self, table):
        pass

    @abc.abstractmethod
    def get_by_version(self, table, entity_id, version):
        pass
