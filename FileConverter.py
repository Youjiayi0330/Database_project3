from BTrees.OOBTree import OOBTree
import json

class FileConverter:
    def _serialize_oobtree(self, oobtree):
        """将 OOBTree 对象转换为字典，以便可以序列化为 JSON。"""
        if isinstance(oobtree, OOBTree):
            return {key: value for key, value in oobtree.items()}
        raise TypeError(f'Object of type {oobtree.__class__.__name__} is not serializable')

    def _deserialize_oobtree(self, data, column_type):
        """从字典数据中重建 OOBTree 对象。"""
        oobtree = OOBTree()
        for key, value in data.items():
            if column_type.upper() == 'INT':
                key = int(key)
            oobtree[key] = value
        return oobtree

    def _default_converter(self, o):
        """定制的 JSON 序列化转换器。"""
        if isinstance(o, OOBTree):
            return self._serialize_oobtree(o)
        raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')

    def write_to_json(self, file_path, data):
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4, default=self._default_converter)

    def read_from_json(self, file_path):
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
        columns = data['columns']
        pk = data['primary_key']
        pk_type = columns[pk]
        data['data'] = self._deserialize_oobtree(data['data'], pk_type)
        for key in data['indexes']:
            key_type = columns[key]
            data['indexes'][key] = self._deserialize_oobtree(data['indexes'][key], key_type)
        return data