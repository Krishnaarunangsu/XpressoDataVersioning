__all__ = ['XprDbMigration']
__author__ = 'Sahil Malav'


import json
from pymongo import MongoClient
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class XprDbMigration:
    """
    class to facilitate mongodb migration
    """
    add = 'add'
    remove = 'remove'
    rename = 'rename'
    current = 'current'

    def __init__(self):
        self.logger = XprLogger()
        # script is supposed to be run on the VM itself, so host is localhost
        client = MongoClient('localhost', replicaset='rs0')
        self.db = client.xprdb
        self.db.authenticate('xprdb_admin', 'xprdb@Abz00ba')
        config_path = XprConfigParser.DEFAULT_CONFIG_PATH
        config = XprConfigParser(config_path)
        MONGO = 'mongodb'
        FILEPATH = 'formats_file'
        self.path = config[MONGO][FILEPATH]
        with open(self.path, 'r') as file:
            self.data = json.loads(file.read())

    def add_field(self, collection_name):
        """
        Adds specified field(s) to each document in the provided collection.
        Format to be used (against appropriate collection key) in
        mongo_collection_formats.json file is :
            "add": [{"key1": <key1>, "value1": <value1>},
                    {"key2": <key2>, "value2": <value2>}]
        Args:
            collection_name: collection name:
        Returns:
            nothing
        """
        if not collection_name:
            self.logger.error('No collection name specified.')
            print('ERROR : No collection name specified.')
            return
        collection = self.data[collection_name]
        if not collection[self.add]:
            self.logger.error('No field specified to add')
            print('ERROR : No field specified to add.')
            return
        coll = self.db[collection_name]
        for field in collection[self.add]:
            if not field['key'] or not field['value']:
                self.logger.error('"key" and/or "value" not specified.')
                print('ERROR : "key" and/or "value" not specified.')
                return
            coll.update_many({}, {"$set": {field['key']: field['value']}})
            print('Completed.')
            with open(self.path, 'w') as f:
                self.data[collection_name][self.current].append(field['key'])
                self.data[collection_name][self.add].remove(field)
                f.write(json.dumps(self.data, indent=5))

    def remove_field(self, collection_name):
        """
        Removes specified field(s) from each document in the provided
        collection. Format to be used (against appropriate collection key) in
        mongo_collection_formats.json file is :
            "remove": [<field1>, <field2>]
        field values have to be strings.
        Args:
            collection_name: collection name
        Returns:
            nothing

        """
        if not collection_name:
            self.logger.error('No collection name specified.')
            print('ERROR : No collection name specified.')
            return
        collection = self.data[collection_name]
        if not collection['remove']:
            self.logger.error('No field specified to remove')
            print('ERROR : No field specified to remove.')
            return
        coll = self.db[collection_name]
        for field in collection[self.remove]:
            coll.update_many({}, {"$unset": {field: 1}})
            print('Completed.')
            with open(self.path, 'w') as f:
                if field in self.data[collection_name][self.current]:
                    self.data[collection_name][self.current].remove(field)
                self.data[collection_name][self.remove].remove(field)
                f.write(json.dumps(self.data, indent=5))

    def rename_field(self, collection_name):
        """
        Renames specified field(s) in each document in the provided
        collection. Format to be used (against appropriate collection key) in
        mongo_collection_formats.json file is :
            "rename": [
               {
                    "old": "<old field name>",
                    "new": "<new field name>"
               }
          ]
        field values have to be strings.
        Args:
            collection_name: collection name
        Returns:
            nothing
        """
        if not collection_name:
            self.logger.error('No collection name specified.')
            print('ERROR : No collection name specified.')
            return
        collection = self.data[collection_name]
        if not collection['rename']:
            self.logger.error('No field specified to rename')
            print('ERROR : No field specified to rename.')
            return
        coll = self.db[collection_name]
        for field in collection[self.rename]:
            if not field['old'] or not field['new']:
                self.logger.error('"old" and/or "new" field not specified.')
                print('ERROR : "old" and/or "new" field not specified.')
                return
            coll.update_many({}, {"$rename": {field['old']: field['new']}})
            print('Completed.')
            with open(self.path, 'w') as f:
                if field['old'] in self.data[collection_name][self.current]:
                    self.data[collection_name][self.current].remove(field['old'])
                self.data[collection_name][self.current].append(field['new'])
                self.data[collection_name][self.rename].remove(field)
                f.write(json.dumps(self.data, indent=5))


if __name__ == "__main__":
    # It is recommended to run the methods in this order to avoid any clashes
    XprDbMigration().add_field('users')
    XprDbMigration().rename_field('users')
    XprDbMigration().remove_field('users')