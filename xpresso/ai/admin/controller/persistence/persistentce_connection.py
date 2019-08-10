""" This contains helper for database operations """

from xpresso.ai.admin.controller.persistence.mongopersistencemanager import \
    MongoPersistenceManager

__author__ = ["Naveen Sinha"]


MONGO_SECTION = 'mongodb'
URL = 'mongo_url'
DB = 'database'
UID = 'mongo_uid'
PWD = 'mongo_pwd'
W = 'w'


def create_persistence_object(config):
    """
    Creates a persistence manager object

    Args:
        config(XprConfigParser): object of XprConfigParser

    Returns:
        object of connected persistence manager
    """
    mongo_persistence_manager = MongoPersistenceManager(
        url=config[MONGO_SECTION][URL],
        db=config[MONGO_SECTION][DB],
        uid=config[MONGO_SECTION][UID],
        pwd=config[MONGO_SECTION][PWD],
        w=config[MONGO_SECTION][W])
    return mongo_persistence_manager
