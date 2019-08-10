from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ConnectionFailure
from pymongo.errors import PyMongoError
from pymongo.errors import DuplicateKeyError
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser

import time
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.core.logging.xpr_log import XprLogger
from enum import Enum, unique


# enumeration class for database operations
@unique
class DBOperation(Enum):
    INSERT = 1
    UPDATE = 2
    REPLACE = 3
    FIND = 4
    DELETE = 5


# design issues: repeated connects are expensive (approx 3 seconds each)
# consequently, once a database connection is established, it should be re-used
# there are 2 ways to achieve this:
# (a) make this class a Singletone
# (b) make the database connection static
# both have repercussions in case of multiple threads
# in general, (b) has been chosen here because only usage of the database connection object needs to be made
# thread-safe,
# as opposed to (a) where each and every method of this class needs to be made thread-safe
# pymongo claims to be thread-safe, hence keeping the connection object static should be OK

class MongoPersistenceManager:
    # code for singleton class
    '''
    class __Singleton:
        def __init__(self):
            pass

        def __str__(self):
            return repr(self) + self.val

    '''
    """
    This class performs various database (CRUD) operations on a Mongo DB database.
    Attributes:
        url (str) - URL of the server
        persistence (str) - name of the database
        uid (str) - user id
        pwd (str) - password
        w (int) - write concern (default = 0)
    """
    config_path = XprConfigParser.DEFAULT_CONFIG_PATH
    config = XprConfigParser(config_path)
    INTERVAL_BETWEEN_RETRIES = int(
        config["mongodb"]["interval_between_retries"])
    MAX_RETRIES = int(config["mongodb"]["max_retries"])
    logger = XprLogger()

    # singleton instance
    # instance = None

    # (static) database connection
    mongo_db = None

    # def __getattr__(self, name):
    #    return getattr(self.instance, name)

    def __init__(self, url: str, db: str, uid: str, pwd: str, w: int = 1):
        """
        Constructor.
        :param url (str): Mongo DB server URL
        :param persistence (str): name of database to perform operations on
        """
        self.logger.info(
            "Entering MongoPersistenceManager constructor with parameters url %s, persistence %s, uid %s, pwd %s, w %s" % (
                url, db, uid, pwd, w))
        self.url = url
        self.db = db
        self.uid = uid
        if len(self.uid) == 0:
            self.uid = None
        self.pwd = pwd
        self.w = w
        self.logger.debug("Created MongoPersistenceManager object successfully")
        self.logger.info("Exiting constructor")

    def connect(self) -> Database:
        """
        Connects to a specific database of a server.
        :return: Mongo Client object and database connection

        """
        self.logger.info("Entering connect method")
        self.logger.debug("Checking if connection already active")
        if self.mongo_db is None:
            self.logger.debug(
                "Attempting connection to database %s on server %s" % (
                    self.db, self.url))
            # connect to server
            mongo_client = MongoClient(host=self.url, w=self.w)
            self.logger.debug("Created Mongo client object")

            # Note Starting with version 3.0 the MongoClient constructor no longer blocks while connecting to the server
            # or servers,
            # and it no longer raises ConnectionFailure if they are unavailable, nor ConfigurationError if the userâ€™s
            # credentials are wrong.
            # Instead, the constructor returns immediately and launches the connection process on background threads.
            # make sure connection has been established
            connected = False
            attempt = 1
            while not connected and attempt <= self.MAX_RETRIES:
                try:
                    # The ismaster command is cheap and does not require auth.
                    self.logger.debug("Checking connection to server")
                    mongo_client.admin.command('ismaster')
                    connected = True
                    self.logger.debug("Connected to server successfully")
                except ConnectionFailure:
                    # TBD: LOG
                    self.logger.debug(
                        "Server not available: waiting for connection. Attempt %s of %s"
                        % (attempt, self.MAX_RETRIES))
                    # wait INTERVAL_BETWEEN_RETRIES seconds and retry MAX_RETRIES times
                    time.sleep(self.INTERVAL_BETWEEN_RETRIES)
                    attempt += 1
            if not connected:
                self.logger.error(
                    "Unable to connect to database after %s attempts" % self.MAX_RETRIES)
                raise UnsuccessfulConnectionException(
                    "Unable to connect to database")

            self.logger.debug("Connected to server successfully")
            # get database pointer
            self.logger.debug("Connecting to database %s" % self.db)
            MongoPersistenceManager.mongo_db = mongo_client[self.db]
            if MongoPersistenceManager.mongo_db is None:
                # TBD: LOG
                raise UnsuccessfulConnectionException(
                    "Unknown database %s" % self.db)

            self.logger.debug(
                "Connected to database successfully. Authenticating user")
            # authenticate user
            try:
                if (self.uid is not None) and (self.pwd is not None):
                    MongoPersistenceManager.mongo_db.authenticate(self.uid,
                                                                  self.pwd)
            except PyMongoError:
                self.logger.debug(("Invalid user ID %s or password" % self.uid))
                raise UnsuccessfulConnectionException(
                    "Invalid user ID %s or password" % self.uid)

            # return database pointer
            self.logger.debug("Authentication successful")
        else:
            self.logger.debug("Connection already active")
        self.logger.info(
            "Exiting connect method with return value %s" % MongoPersistenceManager.mongo_db)
        return MongoPersistenceManager.mongo_db

    def disconnect(self, mongo_client: MongoClient):
        """
        disconnects from the database
        """
        self.logger.info(
            "Entering disconnect method with parameters %s" % mongo_client)
        try:
            # close connection
            # TBD: LOG
            mongo_client.close()
        except ConnectionFailure:
            # do nothing - no point throwing exception if problem closing connection
            self.logger.error(
                "Connection failure while trying to disconnect from %s, %s" % (
                    self.url, self.db))
            pass
        self.logger.debug("Disconnected sucessfully")
        self.logger.info("Exiting disconnect method")

    def insert(self, collection: str, obj, duplicate_ok: bool) -> str:
        """

        :param collection: (string) collection to insert into
        :param obj: (dict) object to be inserted
        :param duplicate_ok: (bool) true if object can be inserted even if it already exists

        :return: ID of the inserted / updated object
        """
        self.logger.info(
            "Entering insert method with parameters collection: %s, obj: %s, duplicate_ok: %s"
            % (collection, obj, duplicate_ok))
        doc_id = None
        # if duplicate_ok == false, check if the object exists
        # assumption: unique index exists in collection, hence no need to check for duplicates
        # duplicate_ok is ignored
        # add_object = duplicate_ok
        ''' if not duplicate_ok:
            self.logger.debug(
                "Duplicates not allowed. Checking if object exists already")
            kwargs = {}
            kwargs["filter"] = obj
            self.logger.debug("Calling perform_db_operation for find operation")
            obj_found = self.perform_db_operation(collection, DBOperation.FIND,
                                                  **kwargs)
            doc_id = -1
            if obj_found is not None:

                try:
                    doc_id = obj_found[0]["_id"]
                    doc_id = -1
                    self.logger.debug("Object exists already. Not inserting")
                except IndexError:
                    add_object = True
                    self.logger.debug("Object does not exist. Inserting")
            else:
                self.logger.debug("Duplicates allowed. Inserting object")
                add_object = True
            obj_found.close()'''
        # if add_object:
        kwargs = obj
        self.logger.debug(
            "Calling perform_db_operation for insert operation")
        result = self.perform_db_operation(collection, DBOperation.INSERT,
                                           **kwargs)
        doc_id = result.inserted_id
        self.logger.debug(
            "Insert successful. New document ID is %s" % doc_id)

        self.logger.info("Exiting insert method with return value %s" % doc_id)
        return doc_id

    def update(self, collection: str, doc_filter, obj, upsert: bool = False):
        """

        :param collection: (string) name of collection to update
        :param doc_filter: filter for the object to be updated
        :param obj: new attributes of the object to be updated
        :param upsert: (bool) true if object to be inserted if not found
        :return: ID of the object updated
        """
        self.logger.info(
            "Entering update method with parameters collection: %s, doc_filter: %s, obj: %s, upsert: %s"
            % (collection, doc_filter, obj, upsert))
        kwargs = {}
        kwargs["filter"] = doc_filter
        update = {}
        update["$set"] = obj
        kwargs["update"] = update
        kwargs["upsert"] = upsert
        self.logger.debug("Calling perform_db_operation for update operation")
        result = self.perform_db_operation(collection, DBOperation.UPDATE,
                                           **kwargs)
        doc_id = result.upserted_id
        self.logger.debug("Update successful. Document ID: %s" % doc_id)

        self.logger.info("Exiting update method with return value %s" % doc_id)
        return doc_id

    def replace(self, collection: str, doc_filter, obj, upsert: bool = False):
        """

        :param collection: (str) name of collection in which to replace document
        :param doc_filter: filter for the object to be replaced
        :param obj: new attributes of the object to be replaced
        :param upsert: (bool) true if object to be inserted if not found
        :return: ID of the object replaced
        """
        self.logger.info(
            "Entering replace method with parameters collection: %s, doc_filter: %s, obj: %s, upsert: %s"
            % (collection, doc_filter, obj, upsert))
        kwargs = {}
        kwargs["filter"] = doc_filter
        kwargs["replacement"] = obj
        kwargs["upsert"] = upsert
        self.logger.debug("Calling perform_db_operation for replace operation")
        result = self.perform_db_operation(collection, DBOperation.REPLACE,
                                           **kwargs)
        doc_id = result.upserted_id
        self.logger.debug("Replace successful. Document ID: %s" % doc_id)

        self.logger.info("Exiting replace method with return value %s" % doc_id)
        return doc_id

    def find(self, collection: str, doc_filter):
        """
        finds one or more documents in the collection matching the specified filter
        :param collection: (str)  to be searched
        :param doc_filter: (dict) query to be applied
        :return: (array of dict) document(s) found, or None
        """
        self.logger.info(
            "Entering fnd method with parameters collection: %s, doc_filter: %s"
            % (collection, doc_filter))
        kwargs = {"filter": doc_filter}
        self.logger.debug("Calling perform_db_operation for find operation")
        result = self.perform_db_operation(collection, DBOperation.FIND,
                                           **kwargs)
        self.logger.debug(
            "Operation completed. Results: %s. Converting to object array" % result)

        # convert result from cursor to array of dict
        final_res = []

        for record in result:
            final_res.append(record)

        result.close()
        self.logger.debug("Conversion complete. Results: %s" % final_res)
        self.logger.info("Exiting find method with return value %s" % final_res)
        return final_res

    def delete(self, collection: str, doc_filter):
        """
        deletes documents from a collection that match the specified filter
        :param collection: (str) collection from which document(s) is/are to be deleted
        :param doc_filter: query to be applied to find documents
        :return: number of documents deleted
        """
        self.logger.info(
            "Entering delete method with parameters collection: %s, doc_filter: %s"
            % (collection, doc_filter))
        kwargs = {}
        kwargs["filter"] = doc_filter
        self.logger.debug("Calling perform_db_operation for delete operation")
        result = self.perform_db_operation(collection, DBOperation.DELETE,
                                           **kwargs)

        self.logger.debug(
            "Operation successful. %s records deleted" % result.deleted_count)
        self.logger.info(
            "Exiting delete method with return value %s" % result.deleted_count)
        return result.deleted_count

    def perform_db_operation(self, collection: str, operation: str, **kwargs):
        """
        performs a database operation - guarantees success within N retries (throws UnsuccessfulOperationException
        if unsuccessful after N retries)
        :param collection: (str) name of collection to be operated on
        :param operation: (str) name of operation to be performed
        :param kwargs: arguments for operation
        :return:
        """
        self.logger.info(
            "Entering perform_db_operation method with parameters collection: %s, operation: %s, "
            "arguments: %s" % (collection, operation, kwargs))
        # connect to the database
        self.logger.debug("Connecting to database")
        mongo_db = self.connect()
        mongo_client = mongo_db.client
        self.logger.debug("Connected to database")

        # get the collection pointer - throw exception if not found
        mongo_collection = mongo_db[collection]
        if mongo_collection is None:
            raise UnsuccessfulOperationException(operation, **kwargs)
        self.logger.debug("Got collection %s" % collection)

        # try the operation - repeat MAX_RETRIES times
        operation_successful = False
        attempt = 1
        result = None
        while not operation_successful and attempt <= self.MAX_RETRIES:
            self.logger.debug("Attempting %s operation. Attempt %s of %s" % (
                operation, attempt, self.MAX_RETRIES))
            try:
                # perform operation
                if operation is DBOperation.INSERT:
                    self.logger.debug("attempting mongo_collection.insert_one")
                    try:
                        result = mongo_collection.insert_one(kwargs)
                        operation_successful = result.acknowledged
                        self.logger.debug(
                            "Attempt complete. Result = %s" % operation_successful)
                    except DuplicateKeyError:
                        print('Duplicate Key Error')
                        operation_successful = False
                        raise UnsuccessfulOperationException(
                            "Insert unsuccessful due to primary key violation")
                elif operation is DBOperation.FIND:
                    self.logger.debug("attempting mongo_collection.find")
                    result = mongo_collection.find(**kwargs)
                    operation_successful = True
                    self.logger.debug(
                        "Attempt complete. Result = %s" % operation_successful)
                elif operation is DBOperation.UPDATE:
                    self.logger.debug("attempting mongo_collection.update_one")
                    result = mongo_collection.update_one(**kwargs)
                    operation_successful = result.acknowledged
                    self.logger.debug(
                        "Attempt complete. Result = %s" % operation_successful)
                elif operation is DBOperation.REPLACE:
                    self.logger.debug("attempting mongo_collection.replace_one")
                    result = mongo_collection.replace_one(**kwargs)
                    operation_successful = result.acknowledged
                    self.logger.debug(
                        "Attempt complete. Result = %s" % operation_successful)
                elif operation is DBOperation.DELETE:
                    self.logger.debug("attempting mongo_collection.delete_many")
                    result = mongo_collection.delete_many(**kwargs)
                    operation_successful = result.acknowledged
                    self.logger.debug(
                        "Attempt complete. Result = %s" % operation_successful)
            except ConnectionFailure:
                # try again after INTERVAL_BETWEEN_RETRIES seconds
                self.logger.debug(
                    "Connection Failure. Trying again after %s seconds" % self.INTERVAL_BETWEEN_RETRIES)
                time.sleep(self.INTERVAL_BETWEEN_RETRIES)
                attempt += 1

        # disconnect from database
        self.logger.debug("Disconnecting from database")
        # self.disconnect(mongo_client)
        self.logger.debug("Disconnected successfully")

        # operation may not have succeeded even after MAX_RETRIES attempts
        if not operation_successful:
            self.logger.error(
                "Operation unsuccessful even after %s attempts" % self.MAX_RETRIES)
            raise UnsuccessfulOperationException(operation, **kwargs)

        self.logger.info(
            "Exiting from perform_db_operation method with return value %s" % result)
        return result
