__all__ = ['XprDbSetup']
__author__ = 'Sahil Malav'

import time
import configparser
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.core.utils import linux_utils
from pymongo import MongoClient, ASCENDING
from passlib.hash import sha512_crypt


class XprDbSetup:
    """
        Class that provides tools to setup mongodb on a node
    """

    def __init__(self, executor=None):
        if not executor:
            self.executor = LocalShellExecutor()
        self.logger = XprLogger()
        self.service_path = '/lib/systemd/system/mongod.service'

    def install_mongo(self):
        """
        installs mongodb on the system
        """
        self.logger.info('entering install_mongo method')
        if not linux_utils.check_root():
            self.logger.fatal("Please run this as root")
        import_key = 'sudo apt-key adv --keyserver ' \
                     'hkp://keyserver.ubuntu.com:80 --recv ' \
                     '9DA31620334BD75D9DCB49F368818C72E52529D4'
        self.executor.execute(import_key)
        create_list = 'echo "deb [ arch=amd64 ] https://repo.mongodb.org/' \
                      'apt/ubuntu bionic/mongodb-org/4.0 multiverse" | ' \
                      'sudo tee /etc/apt/sources.list.d/mongodb-org-4.0.list'
        self.executor.execute(create_list)
        reload_packages = 'sudo apt-get update'
        self.executor.execute(reload_packages)
        self.logger.debug('installing mongo')
        install_mongo = 'sudo apt-get install -y mongodb-org'
        self.executor.execute(install_mongo)
        hold = """echo "mongodb-org hold" | sudo dpkg --set-selections
                  echo "mongodb-org-server hold" | sudo dpkg --set-selections
                  echo "mongodb-org-shell hold" | sudo dpkg --set-selections
                  echo "mongodb-org-mongos hold" | sudo dpkg --set-selections
                  echo "mongodb-org-tools hold" | sudo dpkg --set-selections"""
        self.executor.execute(hold)
        self.logger.info('exiting install_mongo')

    def initial_setup(self, db):
        """
        sets up the initial users and collections in the db
        :param db: database against which the setup is to be done
        :return: nothing
        """
        self.logger.info('entering initial_setup method')
        # initiate users collection
        users = db.users
        self.insert_default_users(users)
        db.users.create_index([('uid', ASCENDING)], unique=True)
        self.logger.debug('created index for users collection')

        # initiate nodes collection
        nodes = db.nodes
        self.logger.debug('setting up initial node')
        initial_node = {
            "name": "initial_node",
            "address": ""
        }
        nodes.insert_one(initial_node)
        nodes.create_index([('address', ASCENDING)], unique=True)
        self.logger.debug('created index for nodes collection')
        nodes.delete_one({"name": "initial_node"})

        # initiate clusters collection
        clusters = db.clusters
        self.logger.debug('setting up initial cluster')
        initial_cluster = {
            "name": "initial_cluster",
            "activationStatus": True,
            "master_nodes": [],
            "worker_nodes": []
        }
        clusters.insert_one(initial_cluster)
        clusters.create_index([('name', ASCENDING)], unique=True)
        self.logger.debug('created index for clusters collection')
        clusters.delete_one({"name": "initial_cluster"})

        # initiate projects collection
        projects = db.projects
        self.logger.debug('setting up initial project')
        initial_project = {
            "name": "initial_project",
            "projectDescription": "Initiates the collection",
            "owner": {},
            "developers": [],
            "components": []
        }
        projects.insert_one(initial_project)
        projects.create_index([('name', ASCENDING)], unique=True)
        self.logger.debug('created index for projects collection')
        projects.delete_one({"name": "initial_project"})

        # create xprdb_admin user in mongo
        self.logger.debug('creating xprdb user in mongo')
        db.command("createUser", "xprdb_admin", pwd="xprdb@Abz00ba",
                   roles=[{"role": "root", "db": "admin"}])
        self.logger.info('exiting initial_setup method')

    def insert_default_users(self, users):
        self.logger.debug('setting up default users')
        admin_user = {
            "uid": "xprdb_admin",
            "firstName": "Xpresso",
            "lastName": "Admin",
            "pwd": sha512_crypt.hash('xprdb@Abz00ba'),
            "email": "xprdb_admin@abzooba.com",
            "primaryRole": "Admin",
            "activationStatus": True,
            "loginStatus": False
        }
        users.insert_one(admin_user)
        superuser = {
            "uid": "superuser1",
            "firstName": "superuser1",
            "lastName": "superuser1",
            "pwd": sha512_crypt.hash('superuser1'),
            "email": "superuser1@abzooba.com",
            "primaryRole": "Su",
            "activationStatus": True,
            "loginStatus": False
        }
        users.insert_one(superuser)
        admin1_user = {
            "uid": "admin1",
            "firstName": "admin1",
            "lastName": "admin1",
            "pwd": sha512_crypt.hash('admin1'),
            "email": "admin1@abzooba.com",
            "primaryRole": "Admin",
            "activationStatus": True,
            "loginStatus": False
        }
        users.insert_one(admin1_user)

    def enable_replication(self):
        """
        installs replica set for the database
        :return: nothing
        """
        self.logger.info('entering enable_replication method')
        path = '/srv/mongodb/rs0-0'
        linux_utils.create_directory(path, 0o777)
        self.logger.debug('created directory for replica set')
        ip = linux_utils.get_ip_address()
        start = 'mongod --replSet rs0 --port 27017 --bind_ip localhost,' \
                '{} --dbpath /srv/mongodb/rs0-0 --fork ' \
                '--logpath /var/log/mongodb/mongod.log'.format(ip)
        self.executor.execute(start)
        self.logger.debug('mongo daemon started')
        client = MongoClient('localhost', replicaset='rs0')
        db = client.xprdb
        client.admin.command("replSetInitiate")
        self.logger.debug('Replica set initiated')
        time.sleep(5)
        self.initial_setup(db)
        # stop mongo to restart with auth
        stop_mongod = 'pgrep mongod | xargs kill'
        self.executor.execute(stop_mongod)
        self.logger.debug('stopping mongo daemon to restart with auth')
        time.sleep(10)
        restart = 'mongod --replSet rs0 --port 27017 --bind_ip localhost,{} ' \
                  '--dbpath /srv/mongodb/rs0-0 --auth --fork --logpath ' \
                  '/var/log/mongodb/mongod.log'.format(ip)
        config = configparser.ConfigParser()
        config.read(self.service_path)
        config['Service']['ExecStart'] = restart
        with open(self.service_path, 'w') as f:
            config.write(f)
        restart_mongod = 'systemctl restart mongod'
        self.executor.execute(restart_mongod)
        self.logger.debug('db setup complete, exiting enable_replication')


if __name__ == "__main__":
    XprDbSetup().install_mongo()
    XprDbSetup().enable_replication()
