"""
@author Team 42, Chengdu, China, Qifan Deng, 1077479
"""
import logging
import os
from time import sleep
from cloudant.client import Cloudant
from requests.exceptions import HTTPError
from utils.config import Config
from utils.logger import get_logger

logger = get_logger('Database', logging.DEBUG)


class CouchDB:
    def __init__(self):
        self.config = Config()
        self.client = None
        self.session = None
        self.connect()

    def dump_db(self, db_name, output_path):
        if db_name in self.client.all_dbs():
            f = open(output_path, 'w+')
            for doc in self.client[db_name]:
                f.write(doc.json() + '\n')
            f.close()

    def connect(self):
        count = 5
        while count:
            logger.debug("[*] Connecting to CouchDB -> {}".format(self.config.couch.url))
            try:
                self.client = Cloudant(self.config.couch.username, self.config.couch.password,
                                       url=self.config.couch.url, connect=True)
                self.client.connect()
                logger.debug("[*] CouchDB connected -> {}".format(self.config.couch.url))
                return
            except HTTPError as e:
                logger.error("[*] CouchDB connecting failed:\n\t{}".format(e))
                sleep(1)
        os._exit(1)


if __name__ == '__main__':
    couch = CouchDB()
    # couch.dump_db('statues', 'statues.json')
    # couch.dump_db('all_users', 'all_users.json')
    # for user in couch.client['stream_users']:
    #     # user['friends_updated_at'] = 0
    #     user['timeline_updated_at'] = 0
    #     user.save()
    # replication = Replicator(couch.client)
    # if 'users' not in couch.client.all_dbs():
    #     couch.client.create_database('users')
    # replication.create_replication(couch.client['all_users'], couch.client['users'])
    print('done')
