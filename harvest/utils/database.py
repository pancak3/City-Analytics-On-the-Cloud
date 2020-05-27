"""
@author Team 42, Chengdu, China, Qifan Deng, 1077479
"""
import os
from time import sleep
from cloudant.client import Cloudant
from requests.exceptions import HTTPError
from utils.config import Config
from utils.logger import get_logger


class CouchDB:
    def __init__(self, log_level):
        """
        initialise a CouchDB object
        :param log_level: log printing level
        """
        self.logger = get_logger('Database', log_level)
        self.config = Config(log_level)
        self.client = None
        self.session = None
        # connect to the database
        self.connect()

    def dump_db(self, db_name, output_path):
        """
        dump database to file
        :param db_name: database name
        :param output_path: output path
        """
        if db_name in self.client.all_dbs():
            f = open(output_path, 'w+')
            for doc in self.client[db_name]:
                f.write(doc.json() + '\n')
            f.close()

    def connect(self):
        """
        connect to the database
        """
        count = 5
        while count:
            self.logger.debug("[*] Connecting to CouchDB -> {}".format(self.config.couch.url))
            try:
                self.client = Cloudant(self.config.couch.username, self.config.couch.password,
                                       url=self.config.couch.url, connect=True)
                self.client.connect()
                self.client.clear()
                self.logger.debug("[*] CouchDB connected -> {}".format(self.config.couch.url))
                return
            except HTTPError as e:
                self.logger.error("[*] CouchDB connecting failed:\n\t{}".format(e))
                count -= 1
                sleep(1)
        # if failed 5 times
        os._exit(1)


if __name__ == '__main__':
    pass
