'''
comp90024 team 42
Qifan Deng
1077479
Zijie Pan
1059454
Mandeep Singh
991857
Steven Tang
832031
26/05/2020
'''
import logging
import json
import os
from time import sleep
from cloudant.client import Cloudant
from cloudant.replicator import Replicator
from requests.exceptions import HTTPError
from utils.config import Config
from utils.logger import get_logger


class CouchDB:
    def __init__(self, log_level):
        self.logger = get_logger('Database', log_level)
        self.config = Config(log_level)
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
                sleep(1)
        os._exit(1)


if __name__ == '__main__':
    pass
