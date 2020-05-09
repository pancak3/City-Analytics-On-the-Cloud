import logging
from cloudant.client import Cloudant
from cloudant.replicator import Replicator
from requests.exceptions import HTTPError
from utils.config import config
from utils.logger import get_logger

logger = get_logger('Database', logging.DEBUG)


class CouchDB:
    def __init__(self):
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
        logger.debug("[*] Connecting to CouchDB -> {}".format(config.couch.url))
        try:
            self.client = Cloudant(config.couch.username, config.couch.password, url=config.couch.url, connect=True)
            self.session = self.client.session()
            self.client.connect()
            logger.debug("[*] CouchDB connected -> {}".format(config.couch.url))
        except HTTPError as e:
            logger.error("[*] CouchDB connecting failed:\n\t{}".format(e))
            exit(1)


if __name__ == '__main__':
    couch = CouchDB()
    # couch.dump_db('statues', 'statues.json')
    # couch.dump_db('all_users', 'all_users.json')
    # for user in couch.client['all_users']:
    #     user['timeline_updated_at'] = 0
    #     user.save()
    # for user in couch.client['stream_users']:
    #     # user['friends_updated_at'] = 0
    #     user['timeline_updated_at'] = 0
    #     user.save()
    # replication = Replicator(couch.client)
    # replication.create_replication(couch.client['statues'], couch.client['statuses'])
    # print('done')
