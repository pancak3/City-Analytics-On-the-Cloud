import logging
from cloudant.client import Cloudant
from requests.exceptions import HTTPError
from utils.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Database')
logger.setLevel(logging.DEBUG)


class CouchDB:
    def __init__(self):
        try:
            self.client = Cloudant(config.couch.username, config.couch.password, url=config.couch.url, connect=True)
            self.session = self.client.session()
            logger.debug("[*] CouchDB connected -> {}".format(config.couch.url))
        except HTTPError as e:
            logger.error("[*] CouchDB connecting failed:\n\t{}".format(e))
            exit(1)

    def dump_db(self, db_name, output_path):
        if db_name in self.client.all_dbs():
            f = open(output_path, 'w+')
            for doc in self.client[db_name]:
                f.write(doc.json() + '\n')
            f.close()


if __name__ == '__main__':
    couch = CouchDB()
    couch.dump_db('statues', 'statues.json')
    couch.dump_db('all_users', 'all_users.json')
