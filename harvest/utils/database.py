import logging
from cloudant.client import Cloudant
from config import config
from requests.exceptions import HTTPError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Database')
logger.setLevel(logging.DEBUG)


class CouchDB:

    def __init__(self):
        try:
            self.client = Cloudant(config.couch.username, config.couch.password, url=config.couch.url, connect=True)
        except HTTPError as e:
            logger.error("[*] CouchDB connecting failed:\n\t{}".format(e))
        finally:
            logger.debug("[*] CouchDB connected -> {}".format(config.couch.url))


if __name__ == '__main__':
    couch = CouchDB()
