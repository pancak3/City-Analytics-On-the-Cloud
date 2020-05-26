"""
@author Team 42, Chengdu, China, Qifan Deng, 1077479
"""
import logging
import os
from time import sleep
from cloudant.client import Cloudant
from cloudant.replicator import Replicator
from requests.exceptions import HTTPError
from config import Config
from logger import get_logger

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
    # with open("data/VictoriaSuburb(ABS-2011)Geojson.json") as f:
    #     vic_areas = json.loads(f.read())["features"]
    #
    #     if "areas" not in couch.client.all_dbs():
    #         couch.client.create_database("areas", partitioned=False)
    #         no_where = {"_id": "0",
    #                     "area_code": "0",
    #                     "area_name": "No Where"}
    #
    #         out_of_vitoria = {"_id": "1",
    #                           "area_code": "1",
    #                           "area_name": "Out of Victoria"}
    #
    #         couch.client["areas"].create_document(no_where)
    #         couch.client["areas"].create_document(out_of_vitoria)
    #         for area in vic_areas:
    #             area["_id"] = area['properties']['feature_code']
    #             area["area_code"] = area['properties']['feature_code']
    #             area["area_name"] = area['properties']['feature_name']
    #             couch.client["areas"].create_document(area)

    replicator = Replicator(couch.client)
    source = couch.client['statuses']
    target = couch.client['transfer-statues']
    replicator.create_replication(source_db=source, target_db=target)
    print('done')
