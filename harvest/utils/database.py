import logging
import json
import os
from time import sleep
from cloudant.client import Cloudant
from cloudant.replicator import Replicator
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
                self.client.clear()
                logger.debug("[*] CouchDB connected -> {}".format(self.config.couch.url))
                return
            except HTTPError as e:
                logger.error("[*] CouchDB connecting failed:\n\t{}".format(e))
                sleep(1)
        os._exit(1)


if __name__ == '__main__':
    couch = CouchDB()

    if "areas" in couch.client.all_dbs():
        couch.client["areas"].delete()
    if "areas" not in couch.client.all_dbs():
        config = Config()
        areas_json = []
        filenames = os.listdir(config.australia_lga2016_path)
        for filename in filenames:
            abs_path = os.path.join(config.australia_lga2016_path, filename)
            if os.path.isfile(abs_path):
                with open(abs_path) as f:
                    areas = json.loads(f.read())['features']
                    for i in range(len(areas)):
                        areas[i]['properties']['states'] = filename[:filename.find('.')]
                    f.close()
                    areas_json += areas

        # f = open("all.json", "w+")
        # f.write(json.dumps(areas_json))
        # f.close()
        # exit()
        couch.client.create_database("areas", partitioned=False)
        no_where = {"_id": "0",
                    "lga2016_area_code": "0",
                    "lga2016_area_name": "No Where"}

        out_of_vitoria = {"_id": "1",
                          "lga2016_area_code": "1",
                          "lga2016_area_name": "Out of Victoria"}

        couch.client["areas"].create_document(no_where)
        couch.client["areas"].create_document(out_of_vitoria)
        for area in areas_json:
            area["_id"] = area['properties']['feature_code']
            area["lga2016_area_code"] = area['properties']['feature_code']
            area["lga2016_area_name"] = area['properties']['feature_name']
            couch.client["areas"].create_document(area)

    # replicator = Replicator(couch.client)
    # source = couch.client['statuses']
    # target = couch.client['transfer-statues']
    # replicator.create_replication(source_db=source, target_db=target)
    print('done')
