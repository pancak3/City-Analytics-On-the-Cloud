import os
import json
import logging
from tqdm import tqdm
from utils.database import CouchDB
from utils.logger import get_logger
from cloudant.design_document import DesignDocument, Document

logger = get_logger('AreasUpdater', logging.DEBUG)

couch = CouchDB()


def update_areas():
    areas_json = []
    if "areas" in couch.client.all_dbs():
        couch.client["areas"].delete()
    if "areas" not in couch.client.all_dbs():
        filenames = os.listdir("data/LocalGovernmentAreas-2016")
        for filename in filenames:
            abs_path = os.path.join("data/LocalGovernmentAreas-2016", filename)
            if os.path.isfile(abs_path):
                with open(abs_path) as f:
                    areas = json.loads(f.read())['features']
                    for i in range(len(areas)):
                        areas[i]['properties']['states'] = filename[:filename.find('.')]
                    f.close()
                    areas_json += areas

        couch.client.create_database("areas", partitioned=False)
        no_where = {"_id": "australia",
                    "lga2016_area_code": "australia",
                    "lga2016_area_name": "In Australia But No Specific Location"}

        out_of_vitoria = {"_id": "out_of_australia",
                          "lga2016_area_code": "out_of_australia",
                          "lga2016_area_name": "Out of Australia"}

        couch.client["areas"].create_document(no_where)
        couch.client["areas"].create_document(out_of_vitoria)
        for area in tqdm(areas_json, unit=' areas'):
            area["_id"] = area['properties']['feature_code']
            area["lga2016_area_code"] = area['properties']['feature_code']
            area["lga2016_area_name"] = area['properties']['feature_name']
            couch.client["areas"].create_document(area)
    logger.info("Wrote {} areas in to couchdb".format(len(areas_json)))


def check_db(combined_db_name):
    (db_name, partitioned) = combined_db_name.split('.')
    partitioned = True if partitioned == "partitioned" else False
    if db_name not in couch.client.all_dbs():
        couch.client.create_database(db_name, partitioned=partitioned)
        logger.info("Created database: {}".format(db_name))


def ddoc_exists(db_name, ddoc_name):
    db_name = db_name.split('.')[0]
    (ddoc_name, partitioned) = ddoc_name.split('.')
    partitioned = True if partitioned == "partitioned" else False
    design_doc = Document(couch.client[db_name], '_design/' + ddoc_name)
    if not design_doc.exists():
        design_doc = DesignDocument(couch.client[db_name], '_design/' + ddoc_name, partitioned=partitioned)
        design_doc.save()
        logger.info("Created design document: {}".format(ddoc_name))
        return False
    return True


def update_view(db_name, ddoc_name, view_name):
    db_name = db_name.split('.')[0]
    (ddoc_name, partitioned) = ddoc_name.split('.')
    partitioned = True if partitioned == "partitioned" else False
    design_doc = DesignDocument(couch.client[db_name], '_design/' + ddoc_name, partitioned=partitioned)


def check_a_single_view():
    pass


def check_views():
    # TODO check views
    for db_name in os.listdir("couch"):
        if os.path.isdir(os.path.join("couch", db_name)):
            check_db(db_name)
            for ddoc_name in os.listdir(os.path.join("couch", db_name)):
                if os.path.isdir(os.path.join("couch", db_name, ddoc_name)):
                    if ddoc_exists(db_name, ddoc_name):
                        for view_name in os.listdir(os.path.join("couch", db_name, ddoc_name)):
                            if os.path.isdir(os.path.join("couch", db_name, ddoc_name, view_name)):
                                check_a_single_view()


if __name__ == '__main__':
    if not os.path.exists("views_backup"):
        os.mkdir("views_backup")
    # update_areas()
    check_views()
