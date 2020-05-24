import json
import os
import zipfile
import logging
from utils.database import CouchDB
from utils.logger import get_logger

logger = get_logger('Models', logging.DEBUG)


def load_aurin_data_ier(jsonfile):
    """
    The Index of Economic Resources (IER) focuses on the financial aspects of relative
    socio-economic advantage and disadvantage, by summarising variables related to income
    and wealth. This index excludes education and occupation variables because they are
    not direct measures of economic resources.
    :param jsonfile:
    :return:
    """

    with open(jsonfile, 'r', errors='ignore') as f:
        data = json.load(f)
        for key in data['features']:
            property = key['properties']
            if property['state'] == 'VIC':
                SA2_id = property['sa2_main16']
                pop = property['usual_res_pop']
                ier_score = property['ier_score']
                doc = {'SA2_id': SA2_id, 'population': pop, 'ier_score': ier_score}
                db_ier.save(doc)

    logger.debug("Saved {} results".format(len(doc)))


def load_aurin_data_ieo(jsonfile):
    # load IEO data from AURIN dataset
    with open(jsonfile, 'r', errors='ignore') as f:
        data = json.load(f)
        for key in data['features']:
            property = key['properties']
            if property['state'] == 'VIC':
                SA2_id = property['sa2_main16']
                pop = property['usual_res_pop']
                ieo_score = property['ieo_score']
                doc = {'SA2_id': SA2_id, 'population': pop, 'ieo_score': ieo_score}
                db_ieo.save(doc)

    logger.debug("Saved {} results".format(len(doc)))


def extract_files(compressed_path, extracted_path, required_filenames):
    for required_filename in required_filenames:
        if not os.path.exists(os.path.join(extracted_path, required_filename)):
            zip_file = zipfile.ZipFile(os.path.join(compressed_path, required_filename + ".zip"))
            zip_file.extractall(os.path.join(extracted_path))
            logger.info("Extracted file to {}".format(os.path.join(extracted_path, required_filename)))


def check_dbs(couch_db, db_names):
    for db_name in db_names:
        if db_name not in couch_db.client.all_dbs():
            couch_db.client.create_database(db_name, partitioned=False)
            logger.info("Created database: {}".format(db_name))


def update_db(couch_db, db_name_file_map, extracted_path):
    for db_name, filename in db_name_file_map.items():
        f = open(os.path.join(extracted_path, filename))
        # file_json = json.loads(f.read())["features"]
        file_json = json.loads(f.read())
        f.close()
        bulk = []
        for i, area_data in enumerate(file_json):
            # if 'geometry' in file_json[i]:
            #     del file_json[i]['geometry']
            # file_json[i]['_id'] = area_data['properties']['sa2_main16']
            # file_json[i]['sa2_2016_lv12_code'] = area_data['properties']['sa2_main16']
            # file_json[i]['sa2_2016_lv12_name'] = area_data['properties']['sa2_name16']
            if file_json[i]['_id'] not in couch_db.client[db_name]:
                bulk.append(file_json[i])
        couch_db.client[db_name].bulk_docs(bulk)
        logger.info("Updated database {}".format(db_name))


if __name__ == '__main__':
    compressed_path = "aurin_data/compressed"
    extracted_path = "aurin_data/extracted"
    db_name_file_map = {'aurin_ier': 'Economic_resource.geojson',
                        'aurin_ieo': 'Education_Occupation.geojson'}
    extract_files(compressed_path, extracted_path, db_name_file_map.values())

    couch_db = CouchDB()
    check_dbs(couch_db, db_name_file_map.keys())
    update_db(couch_db, db_name_file_map, extracted_path)
