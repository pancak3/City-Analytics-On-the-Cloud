import json
import couchdb
import logging
from utils.database import CouchDB
from utils.logger import get_logger

logger = get_logger('Models', logging.DEBUG)

couch_db = CouchDB()
ier_file = 'AURIN_data/Economic_resource.geojson'
ieo_file = 'AURIN_data/Education_Occupation.geojson'
db_names = ['aurin_ier', 'aurin_ieo']
for db_name in db_names:
    if db_name not in couch_db.client.all_dbs():
        couch_db.client.create_database(db_name)


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


def loadAURINdata_ieo(jsonfile):
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


load_aurin_data_ier(ier_file)
load_aurin_data_ier(ieo_file)
