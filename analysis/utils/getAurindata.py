import json
import couchdb
import logging
from utils.logger import get_logger

logger = get_logger('Models', logging.DEBUG)


couch = couchdb.Server('https://comp90024:pojeinaiShoh9Oo@comp90024.steven.cf')
ierfile = 'AURIN_data/Economic_resource.geojson'
ieofile = 'AURIN_data/Education_Occupation.geojson'
db_ier = couch.create('aurindata_ier')
db_ieo = couch.create('aurindata_ieo')


def loadAURINdata_ier(jsonfile):
    '''
    The Index of Economic Resources (IER) focuses on the financial aspects of relative
    socio-economic advantage and disadvantage, by summarising variables related to income
    and wealth. This index excludes education and occupation variables because they are
    not direct measures of economic resources.
    '''
    with open(jsonfile, 'r', errors = 'ignore') as f:
        data = json.load(f)
        for key in data['features']:
            property = key['properties']
            if property['state'] == 'VIC':
                SA2_id = property['sa2_main16']
                pop = property['usual_res_pop']
                ier_score = property['ier_score']
                doc = {'SA2_id': SA2_id, 'population':pop, 'ier_score':ier_score}
                db_ier.save(doc)

    logger.debug("Saved {} results".format(len(doc)))



def loadAURINdata_ieo(jsonfile):
    #load IEO data from AURIN dataset
    with open(jsonfile, 'r', errors = 'ignore') as f:
        data = json.load(f)
        for key in data['features']:
            property = key['properties']
            if property['state'] == 'VIC':
                SA2_id = property['sa2_main16']
                pop = property['usual_res_pop']
                ieo_score = property['ieo_score']
                doc = {'SA2_id': SA2_id, 'population':pop, 'ieo_score':ieo_score}
                db_ieo.save(doc)

    logger.debug("Saved {} results".format(len(doc)))

loadAURINdata_ier(ierfile)
loadAURINdata_ier(ieofile)
