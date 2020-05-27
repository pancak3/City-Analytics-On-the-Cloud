"""
@author Team 42, Chengdu, China, Qifan Deng, 1077479
"""
from tqdm import tqdm
from cloudant.client import Cloudant


def fix_err():
    """
    Fix the wrong id in couch db caused by :
    https://developer.twitter.com/en/docs/basics/twitter-ids
    """
    # initial the connection to CouchDB
    couch = Cloudant('admin', 'password', url='http://127.0.0.1:5984', connect=True)
    old_statuses = couch['statuses']
    bulk = []
    for doc in tqdm(old_statuses, total=old_statuses.doc_count()):
        # iterate all the tweets at this run
        if doc['_id'][0] == '_':
            # if its design documents which begins with "_design/"
            continue
        # get the old partition id which is right
        old_partition_id = doc["_id"][:doc["_id"].find(':')]
        # get the tweet id in str which is correct id
        old_doc_id_str = doc['id_str']
        # delete them from document fields
        if 'id' in doc:
            del doc['id']
        if '_rev' in doc:
            del doc['_rev']
        # create new documents id with tweet id_str and the origin partition id
        doc['_id'] = old_partition_id + ":" + old_doc_id_str
        bulk.append(doc)
        if len(bulk) >= 100:
            # perform multiple documents insertion
            couch['new_statuses'].bulk_docs(bulk)
            bulk = []

    if len(bulk):
        # in case the bulk is not be handled
        couch['new_statuses'].bulk_docs(bulk)


if __name__ == '__main__':
    fix_err()
