from tqdm import tqdm
from cloudant.client import Cloudant


def fix_err():
    couch = Cloudant('admin', 'password', url='http://127.0.0.1:5984', connect=True)
    old_statuses = couch['statuses']
    bulk = []
    for doc in tqdm(old_statuses, total=old_statuses.doc_count()):
        if doc['_id'][0] == '_':
            continue
        old_partition_id = doc["_id"][:doc["_id"].find(':')]
        old_doc_id_str = doc['id_str']
        if 'id' in doc:
            del doc['id']
        if '_rev' in doc:
            del doc['_rev']
        doc['_id'] = old_partition_id + ":" + old_doc_id_str
        bulk.append(doc)
        if len(bulk) >= 100:
            couch['new_statuses'].bulk_docs(bulk)
            bulk = []

    if len(bulk):
        couch['new_statuses'].bulk_docs(bulk)


if __name__ == '__main__':
    fix_err()
