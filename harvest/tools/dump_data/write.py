"""
@author Team 42, Chengdu, China, Qifan Deng, 1077479
"""
import json

from tqdm import tqdm
from cloudant.client import Cloudant

# target_couch = Cloudant('comp90024', 'pojeinaiShoh9Oo', url='http://127.0.0.1:5984', connect=True)
target_couch = Cloudant('admin', 'password', url='http://127.0.0.1:5984/', connect=True)
target_couch.connect()
client = target_couch['statusessss']

f = open("statuses.json", "w+")
statuses = f.readlines()
f.close()

bulk = []
for doc in tqdm(statuses):
    bulk.append(json.loads(doc))
    if len(bulk) > 500:
        client.clientbulk_docs(bulk)
        bulk = []

if len(bulk):
    client.bulk_docs(bulk)
print("[*] recovered")
