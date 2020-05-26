"""
@author Team 42, Chengdu, China, Qifan Deng, 1077479
"""
import json
import threading
import queue
from tqdm import tqdm
from cloudant.client import Cloudant

q = queue.Queue()
flag = False


def get():
    global flag
    source_couch = Cloudant('comp90024', 'pojeinaiShoh9Oo', url='http://127.0.0.1:5984', connect=True)
    # source_couch = Cloudant('admin', 'password', url='http://127.0.0.1:5984/', connect=True)
    source_couch.connect()
    for doc in tqdm(source_couch['statuses'], total=source_couch['statuses'].doc_count()):
        doc_json = json.dumps(doc)
        q.put(doc_json)
    flag = True


def write():
    global flag
    while True:
        f = open("statuses.json", "a+")
        doc = q.get()
        doc_json = json.dumps(doc)
        f.write(doc_json + '\n')
        if q.empty() and flag:
            break
        f.close()


threading.Thread(target=get).start()
threading.Thread(target=write).start()
