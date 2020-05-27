"""
@author Team 42, Chengdu, China, Qifan Deng, 1077479
"""
import queue
import threading
import logging
from tqdm import tqdm
from time import sleep, time
from utils.crawlers import Crawler
from cloudant.client import Cloudant

q = queue.Queue()
couch = Cloudant('comp90024', 'pojeinaiShoh9Oo', url='http://127.0.0.1:5984', connect=True)
fix_db = couch['statuses']
flag = True


def worker():
    """
    save fixed data to couch db
    """
    global q, fix_db, flag
    bulk = []
    while flag or not q.empty():
        # Get document from queue and  performs multiple document inserts
        # and/or updates through a single request
        try:
            doc = q.get(timeout=1)
            bulk.append(doc)
            if len(bulk) > 100:
                fix_db.bulk_docs(bulk)
                bulk = []
        except Exception:
            pass

    if len(bulk):
        fix_db.bulk_docs(bulk)
    print("Finished fixing full text")


def fix_full():
    """
    fix the old data which does not have full text of tweets
    """
    global q, fix_db, flag
    # initialise a crawler
    crawler = Crawler(logging.DEBUG)
    for item in crawler.api_keys.items():
        # take the first api key
        crawler.init(item[0], 0)
        break

    # get unfinished full text task from the view
    statuses = fix_db.get_view_result('_design/task', view_name='full-text', reduce=False).all()

    id_str_list = []
    id_map = {}
    access_time = time()
    for status in tqdm(statuses, desc="Fix full text"):
        # handle each task
        id_str_list.append(status['key'])
        id_map[status['key']] = status['id']
        if len(id_str_list) >= 100:
            # if got more than 100 raw docs from database
            now_time = time()
            if now_time - access_time < 1:
                # block if the previous api access is less than 1 second
                # for the sake of error about rate limits
                sleep(now_time - access_time)
            access_time = now_time
            # get statues of a list
            res = crawler.lookup_statuses(id_=id_str_list, tweet_mode='extended')
            res_id_set = set()
            for new_status in res:
                # update full text field for every doc of this round
                doc = fix_db[id_map[new_status.id_str]]
                doc['full_text'] = new_status.full_text
                q.put(doc)
                # put the results to set A
                res_id_set.add(new_status.id_str)
            id_set = set(id_str_list)
            # make the all ids set B
            diff = id_set.difference(res_id_set)
            for non_exist_id in list(diff):
                # delete documents that are not available at the moment
                fix_db[id_map[non_exist_id]].delete()
            id_str_list = []

    # handle the left documents when the bulk may not be handled
    # cuz its size may less than the threshold
    if len(id_str_list):
        now_time = time()
        if now_time - access_time < 1:
            sleep(now_time - access_time)
        res = crawler.lookup_statuses(id_=id_str_list, tweet_mode='extended')
        res_id_set = set()
        for new_status in res:
            doc = fix_db[id_map[new_status.id_str]]
            doc['full_text'] = new_status.full_text
            q.put(doc)
            res_id_set.add(new_status.id_str)
        id_set = set(id_str_list)
        diff = id_set.difference(res_id_set)
        for non_exist_id in list(diff):
            fix_db[id_map[non_exist_id]].delete()

    flag = False


if __name__ == '__main__':
    # start a producer thread which gets and processes the tweets without full text
    threading.Thread(target=fix_full).start()
    # start a worker that stores the processed tweets
    threading.Thread(target=worker).start()
