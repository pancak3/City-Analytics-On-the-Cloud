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
    :return:
    """
    global q, fix_db, flag
    bulk = []
    while flag or not q.empty():
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
    :return:
    """
    global q, fix_db, flag
    crawler = Crawler(logging.DEBUG)
    for item in crawler.api_keys.items():
        crawler.init(item[0], 0)
        break

    statuses = fix_db.get_view_result('_design/task', view_name='full-text', reduce=False).all()

    id_str_list = []
    id_map = {}
    access_time = time()
    for status in tqdm(statuses, desc="Fix full text"):
        id_str_list.append(status['key'])
        id_map[status['key']] = status['id']
        if len(id_str_list) >= 100:
            now_time = time()
            if now_time - access_time < 1:
                sleep(now_time - access_time)
            access_time = now_time
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
            id_str_list = []

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
    threading.Thread(target=fix_full).start()
    threading.Thread(target=worker).start()
