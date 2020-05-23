import logging
from tqdm import tqdm
from utils.config import Config
from utils.database import CouchDB
from utils.models import SemanticAnalysis
from utils.sentiment_model import *
from utils.logger import get_logger

logger = get_logger('Analysis', logging.DEBUG)
couch_db = CouchDB()
statuses_db = couch_db.client['statuses']


def save_to_db(ml_res):
    """
    Currently empty; be careful when you try to insert res to our sharing db
    :param ml_res: the ML results to save
    :return:
    """
    logger.debug("Saved {} results".format(len(ml_res)))
    pass


def get_unprocessed_statuses(limit=None):
    """
    :return: list res: the result of the Machine Learning tasks which means unprocessed by ML
    """
    # Create a couch_db instance; it will try connecting automatically

    res = []
    # Get ML unprocessed statuses
    if 'statuses' in couch_db.client.all_dbs():
        if limit is not None:
            result = statuses_db.get_view_result('_design/task', view_name='ml', limit=limit, reduce=False).all()
        else:
            result = statuses_db.get_view_result('_design/task', view_name='ml', reduce=False).all()
        res = result

    logger.info("Got {} statuses".format(len(res)))
    return res


if __name__ == '__main__':

    # get unprocessed statuses
    sent_tasks = get_unprocessed_statuses()

    # predict statuses
    bulk = []
    for doc in tqdm(sent_tasks):
        scores = generate_sentiment(doc['key'][1])
        real_doc = statuses_db[doc['id']]
        real_doc['sentiment'] = scores['sentiment']
        real_doc['sentiment_scores'] = scores
        bulk.append(real_doc)
        if len(bulk) > 100:
            statuses_db.bulk_docs(bulk)
            bulk = []

    if len(bulk):
        statuses_db.bulk_docs(bulk)
