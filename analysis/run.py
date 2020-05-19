import logging
from utils.config import Config
from utils.database import CouchDB
from utils.models import SemanticAnalysis
from utils.logger import get_logger

logger = get_logger('Analysis', logging.DEBUG)


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
    couch_db = CouchDB()

    res = []
    # Get ML unprocessed tasks
    if 'statuses' in couch_db.client.all_dbs():
        if limit is not None:
            result = couch_db.client['statuses'].get_view_result('_design/api-global',
                                                                 view_name='ml_tasks',
                                                                 limit=limit,
                                                                 reduce=False).all()
        else:

            result = couch_db.client['statuses'].get_view_result('_design/api-global',
                                                                 view_name='ml_tasks',
                                                                 reduce=False).all()
        res = result

    logger.info("Got {} statuses".format(len(res)))
    return res


if __name__ == '__main__':

    # get unprocessed statuses
    results = get_unprocessed_statuses(limit=3)

    # load config
    config = Config()

    # create ML model
    model = SemanticAnalysis(model_path=config.emotion)

    # predict statuses
    predicted_results = []
    for doc in results:
        res = model.predict(doc)
        predicted_results.append(res)

    # save to couch db
    save_to_db(predicted_results)
