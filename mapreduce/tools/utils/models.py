"""
@author Team 42, Chengdu, China, Qifan Deng, 1077479
"""
import logging
from utils.logger import get_logger

logger = get_logger('Models', logging.DEBUG)


class SemanticAnalysis:
    def __init__(self, model_path):
        self.model = self.load_model(model_path)

    def load_model(self, model_path):
        """
        Implement your model loading here
        :param model_path:
        :return:
        """
        logger.debug("Loaded model: {}".format(model_path))
        return 1

    def predict(self, doc):
        """
        Implement your predict here
        :param doc:
        :return:
        """
        logger.debug("Predicting doc: {}".format(doc))
        pass
