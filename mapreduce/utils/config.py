"""
@author Team 42, Chengdu, China, Qifan Deng, 1077479
"""
import json
import logging
from utils.logger import get_logger

logger = get_logger('Config', logging.DEBUG)


class CouchConfig:
    def __init__(self, protocol, host, port, username, password):
        """
        :param protocol: CouchDB protocol, e.g. 'http'
        :param host: CouchDB host addr, e.g. '127.0.0.1'
        :param port: CouchDB port number, e.g. 5984
        :param username: CouchDB username
        :param password: password of user
        """
        self.protocol = protocol
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.url = "{}://{}:{}".format(self.protocol, self.host, self.port)


class Config:
    def __init__(self):
        with open("couchdb.json") as t:
            t_json = json.loads(t.read())
            self.couch = CouchConfig(t_json["protocol"], t_json["host"], t_json["port"], t_json["username"],
                                     t_json["password"])
            logger.debug(
                "[*] Loaded CouchDB config -> {}://{}:{}".format(self.couch.protocol, self.couch.host, self.couch.port))

