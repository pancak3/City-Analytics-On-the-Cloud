"""
@author Team 42, Chengdu, China, Qifan Deng, 1077479
"""
import json
from utils.logger import get_logger
from collections import namedtuple


class TwitterCredential:
    def __init__(self, api_key, api_secrete_key, access_token, access_token_secret):
        """
        A object contains twitter credentials
        :param api_key: twitter api key
        :param api_secrete_key: twitter secrete key
        :param access_token: twitter  access token
        :param access_token_secret: twitter access token secret
        """
        self.api_key = api_key
        self.api_secrete_key = api_secrete_key
        self.access_token = access_token
        self.access_token_secret = access_token_secret


def _json_object_hook(d): return namedtuple('X', d.keys())(*d.values())


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
    def __init__(self, log_level):
        """
        load configs from files
        :param log_level: logging level
        """
        self.logger = get_logger('Config', log_level)
        with open("twitter.json") as t:
            t_json = json.loads(t.read())
            self.twitter = t_json
            self.logger.debug("[*] Loaded {} credentials from twitter.json".format(len(self.twitter)))

        with open("couchdb.json") as t:
            t_json = json.loads(t.read())
            self.couch = CouchConfig(t_json["protocol"],  t_json["host"], t_json["port"],
                                     t_json["username"], t_json["password"])
            self.logger.debug(
                "[*] Loaded CouchDB config -> {}://{}:{}".format(self.couch.protocol, self.couch.host, self.couch.port))

        with open("harvest.json") as f:
            content = f.read()
            conf = json.loads(content, object_hook=_json_object_hook)
            for key in conf._fields:
                setattr(self, key, getattr(conf, key))
