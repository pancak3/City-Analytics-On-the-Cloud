import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Config')
logger.setLevel(logging.DEBUG)


class TwitterCredential:
    def __init__(self, api_key, api_secrete_key, access_token, access_token_secret):
        self.api_key = api_key
        self.api_secrete_key = api_secrete_key
        self.access_token = access_token
        self.access_token_secret = access_token_secret


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
        with open("twitter.json") as t:
            t_json = json.loads(t.read())
            self.twitter = []
            for c in t_json:
                self.twitter.append(
                    TwitterCredential(c['api_key'], c['api_secret_key'], c['access_token'], c['access_token_secret']))
            logger.debug("[*] Loaded {} credentials from twitter.json".format(len(self.twitter)))

        with open("couchdb.json") as t:
            t_json = json.loads(t.read())
            self.couch = CouchConfig(t_json["protocol"], t_json["host"], t_json["port"], t_json["username"],
                                     t_json["password"])
            logger.debug(
                "[*] Loaded CouchDB config -> {}://{}:{}".format(self.couch.protocol, self.couch.host, self.couch.port))


config = Config()
