import json
import logging
from utils.logger import get_logger

logger = get_logger('Config', logging.DEBUG)


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
            self.twitter = t_json
            logger.debug("[*] Loaded {} credentials from twitter.json".format(len(self.twitter)))

        with open("couchdb.json") as t:
            t_json = json.loads(t.read())
            self.couch = CouchConfig(t_json["protocol"], t_json["host"], t_json["port"], t_json["username"],
                                     t_json["password"])
            logger.debug(
                "[*] Loaded CouchDB config -> {}://{}:{}".format(self.couch.protocol, self.couch.host, self.couch.port))
        with open("harvest.json") as t:
            harvest_json = json.loads(t.read())
            self.registry_port = harvest_json['registry_port']
            self.token = harvest_json['token']
            self.melbourne_bbox = harvest_json['melbourne_bbox']
            self.victoria_bbox = harvest_json['victoria_bbox']
            self.hash_algorithm = harvest_json['hash_algorithm']
            self.timeline_updating_window = harvest_json['timeline_updating_window']
            self.friends_updating_window = harvest_json['friends_updating_window']
            self.task_chunk_size = harvest_json['task_chunk_size']
            self.heartbeat_time = harvest_json['heartbeat_time']
            self.max_heartbeat_lost_time = harvest_json['max_heartbeat_lost_time']
            self.user_timeline_max_statues = harvest_json['user_timeline_max_statues']
            self.network_err_reconnect_time = harvest_json['network_err_reconnect_time']
            self.max_network_err = harvest_json['max_network_err']
            self.friends_max_ids = harvest_json['friends_max_ids']
            self.max_save_tries = harvest_json['max_save_tries']
            self.max_running_friends = harvest_json['max_running_friends']
            self.max_running_timeline = harvest_json['max_running_timeline']
            self.max_tasks_num = harvest_json['max_tasks_num']
            self.max_task_runtime = harvest_json['max_task_runtime']
            self.print_log_when_saved = harvest_json['print_log_when_saved']
            self.max_ids_single_task = harvest_json['max_ids_single_task']
            self.max_queue_size = harvest_json['max_queue_size']
            self.aus_sa2_2016_lv12_path = harvest_json['aus_sa2_2016_lv12_path']
            self.ignore_statuses_out_of_australia = harvest_json['ignore_statuses_out_of_australia']
            self.bulk_size = harvest_json['bulk_size']
