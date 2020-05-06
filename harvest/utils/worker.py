import logging
import socket
import threading
import json
import queue

from time import sleep, time
from urllib3.exceptions import ReadTimeoutError, ProtocolError
from utils.config import config
from utils.database import CouchDB
from utils.crawlers import Crawler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Worker')
logger.setLevel(logging.DEBUG)


class Task:
    def __init__(self, _id, _type, ids, status='not_assigned'):
        self.id = _id
        self.type = _type
        self.ids = ids
        self.status = status


class WorkerData:
    def __init__(self, _id, receiver_conn, receiver_addr, api_key_hash):
        self.worker_id = _id
        self.sender_conn = None
        self.sender_addr = None
        self.receiver_conn = receiver_conn
        self.receiver_addr = receiver_addr
        self.api_key_hash = api_key_hash
        self.msg_queue = queue.Queue()


class Worker:
    def __init__(self, ip):
        self.ip = ip
        self.couch = CouchDB()
        self.client = self.couch.client
        self.stream_res_queue = queue.Queue()
        self.msg_received = queue.Queue()
        self.msg_to_send = queue.Queue()
        self.crawler = Crawler()
        self.socket_send, self.socket_recv, valid_api_key_hash, worker_id = self.connect_reg()
        self.crawler.init(valid_api_key_hash)
        self.worker_id = worker_id

    def get_registry(self):
        registry = self.client['control']['registry']
        return registry['ip'], registry['port'], registry['token']

    def connect_reg(self):
        reg_ip, port, token = self.get_registry()

        socket_sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_sender.connect((reg_ip, port))
        msg = {'action': 'init', 'role': 'sender', 'token': config.token,
               'api_keys_hashes': list(self.crawler.api_keys)}
        socket_sender.send(bytes(json.dumps(msg), 'utf-8'))
        msg = socket_sender.recv(1024)
        msg_json = json.loads(msg)
        if 'token' in msg_json and msg_json['token'] == config.token:
            if msg_json['res'] == 'use_api_key':
                valid_api_key_hash = msg_json['api_key_hash']
                socket_receiver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                socket_receiver.connect((reg_ip, port))
                msg = {'action': 'init', 'role': 'receiver', 'token': config.token, 'worker_id': msg_json['worker_id']}
                socket_receiver.send(bytes(json.dumps(msg), 'utf-8'))
                logger.debug("[*] Worker-{} connected to {}".format(msg_json['worker_id'], (reg_ip, port)))
                return socket_sender, socket_receiver, valid_api_key_hash, msg_json['worker_id']
            else:
                logger.info("[!] No valid api key. Exit.")
                exit(0)
        logger.info("[!] Registry didn't respond correctly. Exit. -> {}".format(msg))
        exit(0)

    def msg_receiver(self):
        while True:
            msg = self.socket_recv.recv(1024)
            self.msg_received.put(msg)
            sleep(0.01)

    def msg_sender(self):
        while True:
            while not self.msg_to_send.empty():
                msg = self.msg_to_send.get()
                self.socket_send.send(bytes(msg, 'utf-8'))
                logger.debug("[*] Worker-{} sent: {}".format(self.worker_id, msg))
            sleep(0.01)

    def msg_received_handler(self):
        while True:
            if not self.msg_received.empty():
                msg = self.msg_received.get()
                try:
                    msg_json = json.loads(msg)
                    if 'token' in msg_json and msg_json['token'] == config.token:
                        logger.debug("[*] Worker-{} received: {}".format(self.worker_id, msg))
                        task = msg_json['task']
                        if task == 'stream':
                            threading.Thread(target=self.stream,
                                             args=(msg_json['data']['locations'],)).start()
                        # msg = {'token': config.token, 'task': 'task', 'type': task.type}
                        elif task == 'task':
                            if self.have_quota(msg_json['type']):
                                msg = {'token': config.token, 'action': 'take_task', 'worker_id': self.worker_id,
                                       'task_id': msg_json['task_id']}
                                self.msg_to_send.put(json.dumps(msg))
                        elif task == 'take_task':
                            logger.debug("Worker-{} got task: {}".format(self.worker_id, msg))
                except json.decoder.JSONDecodeError as e:
                    logger.warning("Worker-{} received invalid json: {}".format(self.worker_id, msg))
                    exit(0)
            else:
                sleep(0.01)

    def have_quota(self, type_):
        return True

    def status_handler(self):
        while True:
            if not self.stream_res_queue.empty():
                status = self.stream_res_queue.get()
                self.save_status(status, is_stream=True)
            else:
                sleep(0.01)

    def save_status(self, status_, is_stream=False):
        # use id_str
        # The string representation of the unique identifier for this Tweet.
        # Implementations should use this rather than the large integer in id
        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
        if status_.id_str not in self.client['statues']:
            status_json = status_._json
            status_json['_id'] = status_.id_str
            self.client['statues'].create_document(status_json)
            logger.debug("[*] Worker-{} saved status: {}".format(self.worker_id, status_.id_str))
        else:
            logger.debug("[*] Worker-{} ignored status: {}".format(self.worker_id, status_.id_str))
        if is_stream:
            if status_.author.id_str not in self.client['stream_users']:
                user_json = status_.author._json
                user_json['_id'] = status_.author.id_str
                user_json['friends_updated_at'] = 0
                self.client['stream_users'].create_document(user_json)
                logger.debug("[*] Worker-{} saved to stream users: {}".format(self.worker_id, status_.author.id_str))
        if status_.author.id_str not in self.client['all_users']:
            user_json = status_.author._json
            user_json['_id'] = status_.author.id_str
            user_json['friends_updated_at'] = 0
            self.client['all_users'].create_document(user_json)
            logger.debug("[*] Worker-{} saved to all users: {}".format(self.worker_id, status_.author.id_str))

    def check_db(self):
        if 'statues' not in self.client.all_dbs():
            self.client.create_database('statues')
            logger.debug("[*] Statues table not in database; created.")
        if 'stream_users' not in self.client.all_dbs():
            self.client.create_database('stream_users')
            logger.debug("[*] Stream_users table not in database; created.")

        if 'all_users' not in self.client.all_dbs():
            self.client.create_database('all_users')
            logger.debug("[*] All_users table not in database; created.")

    def stream(self, bbox_, count=0):
        if count > 5:
            logger.warning("Worker-{} stream failed {} times, worker exit.".format(self.worker_id, count))
            exit(1)
        else:
            try:
                threading.Thread(target=self.crawler.stream_filter,
                                 args=(self.worker_id, self.stream_res_queue,),
                                 kwargs={'languages': ['en'],
                                         'locations': bbox_}
                                 ).start()
            except ProtocolError as e:
                count += 1
                logger.warning("Worker-{} stream err: {}".format(self.worker_id, e))
                sleep(count ** 2)
                self.stream(bbox_, count)
            except ReadTimeoutError as e:
                count += 1
                logger.warning("Worker-{} stream err: {}".format(self.worker_id, e))
                sleep(count ** 2)
                self.stream(bbox_, count)

    def run(self):
        self.check_db()
        # start a stream listener, statues will be put in to a res queue
        threading.Thread(target=self.msg_receiver).start()
        threading.Thread(target=self.msg_received_handler).start()
        threading.Thread(target=self.status_handler).start()
        threading.Thread(target=self.msg_sender).start()
        threading.Thread(target=self.msg_receiver).start()
        threading.Thread(target=self.msg_received_handler).start()
