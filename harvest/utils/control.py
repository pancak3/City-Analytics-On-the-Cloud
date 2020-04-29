import logging
import socket
import threading
import json
import queue

from time import sleep
from utils.config import config
from utils.database import CouchDB
from utils.crawlers import Crawler
from utils.funcs import extract_status

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Control')
logger.setLevel(logging.DEBUG)


class Registry:
    def __init__(self, ip):
        self.ip = ip
        self.couch = CouchDB()
        self.client = self.couch.client
        self.lock = threading.Lock()
        self.working = {
            "credential_states": [None for i in range(len(config.twitter))],
            "stream_listeners": [],
            "user_readers": []
        }

    def create(self):
        if 'registry' in self.client.all_dbs():
            self.client['registry'].delete()
        db_registry = self.client.create_database('registry')
        db_registry.create_document({
            'ip': self.ip,
            'port': config.registry_port,
            'token': config.token
        })
        logger.debug('[*] Registry created in database: {}:{}'.format(self.ip, config.registry_port))

    def tcp_server(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.ip, config.registry_port))
        s.listen(1)
        logger.debug('[*] Registry TCP server started at {}:{}'.format(self.ip, config.registry_port))
        while True:
            conn, addr = s.accept()
            msg = {"task": "stream",
                   "data": {
                       "locations": config.melbourne_bbox},
                   "token": config.token
                   }
            conn.send(bytes(json.dumps(msg), 'utf-8'))
            logger.debug('[*] Sent stream task to Worker-{}.'.format(addr))
            data = conn.recv(1024)
            if not data:
                continue
            print(addr, data)

    def run(self):
        self.create()
        t = threading.Thread(target=self.tcp_server, args=())
        t.start()


class Worker:
    def __init__(self, ip):
        self.ip = ip
        self.couch = CouchDB()
        self.client = self.couch.client
        self.res_queue = queue.Queue()
        self.socket = self.connect()
        self.lock_socket = threading.Lock()
        self.received = queue.Queue()
        self.to_send = queue.Queue()
        self.statuses = {}
        self.lock_statuses = threading.Lock()

    def get_registry(self):
        for document in self.client['registry']:
            registry = document
            return registry['ip'], registry['port'], registry['token']

    def connect(self):
        ip, port, token = self.get_registry()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, port))
        return s

    def msg_receiver(self):
        while True:
            self.lock_socket.acquire()
            msg = self.socket.recv(1024)
            self.lock_socket.release()
            self.received.put(msg)

    def msg_sender(self):
        while True:
            self.lock_socket.acquire()
            while not self.to_send.empty():
                msg = self.to_send.get()
                self.socket.send(msg)
            self.lock_socket.release()

    def msg_received_handler(self):
        while True:
            if not self.received.empty():
                msg = self.received.get()
                print(msg)
                msg_json = json.loads(msg)
                if 'token' in msg_json and msg_json['token'] == config.token:
                    task = msg_json['task']
                    if task == 'stream':
                        crawler = Crawler()
                        crawler.stream_filter(self.ip, self.res_queue,
                                              languages=['en'], locations=msg_json['data']['locations'])
            else:
                sleep(0.01)

    def status_handler(self):
        while True:
            if not self.res_queue.empty():
                status = self.res_queue.get()
                self.lock_statuses.acquire()
                self.statuses[status.id_str] = status
                self.lock_statuses.release()

            else:
                sleep(0.01)

    def check_db(self):
        if 'statues' not in self.client.all_dbs():
            self.client.create_database('statues')
            logger.debug("[*] Statues table not in database; created.")
        if 'status_ids' not in self.client.all_dbs():
            self.client.create_database('status_ids')
            logger.debug("[*] Status_ids table not in database; created.")

    def run(self):
        self.check_db()
        # start a stream listener, statues will be put in to a res queue
        threading.Thread(target=self.msg_receiver).start()
        threading.Thread(target=self.msg_received_handler).start()
        threading.Thread(target=self.status_handler).start()
