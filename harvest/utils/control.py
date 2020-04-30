import logging
import socket
import threading
import json
import queue

from time import sleep
from collections import defaultdict
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
        self.msg_queue = queue.Queue()
        self.msg_queue_dict = defaultdict(queue.Queue)
        self.workers = []
        self.lock_workers = threading.Lock()

    def create(self):
        if 'control' not in self.client.all_dbs():
            self.client.create_database('control')

            self.client['control'].create_document({
                '_id': 'registry',
                'ip': self.ip,
                'port': config.registry_port,
                'token': config.token
            })
            logger.debug('[*] Registry created in database: {}:{}'.format(self.ip, config.registry_port))
        else:
            doc = self.client['control']['registry']
            doc['ip'] = self.ip
            doc['port'] = config.registry_port
            doc['token'] = config.token
            doc.save()
            logger.debug('[*] Registry updated in database: {}:{}'.format(self.ip, config.registry_port))

    def tcp_server(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.ip, config.registry_port))
        s.listen(1)
        logger.debug('[*] Registry TCP server started at {}:{}'.format(self.ip, config.registry_port))
        while True:
            conn, addr = s.accept()
            self.msg_queue.put((conn, addr))

    def msg_handler(self):
        while True:
            sleep(0.01)
            while not self.msg_queue.empty():
                (conn, addr) = self.msg_queue.get()

                data = conn.recv(1024)
                try:
                    recv_json = json.loads(data)
                    if 'token' in recv_json and recv_json['token'] == config.token:
                        if recv_json['action'] == 'init':
                            if recv_json['role'] == 'sender':
                                addr_str = "%s:%s" % (addr[0], addr[1])
                                threading.Thread(target=self.receiver,
                                                 args=(conn, addr, self.msg_queue_dict[addr_str],)).start()
                            elif recv_json['role'] == 'receiver':
                                threading.Thread(target=self.sender,
                                                 args=(
                                                     conn, addr,
                                                     self.msg_queue_dict[recv_json['sender_info']],)).start()
                except json.JSONDecodeError as e:
                    pass

    @staticmethod
    def receiver(conn, addr, msg_queue):
        while True:
            try:
                recv_json = json.loads(conn.recv(1024))
                if 'token' in recv_json and recv_json['token'] == config.token:
                    if recv_json['action'] == 'ask':
                        logger.debug("[*] Ask from {}: {} ".format(addr, recv_json['data']['id_str']))
                        msg = {'task': 'save', 'id_str': recv_json['data']['id_str'], 'token': config.token}
                        msg_queue.put(json.dumps(msg))
                pass
            except socket.error as e:
                pass
            except json.JSONDecodeError as e:
                pass
            sleep(0.01)

    @staticmethod
    def sender(conn, addr, msg_queue):
        try:
            msg = {"task": "stream",
                   "data": {
                       "locations": config.melbourne_bbox},
                   "token": config.token
                   }
            conn.send(bytes(json.dumps(msg), 'utf-8'))
            logger.debug('[*] Sent stream task to Worker-{}.'.format(addr))
            while True:
                while not msg_queue.empty():
                    msg = msg_queue.get()
                    try:
                        conn.send(bytes(msg, 'utf-8'))
                        logger.debug("[*] Sent  to {}: {} ".format(addr, msg))
                    except socket.error as e:
                        msg_queue.put(msg)
                sleep(0.01)
        except socket.error as e:
            pass

    def run(self):
        self.create()
        threading.Thread(target=self.tcp_server).start()
        threading.Thread(target=self.msg_handler).start()


class Worker:
    def __init__(self, ip):
        self.ip = ip
        self.couch = CouchDB()
        self.client = self.couch.client
        self.stream_res_queue = queue.Queue()
        self.received = queue.Queue()
        self.to_send = queue.Queue()
        self.statuses = {}
        self.lock_statuses = threading.Lock()
        self.crawler = Crawler()

        self.socket_send = self.get_socket(is_sender=True)
        addr = self.socket_send.getsockname()
        self.addr_str = "%s:%s" % (addr[0], addr[1])
        self.socket_recv = self.get_socket(is_sender=False, sender_info=self.addr_str)

    def get_registry(self):
        registry = self.client['control']['registry']
        return registry['ip'], registry['port'], registry['token']

    def get_socket(self, is_sender, sender_info=None):
        ip, port, token = self.get_registry()
        s_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s_.connect((ip, port))

        if is_sender:
            addr = s_.getsockname()
            addr_str = "%s:%s" % (addr[0], addr[1])
            msg = {'action': 'init', 'role': 'sender', 'token': config.token}
            logger.debug("[*] Worker-{}-sender connected to {}".format(addr_str, (ip, port)))
        else:
            msg = {'action': 'init', 'role': 'receiver', 'token': config.token, 'sender_info': sender_info}
            logger.debug("[*] Worker-{}-receiver connected to {}".format(self.addr_str, (ip, port)))

        s_.send(bytes(json.dumps(msg), 'utf-8'))
        return s_

    def msg_receiver(self):
        while True:
            msg = self.socket_recv.recv(1024)
            self.received.put(msg)
            sleep(0.01)

    def msg_sender(self):
        while True:
            while not self.to_send.empty():
                msg = self.to_send.get()
                self.socket_send.send(bytes(msg, 'utf-8'))
                logger.debug("[*] Worker-{} sent: {}".format(self.addr_str, msg))
            sleep(0.01)

    def msg_received_handler(self):
        while True:
            if not self.received.empty():
                msg = self.received.get()
                msg_json = json.loads(msg)
                if 'token' in msg_json and msg_json['token'] == config.token:
                    logger.debug("[*] Worker-{} received: {}".format(self.addr_str, msg))
                    task = msg_json['task']
                    if task == 'stream':
                        threading.Thread(target=self.crawler.stream_filter,
                                         args=(self.ip, self.stream_res_queue,),
                                         kwargs={'languages': ['en'],
                                                 'locations': msg_json['data']['locations']}
                                         ).start()
                        # run two in case of disconnecting as doc recommended
                        # https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/connecting
                        # sleep(5)
                        # threading.Thread(target=self.crawler.stream_filter,
                        #                  args=(self.ip, self.res_queue,),
                        #                  kwargs={'languages': ['en'],
                        #                          'locations': msg_json['data']['locations']}
                        #                  ).start()
                    elif task == 'save':
                        self.lock_statuses.acquire()
                        status = self.statuses[msg_json['id_str']]
                        del self.statuses[msg_json['id_str']]
                        self.lock_statuses.release()
                        self.save_status(status)
                    elif task == 'abandon':
                        self.lock_statuses.acquire()
                        del self.statuses[msg_json['id_str']]
                        self.lock_statuses.release()
            else:
                sleep(0.01)

    def status_handler(self):
        while True:
            if not self.stream_res_queue.empty():
                status = self.stream_res_queue.get()
                self.save_status(status, is_stream=True)
                # msg = {
                #     "action": "ask",
                #     "data": {
                #         "id_str": status.id_str
                #     },
                #     "token": config.token
                # }
                # self.to_send.put(json.dumps(msg))
                # self.lock_statuses.acquire()
                # self.statuses[status.id_str] = status
                # self.lock_statuses.release()
                # logger.debug("[*] Worker-{} got status: {}".format(self.ip, status.id_str))
            else:
                sleep(0.01)

    def save_status(self, status_, is_stream):
        # use id_str
        # The string representation of the unique identifier for this Tweet.
        # Implementations should use this rather than the large integer in id
        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
        if status_.id_str not in self.client['statues']:
            status_json = status_._json
            status_json['_id'] = status_.id_str
            self.client['statues'].create_document(status_json)
            logger.debug("[*] Worker-{} saved status: {}".format(self.addr_str, status_.id_str))
        else:
            logger.debug("[*] Worker-{} ignored status: {}".format(self.addr_str, status_.id_str))
        if is_stream:
            if status_.author.id_str not in self.client['stream_users']:
                user_json = status_.author._json
                user_json['_id'] = status_.author.id_str
                self.client['stream_users'].create_document(user_json)
                logger.debug("[*] Worker-{} saved user: {}".format(self.addr_str, status_.author.id_str))
            else:
                logger.debug("[*] Worker-{} ignored user: {}".format(self.addr_str, status_.author.id_str))
        if status_.author.id_str not in self.client['all_users']:
            user_json = status_.author._json
            user_json['_id'] = status_.author.id_str
            self.client['all_users'].create_document(user_json)
            logger.debug("[*] Worker-{} saved user: {}".format(self.addr_str, status_.author.id_str))
        else:
            logger.debug("[*] Worker-{} ignored user: {}".format(self.addr_str, status_.author.id_str))

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

    def run(self):
        self.check_db()
        # start a stream listener, statues will be put in to a res queue
        threading.Thread(target=self.msg_receiver).start()
        threading.Thread(target=self.msg_received_handler).start()
        threading.Thread(target=self.status_handler).start()
        threading.Thread(target=self.msg_sender).start()
        threading.Thread(target=self.msg_receiver).start()
        threading.Thread(target=self.msg_received_handler).start()
        threading.Thread(target=self.status_handler).start()
