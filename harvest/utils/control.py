import logging
import socket
import threading
import json
import queue

from time import sleep, time
from pprint import pprint
from collections import defaultdict
from utils.config import config
from utils.database import CouchDB
from utils.crawlers import Crawler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Control')
logger.setLevel(logging.DEBUG)


class Task:
    def __init__(self, _type, ids):
        self.type = _type
        self.ids = ids


class Registry:
    def __init__(self, ip):
        self.ip = ip
        self.couch = CouchDB()
        self.client = self.couch.client
        self.api_key_worker = {}
        self.lock = threading.Lock()
        self.working = {
            "credential_states": [None for i in range(len(config.twitter))],
            "stream_listeners": [],
            "user_readers": []
        }
        self.conn_queue = queue.Queue()
        self.msg_queue_dict = defaultdict(queue.Queue)
        self.tasks_queue = queue.Queue()

    def update_db(self):
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
            self.conn_queue.put((conn, addr))

    def conn_handler(self):
        while True:
            while not self.conn_queue.empty():
                (conn, addr) = self.conn_queue.get()
                threading.Thread(target=self.msg_handler, args=(conn, addr,)).start()
            sleep(0.01)

    def msg_handler(self, conn, addr):
        addr_str = "%s:%s" % (addr[0], addr[1])
        while True:
            try:
                data = conn.recv(1024)
                recv_json = json.loads(data)
                if 'token' in recv_json and recv_json['token'] == config.token:
                    if recv_json['action'] == 'init':
                        if recv_json['role'] == 'sender':
                            valid_api_key_hash = None
                            self.lock.acquire()
                            for api_key_hash in recv_json['api_keys_hashes']:
                                if api_key_hash not in self.api_key_worker:
                                    self.api_key_worker[api_key_hash] = (conn, addr)
                                    valid_api_key_hash = api_key_hash
                                    break
                            self.lock.release()
                            if valid_api_key_hash is None:
                                msg = {'token': config.token, 'res': 'deny', 'msg': 'no valid api key'}
                            else:
                                msg = {'token': config.token, 'res': 'approve', 'api_key_hash': valid_api_key_hash}
                                threading.Thread(target=self.receiver,
                                                 args=(conn, addr, self.msg_queue_dict[addr_str],)).start()
                            conn.send(bytes(json.dumps(msg), 'utf-8'))

                        elif recv_json['role'] == 'receiver':
                            threading.Thread(target=self.sender,
                                             args=(
                                                 conn, addr,
                                                 self.msg_queue_dict[recv_json['sender_info']],)).start()
            except json.JSONDecodeError as e:
                pass
            except socket.error as e:
                logger.warning("Worker-{} disconnected: {}".format(addr_str, e))
                break

    def tasks_generator(self):
        while True:
            if 'all_users' in self.client.all_dbs():
                all_users = self.client['all_users']
                task_timeline_ids = []

                for user in all_users:
                    if 'timeline_updated_at' not in user:
                        user['timeline_updated_at'] = 0
                        user.save()
                    if int(time()) - user['timeline_updated_at'] > config.timeline_updating_window:
                        task_timeline_ids.append(user['id_str'])
                if len(task_timeline_ids):
                    for i in range(0, len(task_timeline_ids), config.task_chunk_size):
                        self.tasks_queue.put(Task('timeline', task_timeline_ids[i:i + config.task_chunk_size]))

            if 'stream_users' in self.client.all_dbs():
                stream_users = self.client['stream_users']
                task_friend_ids = []

                for user in stream_users:
                    if 'friends_updated_at' not in user:
                        user['friends_updated_at'] = 0
                        user.save()
                    if int(time()) - user['friends_updated_at'] > config.friends_updating_window:
                        task_friend_ids.append(user['id_str'])
                if len(task_friend_ids):
                    for i in range(0, len(task_friend_ids), config.task_chunk_size):
                        self.tasks_queue.put(Task('friends', task_friend_ids[i:i + config.task_chunk_size]))
            sleep(0.01)

    def master(self):
        # Broadcast tasks and manage task states
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
        self.update_db()
        threading.Thread(target=self.tcp_server).start()
        threading.Thread(target=self.conn_handler).start()
        threading.Thread(target=self.tasks_generator).start()
        threading.Thread(target=self.master).start()


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
        self.socket_send, self.socket_recv, valid_api_key_hash, addr_str = self.connect_reg()
        self.crawler.init(valid_api_key_hash)
        self.addr_str = addr_str

    def get_registry(self):
        registry = self.client['control']['registry']
        return registry['ip'], registry['port'], registry['token']

    def connect_reg(self):
        ip, port, token = self.get_registry()

        socket_sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_sender.connect((ip, port))
        addr = socket_sender.getsockname()
        addr_str = "%s:%s" % (addr[0], addr[1])
        msg = {'action': 'init', 'role': 'sender', 'token': config.token,
               'api_keys_hashes': list(self.crawler.api_keys)}
        socket_sender.send(bytes(json.dumps(msg), 'utf-8'))
        msg = socket_sender.recv(1024)
        msg_json = json.loads(msg)
        if 'token' in msg_json and msg_json['token'] == config.token:
            if msg_json['res'] == 'approve':
                valid_api_key_hash = msg_json['api_key_hash']
                socket_receiver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                socket_receiver.connect((ip, port))
                msg = {'action': 'init', 'role': 'receiver', 'token': config.token, 'sender_info': addr_str}
                socket_receiver.send(bytes(json.dumps(msg), 'utf-8'))
                logger.debug("[*] Worker-{} connected to {}".format(addr_str, (ip, port)))
                return socket_sender, socket_receiver, valid_api_key_hash, addr_str
            else:
                logger.info("[!] No valid api key. Exit.")
                exit(0)
        logger.info("[!] Registry didn't respond correctly. Exit. -> {}".format(msg))
        exit(0)

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
            logger.debug("[*] Worker-{} saved status: {}".format(self.addr_str, status_.id_str))
        else:
            logger.debug("[*] Worker-{} ignored status: {}".format(self.addr_str, status_.id_str))
        if is_stream:
            if status_.author.id_str not in self.client['stream_users']:
                user_json = status_.author._json
                user_json['_id'] = status_.author.id_str
                self.client['stream_users'].create_document(user_json)
                logger.debug("[*] Worker-{} saved to stream users: {}".format(self.addr_str, status_.author.id_str))
        if status_.author.id_str not in self.client['all_users']:
            user_json = status_.author._json
            user_json['_id'] = status_.author.id_str
            self.client['all_users'].create_document(user_json)
            logger.debug("[*] Worker-{} saved to all users: {}".format(self.addr_str, status_.author.id_str))

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
