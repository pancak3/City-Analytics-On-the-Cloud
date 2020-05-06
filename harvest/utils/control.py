import logging
import socket
import threading
import json
import queue

from time import sleep, time
from pprint import pprint
from urllib3.exceptions import ReadTimeoutError, ProtocolError
from collections import defaultdict
from utils.config import config
from utils.database import CouchDB
from utils.crawlers import Crawler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Control')
logger.setLevel(logging.DEBUG)


class Task:
    def __init__(self, _id, _type, ids, assigned=False):
        self.id = _id
        self.type = _type
        self.ids = ids
        self.assigned = assigned


class WorkerData:
    def __init__(self, _id, receiver_conn, receiver_addr, api_key_hash):
        self.worker_id = _id
        self.sender_conn = None
        self.sender_addr = None
        self.receiver_conn = receiver_conn
        self.receiver_addr = receiver_addr
        self.api_key_hash = api_key_hash
        self.msg_queue = queue.Queue()


class Registry:
    def __init__(self, ip):
        self.ip = ip
        self.couch = CouchDB()
        self.client = self.couch.client

        self.api_key_worker = {}
        self.lock_api_key_worker = threading.Lock()

        self.working = {
            "credential_states": [None for i in range(len(config.twitter))],
            "stream_listeners": [],
            "user_readers": []
        }
        self.conn_queue = queue.Queue()

        self.msg_queue_dict = defaultdict(queue.Queue)
        self.lock_msg_queue_dict = threading.Lock()

        self.tasks_queue = queue.Queue()
        self.tasks = {}
        self.lock_tasks = threading.Lock()
        self.task_id = 0
        self.lock_task_id = threading.Lock()

        self.worker_id = 0
        self.lock_worker_id = threading.Lock()
        self.worker_data = {}
        self.lock_worker_data = threading.Lock()

    def get_task_id(self):
        self.lock_task_id.acquire()
        self.task_id += 1
        id_tmp = self.task_id
        self.lock_task_id.release()
        return id_tmp

    def get_worker_id(self):
        self.lock_task_id.acquire()
        self.worker_id += 1
        id_tmp = self.worker_id
        self.lock_task_id.release()
        return id_tmp

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
        # docker container inner ip is always 0.0.0.0
        s.bind(('0.0.0.0', config.registry_port))
        s.listen(1)
        logger.debug('[*] Registry TCP server started at {}:{}'.format(self.ip, config.registry_port))
        while True:
            conn, addr = s.accept()
            self.conn_queue.put((conn, addr))

    def conn_handler(self):
        logger.info("[*] ConnectionHandler started.")
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
                            # Worker send API key hashes to ask which it can use
                            valid_api_key_hash = None
                            self.lock_api_key_worker.acquire()
                            for api_key_hash in recv_json['api_keys_hashes']:
                                if api_key_hash not in self.api_key_worker:
                                    self.api_key_worker[api_key_hash] = addr
                                    valid_api_key_hash = api_key_hash
                                    break
                            self.lock_api_key_worker.release()
                            if valid_api_key_hash is None:
                                msg = {'token': config.token, 'res': 'deny', 'msg': 'no valid api key'}
                            else:
                                worker_id = self.get_worker_id()
                                worker_data = WorkerData(worker_id, conn, addr, valid_api_key_hash)
                                self.lock_worker_data.acquire()
                                self.worker_data[worker_id] = worker_data
                                self.lock_worker_data.release()

                                # msg queue dict used to broadcast
                                self.lock_msg_queue_dict.acquire()
                                self.msg_queue_dict[worker_id] = worker_data.msg_queue
                                self.lock_msg_queue_dict.release()

                                threading.Thread(target=self.receiver, args=(worker_data,)).start()

                                msg = {'token': config.token, 'res': 'use_api_key',
                                       'api_key_hash': valid_api_key_hash, 'worker_id': worker_id}

                            conn.send(bytes(json.dumps(msg), 'utf-8'))

                        elif recv_json['role'] == 'receiver':
                            worker_id = recv_json['worker_id']

                            self.lock_worker_data.acquire()
                            self.worker_data[worker_id].sender_conn = conn
                            self.worker_data[worker_id].sender_addr = addr
                            worker_data = self.worker_data[worker_id]
                            self.lock_worker_data.release()

                            threading.Thread(target=self.sender, args=(worker_data,)).start()

                    if recv_json['action'] == 'take_task':
                        task_assigned = False
                        self.lock_tasks.acquire()
                        if not self.tasks[recv_json['task_id']].assigned:
                            self.tasks[recv_json['task_id']].assigned = True
                            task_assigned = True
                        self.lock_tasks.release()
                        if not task_assigned:
                            task = self.tasks[recv_json['task_id']]
                            msg = {'token': config.token, 'task': 'take_task', 'type': task.type, 'ids': task.ids}
                            self.msg_queue_dict[recv_json['worker_id']].put(json.dumps(msg))
                        pass

            except json.JSONDecodeError as e:
                pass
            except socket.error as e:
                pass

    def tasks_generator(self):
        logger.info("[*] TaskGenerator started.")
        while True:
            self.generate_tasks('all_users', 'timeline')
            self.generate_tasks('stream_users', 'friends')
            sleep(0.01)

    def generate_tasks(self, user_db_name, task_type):
        column_name = task_type + '_updated_at'
        if user_db_name in self.client.all_dbs():
            all_users = self.client[user_db_name]
            task_ids = []

            for user in all_users:
                if column_name not in user:
                    user[column_name] = 0
                    user.save()
                timestamp = int(time())
                if timestamp - user[column_name] > config.timeline_updating_window:
                    task_ids.append(user['id_str'])
                    user[column_name] = timestamp
                    user.save()
            if len(task_ids):
                count = 0
                for i in range(0, len(task_ids), config.task_chunk_size):
                    task_id = self.get_task_id()
                    task = Task(task_id, task_type, task_ids[i:i + config.task_chunk_size])
                    self.lock_tasks.acquire()
                    self.tasks[task_id] = task
                    self.lock_tasks.release()
                    self.tasks_queue.put(task)
                    count += 1
                logger.debug("Generated {} {} tasks".format(count, task_type))

    def master(self):
        logger.info("[*] Master started.")
        # Broadcast tasks and manage task states

        while True:
            while not self.tasks_queue.empty():
                task = self.tasks_queue.get()
                msg = {'token': config.token, 'task': 'task', 'type': task.type, 'task_id': task.id}

                # copy out to avoid occupying msg_queue long time
                self.lock_msg_queue_dict.acquire()
                msg_queue_dict = self.msg_queue_dict.copy()
                self.lock_msg_queue_dict.release()

                # <put_back> flag: to avoid there is no worker connected or workers are all busy
                put_back = True
                for msg_item in msg_queue_dict.items():
                    self.lock_tasks.acquire()
                    if self.tasks[task.id].assigned:
                        self.lock_tasks.release()
                        put_back = False
                        break
                    else:
                        # msg queue
                        msg_item[1].put(json.dumps(msg))
                        self.lock_tasks.release()
                if put_back:
                    self.tasks_queue.put(task)
            sleep(0.01)

    def broadcast_task(self, task):
        pass

    def receiver(self, worker_data):
        while True:
            try:
                recv_json = json.loads(worker_data.receiver_conn.recv(1024))
                if 'token' in recv_json and recv_json['token'] == config.token:
                    if recv_json['action'] == 'ask':
                        logger.debug(
                            "[*] Ask from Worker-{}: {} ".format(worker_data.worker_id, recv_json['data']['id_str']))
                        msg = {'task': 'save', 'id_str': recv_json['data']['id_str'], 'token': config.token}
                        worker_data.msg_queue.put(json.dumps(msg))
                pass
            except socket.error as e:
                self.remove_worker(worker_data)
                logger.warning("[*] Worker-{} exit.".format(worker_data.worker_id))
                break
            except json.JSONDecodeError as e:
                self.remove_worker(worker_data)
                break
            sleep(0.01)

    def remove_worker(self, worker_data):
        self.lock_worker_data.acquire()
        del self.worker_data[worker_data.worker_id]
        self.lock_worker_data.release()

        self.lock_msg_queue_dict.acquire()
        del self.msg_queue_dict[worker_data.worker_id]
        self.lock_msg_queue_dict.release()

        self.lock_api_key_worker.acquire()
        del self.api_key_worker[worker_data.api_key_hash]
        self.lock_api_key_worker.release()

        logger.warning("[*] Worker-{} exit.".format(worker_data.worker_id))

    def sender(self, worker_data):
        logger.debug('[*] Worker-{} connected.'.format(worker_data.worker_id))
        try:
            msg = {"task": "stream",
                   "data": {
                       "locations": config.victoria_bbox},
                   "token": config.token
                   }
            worker_data.sender_conn.send(bytes(json.dumps(msg), 'utf-8'))
            logger.debug('[*] Sent stream task to Worker-{}.'.format(worker_data.worker_id))
            while True:
                while not worker_data.msg_queue.empty():
                    msg = worker_data.msg_queue.get()
                    try:
                        worker_data.sender_conn.send(bytes(msg, 'utf-8'))
                        logger.debug("[*] Sent to Worker-{}: {} ".format(worker_data.worker_id, msg))
                    except socket.error:
                        worker_data.msg_queue.put(msg)
                sleep(0.01)
        except socket.error:
            self.remove_worker(worker_data)

    def run(self):
        self.update_db()
        threading.Thread(target=self.tcp_server).start()
        threading.Thread(target=self.conn_handler).start()
        # TODO: use view to generate tasks
        # threading.Thread(target=self.tasks_generator).start()
        # threading.Thread(target=self.master).start()


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
                self.client['stream_users'].create_document(user_json)
                logger.debug("[*] Worker-{} saved to stream users: {}".format(self.worker_id, status_.author.id_str))
        if status_.author.id_str not in self.client['all_users']:
            user_json = status_.author._json
            user_json['_id'] = status_.author.id_str
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
