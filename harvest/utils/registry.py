import logging
import socket
import threading
import json
import queue

from os import kill, getpid
from cloudant.design_document import DesignDocument, Document
from time import sleep, time
from signal import SIGUSR1
from collections import defaultdict
from utils.config import config
from utils.database import CouchDB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Registry')
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

    def check_views(self):

        design_doc = Document(self.client['stream_users'], '_design/friends')
        if not design_doc.exists():
            design_doc = DesignDocument(self.client['stream_users'], '_design/friends')
            map_fun = 'function(doc){var date = new Date();var timestamp = date.getTime() / 1000;if (timestamp - doc.friends_updated_at > 172800){emit(doc._id, doc.friends_updated_at);}}'
            design_doc.add_view('need_updating', map_fun)
            design_doc.save()

        design_doc = Document(self.client['all_users'], '_design/timeline')
        if not design_doc.exists():
            design_doc = DesignDocument(self.client['all_users'], '_design/timeline')
            map_fun = 'function(doc){var date = new Date();var timestamp = date.getTime() / 1000;if (timestamp - doc.timeline_updated_at > 172800){emit(doc._id, doc.timeline_updated_at);}}'
            design_doc.add_view('need_updating', map_fun)
            design_doc.save()

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

    def tcp_server(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # docker container inner ip is always 0.0.0.0
            s.bind(('0.0.0.0', config.registry_port))
            s.listen(1)
            logger.debug('[*] Registry TCP server started at {}:{}'.format(self.ip, config.registry_port))
            while True:
                conn, addr = s.accept()
                self.conn_queue.put((conn, addr))
        except OSError as e:
            logger.error('[!] Cannot bind to 0.0.0.0:{}.'.format(config.registry_port))
            kill(getpid(), SIGUSR1)

    def conn_handler(self):
        logger.info("[*] ConnectionHandler started.")
        while True:
            while not self.conn_queue.empty():
                (conn, addr) = self.conn_queue.get()
                threading.Thread(target=self.msg_handler, args=(conn, addr,)).start()
            sleep(0.01)

    def msg_handler(self, conn, addr):
        data = ''
        while True:
            try:
                data += str(conn.recv(1024), 'utf-8')
                while data.find('\n') != -1:
                    first_pos = data.find('\n')
                    recv_json = json.loads(data[:first_pos])
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

                                conn.send(bytes(json.dumps(msg) + '\n', 'utf-8'))

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
                            if not self.tasks[recv_json['task_id']].status:
                                self.tasks[recv_json['task_id']].status = 'assigned'
                                task_assigned = True
                            self.lock_tasks.release()
                            if not task_assigned:
                                task = self.tasks[recv_json['task_id']]
                                msg = {'token': config.token, 'task': 'take_task', 'task_id': recv_json['task_id'],
                                       'type': task.type, 'ids': task.ids}
                                self.msg_queue_dict[recv_json['worker_id']].put(json.dumps(msg))
                            pass
                    data = data[first_pos + 1:]

            except json.JSONDecodeError as e:
                pass
            except socket.error as e:
                pass

    def tasks_generator(self):
        logger.info("[*] TaskGenerator started.")
        while True:
            self.generate_tasks('all_users', 'timeline')
            self.generate_tasks('stream_users', 'friends')
            sleep(config.timeline_updating_window / 2)

    def generate_tasks(self, user_db_name, task_type):
        start_time = time()
        logger.debug("Start to generate {} tasks.".format(task_type))

        if user_db_name in self.client.all_dbs():
            task_ids = []
            result = self.client[user_db_name].get_view_result('_design/' + task_type, 'need_updating')
            for doc in result:
                task_ids.append(doc['key'])
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
                logger.debug("Generated {} {} tasks using {} seconds.".format(count, task_type, time() - start_time))
        logger.debug("Finished generating {} tasks.".format(task_type))

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

                put_back = True
                for msg_item in msg_queue_dict.items():
                    self.lock_tasks.acquire()
                    if self.tasks[task.id].status == 'not_assigned':
                        msg_item[1].put(json.dumps(msg))
                        put_back = False
                        self.lock_tasks.release()
                    else:
                        self.lock_tasks.release()
                        break
                if put_back:
                    self.tasks_queue.put(task)
            sleep(0.01)

    def broadcast_task(self, task):
        pass

    def receiver(self, worker_data):
        data = ''
        while True:
            try:
                data += str(worker_data.receiver_conn.recv(1024), 'utf-8')
                while data.find('\n') != -1:
                    first_pos = data.find('\n')
                    recv_json = json.loads(data[:first_pos])
                    if 'token' in recv_json and recv_json['token'] == config.token:
                        if recv_json['action'] == 'ask':
                            logger.debug(
                                "[*] Ask from Worker-{}: {} ".format(worker_data.worker_id,
                                                                     recv_json['data']['id_str']))
                            msg = {'task': 'save', 'id_str': recv_json['data']['id_str'], 'token': config.token}
                            worker_data.msg_queue.put(json.dumps(msg))
                    data = data[first_pos + 1:]

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
            worker_data.sender_conn.send(bytes(json.dumps(msg) + '\n', 'utf-8'))
            logger.debug('[*] Sent stream task to Worker-{}.'.format(worker_data.worker_id))
            while True:
                while not worker_data.msg_queue.empty():
                    msg = worker_data.msg_queue.get()
                    try:
                        worker_data.sender_conn.send(bytes(msg + '\n', 'utf-8'))
                        logger.debug("[*] Sent to Worker-{}: {} ".format(worker_data.worker_id, msg))
                    except socket.error:
                        worker_data.msg_queue.put(msg)
                sleep(0.01)
        except socket.error:
            self.remove_worker(worker_data)

    def run(self):
        self.update_db()
        self.check_db()
        self.check_views()
        threading.Thread(target=self.tcp_server).start()
        threading.Thread(target=self.conn_handler).start()
        # threading.Thread(target=self.tasks_generator).start()
        # threading.Thread(target=self.master).start()
