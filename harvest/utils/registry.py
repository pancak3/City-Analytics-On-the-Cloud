import logging
import socket
import threading
import json
import queue
import hashlib
import traceback
import os
import psutil

from os import kill, getpid
from signal import SIGUSR1
from cloudant.design_document import DesignDocument, Document
from time import sleep, time
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
        self.pid = None
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

        self.worker_id = 0
        self.active_workers = set()
        self.lock_worker_id = threading.Lock()

        self.worker_data = {}
        self.lock_worker_data = threading.Lock()

        self.tasks_friends = queue.Queue()
        self.tasks_timeline = queue.Queue()

    def get_worker_id(self):
        self.lock_worker_id.acquire()
        self.worker_id += 1
        id_tmp = self.worker_id
        self.active_workers.add(id_tmp)
        self.lock_worker_id.release()
        return id_tmp

    def update_db(self):
        # if 'control' not in self.client.all_dbs():
        if not self.client['control'].exists():
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

        design_doc = Document(self.client['stream_users'], '_design/stream_user_timeline')
        if not design_doc.exists():
            design_doc = DesignDocument(self.client['stream_users'], '_design/stream_user_timeline')
            map_fun = 'function(doc){var date = new Date();var timestamp = date.getTime() / 1000;if (timestamp - doc.timeline_updated_at > 172800){emit(doc._id, doc.timeline_updated_at);}}'
            design_doc.add_view('need_updating', map_fun)
            design_doc.save()

    def check_db(self):
        if not self.client['statuses'].exists():
            self.client.create_database('statuses')
            logger.debug("[*] Statuses table not in database; created.")
        if not self.client['stream_users'].exists():
            self.client.create_database('stream_users')
            logger.debug("[*] Stream_users table not in database; created.")
        if not self.client['all_users'].exists():
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
            kill(self.pid, SIGUSR1)

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

                    data = data[first_pos + 1:]

            except json.JSONDecodeError as e:
                pass
            except socket.error as e:
                pass

    def tasks_generator(self):
        logger.info("[*] TaskGenerator started.")
        while True:
            if not self.generate_tasks('stream_users', 'stream_user_timeline'):
                self.generate_tasks('all_users', 'timeline')
            self.generate_tasks('stream_users', 'friends')
            to_sleep = (config.timeline_updating_window + config.friends_updating_window) / 4
            # to_sleep = 3600 * 2
            logger.info("[*] TaskGenerator waits for {} seconds.".format(to_sleep))
            sleep(to_sleep)
            # To avoid session expired.
            self.couch.connect()

    def generate_tasks(self, user_db_name, task_type):
        if task_type == 'friends':
            self.tasks_friends = queue.Queue()
        elif task_type == 'timeline':
            self.tasks_timeline = queue.Queue()
        start_time = time()
        logger.debug("Start to generate {} tasks.".format(task_type))
        # if user_db_name in self.client.all_dbs():
        if self.client[user_db_name].exists():
            count = 0
            result = self.client[user_db_name].get_view_result('_design/' + task_type, 'need_updating')
            for doc in result:
                count += 1
                if task_type == 'friends':
                    self.tasks_friends.put(doc['key'])
                elif task_type == 'timeline':
                    self.tasks_timeline.put(doc['key'])
                elif task_type == 'stream_user_timeline':
                    self.tasks_timeline.put(doc['key'])
            logger.debug("Generated {} {} tasks using {} seconds.".format(count, task_type, time() - start_time))
            return count
        logger.debug("Finished generating {} tasks.".format(task_type))
        return 0

    def receiver(self, worker_data):
        data = ''
        active_time = int(time())
        while True:
            try:
                data += str(worker_data.receiver_conn.recv(1024), 'utf-8')
                while data.find('\n') != -1:
                    first_pos = data.find('\n')
                    recv_json = json.loads(data[:first_pos])
                    if 'token' in recv_json and recv_json['token'] == config.token:
                        if recv_json['action'] == 'ping':
                            msg = {'token': config.token, 'task': 'pong'}
                            worker_data.msg_queue.put(json.dumps(msg))
                            active_time = int(time())
                        if recv_json['action'] == 'ask_for_task':
                            logger.debug(
                                "Got ask for task from Worker-{}: {}".format(recv_json['worker_id'], recv_json))
                            if 'friends' in recv_json and 'followers' in recv_json and recv_json['friends'] > 0 and \
                                    recv_json['followers'] > 0:
                                logger.debug("[-] Has {} friends tasks in queue.".format(self.tasks_friends.qsize()))
                                try:
                                    user_id = self.tasks_friends.get(timeout=0.01)
                                    msg = {'token': config.token, 'task': 'task', 'friends_ids': [user_id]}
                                    worker_data.msg_queue.put(json.dumps(msg))
                                    logger.debug(
                                        "[*] Sent task to Worker-{}: {} ".format(worker_data.worker_id,
                                                                                 json.dumps(msg)))
                                except queue.Empty:
                                    pass
                            if 'timeline' in recv_json and recv_json['timeline'] > 0:
                                logger.debug("[-] Has {} timeline tasks in queue.".format(self.tasks_timeline.qsize()))
                                try:
                                    user_id = self.tasks_timeline.get(timeout=0.01)
                                    msg = {'token': config.token, 'task': 'task', 'timeline_ids': [user_id]}
                                    worker_data.msg_queue.put(json.dumps(msg))
                                    logger.debug(
                                        "[*] Sent task to Worker-{}: {} ".format(worker_data.worker_id,
                                                                                 json.dumps(msg)))
                                except queue.Empty:
                                    pass
                    data = data[first_pos + 1:]

            except socket.error as e:
                self.remove_worker(worker_data, e)
                break
            except json.JSONDecodeError as e:
                self.remove_worker(worker_data, e)
                break
            if int(time()) - active_time > config.max_heartbeat_lost_time:
                self.remove_worker(worker_data, 'Lost heartbeat for {} seconds.'.format(config.max_heartbeat_lost_time))
                break
            # Check worker status
            self.lock_worker_id.acquire()
            if worker_data.worker_id not in self.active_workers:
                self.lock_worker_id.release()
                break
            self.lock_worker_id.release()
            sleep(0.01)

    def sender(self, worker_data):
        logger.debug('[-] Worker-{} connected.'.format(worker_data.worker_id))
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
                        # logger.debug("[*] Sent to Worker-{}: {} ".format(worker_data.worker_id, msg))
                    except socket.error:
                        worker_data.msg_queue.put(msg)
                # Check worker status
                self.lock_worker_id.acquire()
                if worker_data.worker_id not in self.active_workers:
                    self.lock_worker_id.release()
                    break
                self.lock_worker_id.release()
                sleep(0.01)
        except socket.error as e:
            self.remove_worker(worker_data, e)

    def remove_worker(self, worker_data, e):
        self.lock_worker_data.acquire()
        del self.worker_data[worker_data.worker_id]
        self.lock_worker_data.release()

        self.lock_msg_queue_dict.acquire()
        del self.msg_queue_dict[worker_data.worker_id]
        self.lock_msg_queue_dict.release()

        self.lock_api_key_worker.acquire()
        del self.api_key_worker[worker_data.api_key_hash]
        self.lock_api_key_worker.release()

        self.lock_worker_id.acquire()
        self.active_workers.remove(worker_data.worker_id)
        self.lock_worker_id.release()

        logger.warning("[-] Worker-{} exit: {}".format(worker_data.worker_id, e))

    def update_available_api_keys_num(self):
        while True:
            with open("twitter.json") as t:
                t_json = json.loads(t.read())
                api_keys_num = len(t_json)
                self.lock_worker_id.acquire()
                occupied_api_keys_num = len(self.active_workers)
                self.lock_worker_id.release()
                try:
                    if 'available_api_keys_num' not in self.client['control']:
                        self.client['control'].create_document({
                            '_id': 'available_api_keys_num',
                            'value': api_keys_num - occupied_api_keys_num
                        })
                    else:
                        doc = self.client['control']['available_api_keys_num']
                        doc['value'] = api_keys_num - occupied_api_keys_num
                        doc.save()
                except Exception:
                    logger.error('[!] CouchDB err: \n{}'.format(traceback.format_exc()))
                    kill(self.pid, SIGUSR1)
            sleep(5)

    @staticmethod
    def check_pid():
        # https://stackoverflow.com/questions/568271
        if os.path.exists('registry.pid'):
            f = open('registry.pid')
            pid = int(f.read())
            f.close()

            if pid and psutil.pid_exists(pid):
                logger.info('[-] There is running registry-PID-.'.format(pid))
                user_input = input('Do you want to terminate it? (y/n)')
                if user_input in {'y', 'Y', 'yes'}:
                    kill(pid, SIGUSR1)
                else:
                    logger.info('[-] Let registry-PID-{} run.'.format(pid))
            else:
                logger.info('[-] No running registry.')

    def save_pid(self):
        # Record PID for daemon
        self.pid = getpid()
        try:
            f = open('registry.pid', 'w+')
            f.write(str(self.pid))
            f.close()
            logger.info('[-] Starting Registry PID: {}'.format(self.pid))
        except Exception:
            logger.error('[!] Exit! \n{}'.format(traceback.format_exc()))

    def run(self):
        self.check_pid()
        self.save_pid()
        self.update_db()
        self.check_db()
        self.check_views()
        threading.Thread(target=self.tcp_server).start()
        threading.Thread(target=self.conn_handler).start()
        threading.Thread(target=self.tasks_generator).start()
        threading.Thread(target=self.update_available_api_keys_num).start()
