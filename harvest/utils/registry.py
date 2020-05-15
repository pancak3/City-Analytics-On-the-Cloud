import logging
import socket
import threading
import json
import queue
import traceback
import os

from os import kill, getpid
from signal import SIGUSR1
from cloudant.design_document import DesignDocument, Document
from time import sleep, time
from secrets import token_urlsafe
from collections import defaultdict
from utils.config import config
from utils.database import CouchDB
from utils.logger import get_logger

logger = get_logger('Registry', level_name=logging.DEBUG)


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
        self.token = token_urlsafe(13)
        self.ip = ip
        self.couch = CouchDB()
        self.client = self.couch.client

        self.conn_queue = queue.Queue()

        self.msg_queue_dict = defaultdict(queue.Queue)
        self.lock_msg_queue_dict = threading.Lock()

        self.worker_id = 0
        self.active_workers = set()
        self.api_using = set()
        self.worker_data = {}
        self.lock_worker = threading.Lock()

        self.friends_tasks = queue.Queue()
        self.timeline_tasks = queue.Queue()

        self.friends_tasks_updated_time = 0
        self.timeline_tasks_updated_time = 0
        self.lock_friends_tasks_updated_time = threading.Lock()
        self.lock_timeline_tasks_updated_time = threading.Lock()

    def get_worker_id(self):
        self.lock_worker.acquire()
        self.worker_id += 1
        id_tmp = self.worker_id
        self.active_workers.add(id_tmp)
        self.lock_worker.release()
        return id_tmp

    def update_db(self):
        if 'control' not in self.client.all_dbs():
            self.client.create_database('control')

        self.client['control'].create_document({
            '_id': 'registry',
            'ip': self.ip,
            'port': config.registry_port,
            'token': self.token
        })
        logger.debug('Updated registry info in database: {}:{}'.format(self.ip, config.registry_port))

    def check_views(self):
        # Make view result ascending
        # https://stackoverflow.com/questions/40463629

        # design_doc = Document(self.client['statuses'], '_design/stream')
        # if not design_doc.exists():
        #     design_doc = DesignDocument(self.client['statuses'], '_design/stream')
        #     map_func_statuses = 'function(doc) { ' \
        #                         '   if (!doc.hasOwnProperty("direct_stream")) {doc.direct_stream = False;}' \
        #                         '   if ( doc.direct_stream ) ' \
        #                         '   {emit(doc._id, true);}' \
        #                         '}'
        #
        #     design_doc.add_view('statues', map_func_statuses)
        #
        #     map_func_statuses_expanded = 'function(doc) { ' \
        #                                  '   if (!doc.hasOwnProperty("stream_status")) {doc.stream_status = False;}' \
        #                                  '   if ( doc.stream_status ) ' \
        #                                  '   {emit(doc._id, true);}' \
        #                                  '}'
        #     design_doc.add_view('statuses_expanded', map_func_statuses_expanded, partitioned=False)
        #     design_doc.save()
        #
        # design_doc = Document(self.client['users'], '_design/stream')
        # if not design_doc.exists():
        #     design_doc = DesignDocument(self.client['users'], '_design/stream')
        #     map_func_users = 'function(doc) {' \
        #                      '  if (!doc.hasOwnProperty("stream_user")) {doc.stream_user = false;}' \
        #                      '  if ( doc.stream_user) {emit(doc._id, true);}' \
        #                      '}'
        #     design_doc.add_view('users', map_func_users, partitioned=False)
        #     design_doc.save()

        design_doc = Document(self.client['users'], '_design/tasks')
        if not design_doc.exists():
            design_doc = DesignDocument(self.client['users'], '_design/tasks', partitioned=False)
            map_func_friends = 'function(doc) {' \
                               '    var date = new Date();' \
                               '    var timestamp = date.getTime() / 1000;' \
                               '    if (!doc.hasOwnProperty("friends_updated_at")) {doc.friends_updated_at = 0;}' \
                               '    if (!doc.hasOwnProperty("stream_user")) {doc.stream_user = false;}' \
                               '    if (doc.stream_user && timestamp - doc.friends_updated_at > ' \
                               + str(config.timeline_updating_window) + \
                               '                                          ) {' \
                               '        emit([doc.friends_updated_at, doc.inserted_time]);}' \
                               '}'
            design_doc.add_view('friends', map_func_friends)

            map_func_timeline = 'function(doc) {' \
                                '    var date = new Date();' \
                                '    var timestamp = date.getTime() / 1000;' \
                                '    if (!doc.hasOwnProperty("timeline_updated_at")) {' \
                                '        doc.timeline_updated_at = 0;' \
                                '    }' \
                                '    if (timestamp - doc.timeline_updated_at > ' \
                                + str(config.timeline_updating_window) + \
                                '                                          ) {' \
                                '      if (!doc.hasOwnProperty("stream_user")) {' \
                                '        doc.stream_user = false;' \
                                '      }' \
                                '       if (doc.stream_user) {' \
                                '        emit([doc.timeline_updated_at,0, doc.inserted_time,true],"stream_user");' \
                                '       }else{' \
                                '        emit([doc.timeline_updated_at,1, doc.inserted_time,false],"not_stream");' \
                                '      }' \
                                '    }' \
                                '}'
            design_doc.add_view('timeline', map_func_timeline)
            design_doc.save()

    def check_db(self, db_name):
        if db_name not in self.client.all_dbs():
            partitioned = True
            if db_name in {'control'}:
                partitioned = False
            self.client.create_database(db_name, partitioned=partitioned)
            logger.debug("[*] database-{} not in Couch; created.".format(db_name))

    def check_dbs(self):
        self.check_db('statuses')
        self.check_db('users')
        if 'control' in self.client.all_dbs():
            self.client['control'].delete()
        self.check_db('control')

    def tcp_server(self, lock):
        lock.acquire()
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # docker container inner ip is always 0.0.0.0
            s.bind(('0.0.0.0', config.registry_port))
            s.listen(1)
            logger.debug('TCP server started at {}:{}'.format(self.ip, config.registry_port))
            lock.release()
            while True:
                conn, addr = s.accept()
                self.conn_queue.put((conn, addr))
        except OSError as e:
            lock.release()
            logger.error('[!] Cannot bind to 0.0.0.0:{}.'.format(config.registry_port))
            kill(getpid(), SIGUSR1)

    def conn_handler(self):
        logger.info("ConnectionHandler started.")
        while True:
            (conn, addr) = self.conn_queue.get()
            threading.Thread(target=self.registry_msg_handler, args=(conn, addr,)).start()

    def registry_msg_handler(self, conn, addr):
        data = ''
        while True:
            try:
                data += conn.recv(1024).decode('utf-8')
                while data.find('\n') != -1:
                    first_pos = data.find('\n')
                    recv_json = json.loads(data[:first_pos])
                    if 'token' in recv_json and recv_json['token'] == self.token:
                        del recv_json['token']
                        if recv_json['action'] == 'init':
                            if recv_json['role'] == 'sender':
                                # Worker send API key hashes to ask which it can use
                                valid_api_key_hash = None
                                self.lock_worker.acquire()
                                for api_key_hash in recv_json['api_keys_hashes']:
                                    if api_key_hash not in self.api_using:
                                        valid_api_key_hash = api_key_hash
                                        break
                                self.lock_worker.release()
                                if valid_api_key_hash is None:
                                    msg = {'token': self.token, 'res': 'deny', 'msg': 'no valid api key'}
                                else:
                                    worker_id = self.get_worker_id()
                                    worker_data = WorkerData(worker_id, conn, addr, valid_api_key_hash)
                                    self.lock_worker.acquire()
                                    self.worker_data[worker_id] = worker_data
                                    self.api_using.add(valid_api_key_hash)
                                    self.lock_worker.release()

                                    # msg queue dict used to broadcast
                                    self.lock_msg_queue_dict.acquire()
                                    self.msg_queue_dict[worker_id] = worker_data.msg_queue
                                    self.lock_msg_queue_dict.release()

                                    threading.Thread(target=self.master_receiver, args=(worker_data,)).start()
                                    self.update_available_api_keys_num()
                                    msg = {'token': self.token, 'res': 'use_api_key',
                                           'api_key_hash': valid_api_key_hash, 'worker_id': worker_id}

                                conn.send(bytes(json.dumps(msg) + '\n', 'utf-8'))

                            elif recv_json['role'] == 'receiver':
                                worker_id = recv_json['worker_id']

                                self.lock_worker.acquire()
                                self.worker_data[worker_id].sender_conn = conn
                                self.worker_data[worker_id].sender_addr = addr
                                worker_data = self.worker_data[worker_id]
                                self.lock_worker.release()

                                threading.Thread(target=self.master_sender, args=(worker_data,)).start()

                    data = data[first_pos + 1:]

            except json.JSONDecodeError as e:
                pass
            except socket.error as e:
                pass

    def tasks_generator(self):
        logger.info("TaskGenerator started.")
        to_sleep = config.tasks_generating_window
        self.generate_tasks('friends')
        self.generate_tasks('timeline')
        while True:
            if to_sleep < 0 or (self.timeline_tasks.empty() and self.friends_tasks.empty()):
                self.lock_timeline_tasks_updated_time.acquire()
                if int(time()) - self.timeline_tasks_updated_time > 120:
                    self.lock_timeline_tasks_updated_time.release()
                    if not self.friends_tasks.qsize():
                        self.generate_tasks('friends')
                    if not self.timeline_tasks.qsize():
                        self.generate_tasks('timeline')
                else:
                    self.lock_timeline_tasks_updated_time.release()
                to_sleep = config.tasks_generating_window
            sleep(5)
            to_sleep -= 5
            # To avoid session expired.

    def generate_tasks(self, task_type):
        self.client.connect()
        if task_type == 'friends':
            self.friends_tasks = queue.Queue()
            self.lock_friends_tasks_updated_time.acquire()
        elif task_type == 'timeline':
            self.timeline_tasks = queue.Queue()
            self.lock_timeline_tasks_updated_time.acquire()
        else:
            return 0
        start_time = time()
        logger.debug("Start to generate {} tasks.".format(task_type))
        if 'users' in self.client.all_dbs():
            count = 0
            result = self.client['users'].get_view_result('_design/tasks',
                                                          view_name=task_type,
                                                          limit=config.max_tasks_num).all()
            if task_type == 'friends':
                for doc in result:
                    count += 1
                    self.friends_tasks.put(doc['id'])
            if task_type == 'timeline':
                for i in range(0, len(result), config.max_ids_single_task):
                    timeline_tasks = [[doc['id'], doc['key'][3]] for doc in
                                      result[:config.max_tasks_num][i:i + config.max_ids_single_task]]
                    count += len(timeline_tasks)
                    self.timeline_tasks.put(timeline_tasks)

            logger.debug("Generated {} {} tasks in {:.2} seconds.".format(count, task_type, time() - start_time))
            if task_type == 'friends':
                self.friends_tasks_updated_time = int(time())
                self.lock_friends_tasks_updated_time.release()
            elif task_type == 'timeline':
                self.timeline_tasks_updated_time = int(time())
                self.lock_timeline_tasks_updated_time.release()
            return count
        logger.debug("Finished generating {} tasks.".format(task_type))
        if task_type == 'friends':
            self.friends_tasks_updated_time = int(time())
            self.lock_friends_tasks_updated_time.release()
        elif task_type == 'timeline':
            self.timeline_tasks_updated_time = int(time())
            self.lock_timeline_tasks_updated_time.release()
        return 0

    def master_receiver(self, worker_data):
        data = ''
        active_time = int(time())
        while True:
            try:
                data += worker_data.receiver_conn.recv(1024).decode('utf-8')
                while data.find('\n') != -1:
                    first_pos = data.find('\n')
                    recv_json = json.loads(data[:first_pos])
                    if 'token' in recv_json and recv_json['token'] == self.token:
                        del recv_json['token']
                        active_time = int(time())
                        if recv_json['action'] == 'ping':
                            msg = {'token': self.token, 'task': 'pong'}
                            worker_data.msg_queue.put(json.dumps(msg))
                        if recv_json['action'] == 'ask_for_task':
                            logger.debug(
                                "Got ask for task from Worker-{}: {}".format(recv_json['worker_id'], recv_json))
                            if 'friends' in recv_json and 'followers' in recv_json and recv_json['friends'] > 0 and \
                                    recv_json['followers'] > 0:
                                logger.debug("[-] Has {} friends tasks in queue.".format(self.friends_tasks.qsize()))
                                if self.friends_tasks.empty():
                                    self.lock_friends_tasks_updated_time.acquire()
                                    if self.friends_tasks.empty() \
                                            and int(time()) - self.friends_tasks_updated_time > 120:
                                        self.lock_friends_tasks_updated_time.release()
                                        self.generate_tasks('friends')
                                    else:
                                        self.lock_friends_tasks_updated_time.release()
                                else:
                                    try:
                                        user_id = self.friends_tasks.get(timeout=0.01)
                                        msg = {'token': self.token, 'task': 'task', 'friends_ids': [user_id]}
                                        worker_data.msg_queue.put(json.dumps(msg))
                                        del msg['token']
                                        logger.debug(
                                            "[*] Sent task to Worker-{}: {} ".format(worker_data.worker_id, msg))
                                    except queue.Empty:
                                        pass
                            if 'timeline' in recv_json and recv_json['timeline'] > 0:
                                logger.debug("[-] Has {} timeline tasks in queue.".format(self.timeline_tasks.qsize()))
                                if self.timeline_tasks.empty():
                                    self.lock_timeline_tasks_updated_time.acquire()
                                    if self.timeline_tasks.empty() \
                                            and int(time()) - self.timeline_tasks_updated_time > 120:
                                        self.lock_timeline_tasks_updated_time.release()
                                        self.generate_tasks('timeline')
                                    else:
                                        self.lock_timeline_tasks_updated_time.release()
                                else:
                                    try:
                                        timeline_tasks = self.timeline_tasks.get(timeout=0.01)
                                        msg = {'token': self.token, 'task': 'task', 'timeline_ids': timeline_tasks}
                                        worker_data.msg_queue.put(json.dumps(msg))
                                        logger.debug(
                                            "[*] Sent task to Worker-{}: {} ".format(worker_data.worker_id, recv_json))
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
            self.lock_worker.acquire()
            if worker_data.worker_id not in self.active_workers:
                self.lock_worker.release()
                break
            self.lock_worker.release()
            sleep(0.01)

    def master_sender(self, worker_data):
        logger.debug('[-] Worker-{} connected.'.format(worker_data.worker_id))
        try:
            msg = {"task": "stream",
                   "data": {
                       "locations": config.victoria_bbox},
                   "token": self.token
                   }
            worker_data.sender_conn.send(bytes(json.dumps(msg) + '\n', 'utf-8'))
            logger.debug('[*] Sent stream task to Worker-{}.'.format(worker_data.worker_id))
            while True:
                msg = worker_data.msg_queue.get()
                try:
                    worker_data.sender_conn.send(bytes(msg + '\n', 'utf-8'))
                    # logger.debug("[*] Sent to Worker-{}: {} ".format(worker_data.worker_id, msg))
                except socket.error:
                    worker_data.msg_queue.put(msg)
                # Check worker status
                self.lock_worker.acquire()
                if worker_data.worker_id not in self.active_workers:
                    self.lock_worker.release()
                    break
                self.lock_worker.release()
                sleep(1)
        except socket.error as e:
            self.remove_worker(worker_data, e)

    def remove_worker(self, worker_data, e):
        self.lock_worker.acquire()
        del self.worker_data[worker_data.worker_id]
        self.lock_worker.release()

        self.lock_msg_queue_dict.acquire()
        del self.msg_queue_dict[worker_data.worker_id]
        self.lock_msg_queue_dict.release()

        self.lock_worker.acquire()
        self.active_workers.remove(worker_data.worker_id)
        self.api_using.remove(worker_data.api_key_hash)
        remaining = [worker_id for worker_id in self.active_workers]
        self.lock_worker.release()

        # https://stackoverflow.com/questions/409783
        worker_data.sender_conn.close()
        worker_data.receiver_conn.close()
        self.update_available_api_keys_num()
        logger.warning(
            "[-] Worker-{} exit: {}(remaining active workers:{})".format(worker_data.worker_id, e, remaining))

    def update_available_api_keys_num(self):
        with open("twitter.json") as t:
            t_json = json.loads(t.read())
            api_keys_num = len(t_json)
            self.lock_worker.acquire()
            occupied_api_keys_num = len(self.active_workers)
            self.lock_worker.release()
            try:
                if 'available_api_keys_num' not in self.client['control']:
                    self.client['control'].create_document({
                        '_id': 'available_api_keys_num',
                        'available': api_keys_num - occupied_api_keys_num,
                        'occupied': occupied_api_keys_num,
                        'total': api_keys_num,
                        'updated_at': int(time())
                    })
                else:
                    doc = self.client['control']['available_api_keys_num']
                    doc['available'] = api_keys_num - occupied_api_keys_num
                    doc['occupied'] = occupied_api_keys_num
                    doc['total'] = api_keys_num
                    doc['updated_at'] = int(time())
                    self.save_doc(doc)
            except Exception:
                logger.error('[!] CouchDB err: \n{}'.format(traceback.format_exc()))
                os._exit(1)

    @staticmethod
    def save_doc(doc):
        try:
            doc.save()
        except Exception:
            # prevent unexpected err
            pass

    def save_pid(self):
        # Record PID for daemon
        self.pid = getpid()
        try:
            f = open('registry.pid', 'w+')
            f.write(str(self.pid))
            f.close()
            logger.info('Starting with PID: {}'.format(self.pid))
        except Exception:
            logger.error('Exit! \n{}'.format(traceback.format_exc()))

    def run(self):
        lock = threading.Lock()
        threading.Thread(target=self.tcp_server, args=(lock,)).start()
        lock.acquire()
        lock.release()
        # self.save_pid()
        self.check_dbs()
        self.update_db()
        self.check_views()
        threading.Thread(target=self.conn_handler).start()
        threading.Thread(target=self.tasks_generator).start()
        threading.Thread(target=self.update_available_api_keys_num).start()
