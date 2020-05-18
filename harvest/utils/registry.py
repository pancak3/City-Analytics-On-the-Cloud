import logging
import socket
import threading
import json
import queue
import traceback
import os

from abc import abstractmethod
from os import kill, getpid
from signal import SIGUSR1
from cloudant.design_document import DesignDocument, Document
from time import sleep, time
from secrets import token_urlsafe
from collections import defaultdict
from utils.config import Config
from utils.database import CouchDB
from utils.logger import get_logger


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
        self.active_time = ActiveTime()


class ActiveTime:
    def __init__(self):
        self.time = int(time())
        self.lock = threading.Lock()

    def update(self):
        self.lock.acquire()
        self.time = int(time())
        self.lock.release()

    def get(self):
        self.lock.acquire()
        t = self.time = int(time())
        self.lock.release()
        return t


class Registry:
    def __init__(self, ip):
        self.logger = get_logger('Registry', level_name=logging.DEBUG)

        self.config = Config()
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

        self.lock_available_api_keys_num = threading.Lock()

    def get_worker_id(self):
        self.lock_worker.acquire()
        self.worker_id += 1
        id_tmp = self.worker_id
        self.active_workers.add(id_tmp)
        self.lock_worker.release()
        return id_tmp

    def check_views(self):
        # Make view result ascending
        # https://stackoverflow.com/questions/40463629

        design_doc = Document(self.client['users'], '_design/tasks')
        if not design_doc.exists():
            design_doc = DesignDocument(self.client['users'], '_design/tasks', partitioned=False)
            map_func_friends = 'function(doc) {' \
                               '    var date = new Date();' \
                               '    var timestamp = date.getTime() / 1000;' \
                               '    if (!doc.hasOwnProperty("friends_updated_at")) {doc.friends_updated_at = 0;}' \
                               '    if (!doc.hasOwnProperty("stream_user")) {doc.stream_user = false;}' \
                               '    if (doc.stream_user && timestamp - doc.friends_updated_at > ' \
                               + str(self.config.timeline_updating_window) + \
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
                                + str(self.config.timeline_updating_window) + \
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

        design_doc = Document(self.client['control'], '_design/api-global')
        if not design_doc.exists():
            design_doc = DesignDocument(self.client['control'], '_design/api-global', partitioned=False)
            map_func_running_worker = 'function(doc) {' \
                                      ' if(doc.is_running){' \
                                      ' emit(doc._id, doc.api_key_hash);' \
                                      ' }' \
                                      '}'
            design_doc.add_view('running-worker', map_func_running_worker)

            design_doc.save()

    def check_db(self, db_name):
        if db_name not in self.client.all_dbs():
            partitioned = True
            if db_name in {'control'}:
                partitioned = False
            self.client.create_database(db_name, partitioned=partitioned)
            self.logger.debug("[*] database-{} not in Couch; created.".format(db_name))

    def check_dbs(self):
        for dn_name in {'statuses', 'users', 'control'}:
            self.check_db(dn_name)

    def tcp_server(self, lock):
        lock.acquire()
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # docker container inner ip is always 0.0.0.0
            s.bind(('0.0.0.0', self.config.registry_port))
            s.listen(1)
            self.logger.debug('TCP server started at {}:{}'.format(self.ip, self.config.registry_port))
            lock.release()
            while True:
                conn, addr = s.accept()
                self.conn_queue.put((conn, addr))
        except OSError as e:
            lock.release()
            self.logger.error('[!] Cannot bind to 0.0.0.0:{}.'.format(self.config.registry_port))
            os._exit(0)

    def conn_handler(self):
        self.logger.info("ConnectionHandler started.")
        while True:
            (conn, addr) = self.conn_queue.get()
            threading.Thread(target=self.registry_msg_handler, args=(conn, addr,)).start()

    def keep_alive(self, worker_data):
        while True:
            if int(time()) - worker_data.active_time.get() > self.config.max_heartbeat_lost_time:
                self.remove_worker(worker_data,
                                   'Lost heartbeat for {} seconds.'.format(self.config.max_heartbeat_lost_time))
                break
            sleep(5)
            # print(worker_data.worker_id, int(time()) - active_time[0])

    def receiver(self, worker_data):
        buffer_data = ['']
        threading.Thread(target=self.keep_alive, args=(worker_data,)).start()

        while True:
            try:
                self.handle_receive_buffer_data(buffer_data, worker_data)
            except socket.error as e:
                self.remove_worker(worker_data, e)
                break
            except json.JSONDecodeError as e:
                self.remove_worker(worker_data, e)
                break
            if not self.is_worker_active(worker_data):
                break

    def handle_receive_buffer_data(self, buffer_data, worker_data):
        buffer_data[0] += worker_data.receiver_conn.recv(1024).decode('utf-8')
        while buffer_data[0].find('\n') != -1:
            first_pos = buffer_data[0].find('\n')
            recv_json = json.loads(buffer_data[0][:first_pos])
            # self.logger.info("Received: {}".format(recv_json))
            if 'token' in recv_json and recv_json['token'] == self.token:
                worker_data.active_time.update()
                del recv_json['token']
                if recv_json['action'] == 'ping':
                    self.handle_action_ping(worker_data)
                if recv_json['action'] == 'ask_for_task':
                    self.handle_action_ask_for_task(worker_data, recv_json)

            buffer_data[0] = buffer_data[0][first_pos + 1:]

    def handle_action_ping(self, worker_data):
        msg = {'token': self.token, 'task': 'pong'}
        worker_data.msg_queue.put(json.dumps(msg))

    def handle_action_init_sender(self, recv_json, conn, addr):
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

            threading.Thread(target=self.receiver, args=(worker_data,)).start()
            self.update_worker_info_in_db(worker_id, valid_api_key_hash, True)
            msg = {'token': self.token, 'res': 'use_api_key',
                   'api_key_hash': valid_api_key_hash, 'worker_id': worker_id}

        conn.send(bytes(json.dumps(msg) + '\n', 'utf-8'))

    def handle_action_init_receiver(self, recv_json, conn, addr):
        worker_id = recv_json['worker_id']

        self.lock_worker.acquire()
        self.worker_data[worker_id].sender_conn = conn
        self.worker_data[worker_id].sender_addr = addr
        worker_data = self.worker_data[worker_id]
        self.lock_worker.release()

        threading.Thread(target=self.sender, args=(worker_data,)).start()

    def handle_action_ask_for_task(self, worker_data, recv_json):
        self.logger.debug("Got ask for task from Worker-{}: {}".format(recv_json['worker_id'], recv_json))
        if 'friends' in recv_json and 'followers' in recv_json \
                and recv_json['friends'] > 0 and recv_json['followers'] > 0:
            self.handle_task_friends(worker_data)
        if 'timeline' in recv_json and recv_json['timeline'] > 0:
            self.handle_task_timeline(worker_data)

    def handle_task_friends(self, worker_data):
        self.logger.debug("[-] Has {} friends tasks in queue.".format(self.friends_tasks.qsize()))

        try:
            self.generate_friends_task()
            user_id = self.friends_tasks.get(timeout=0.01)
            msg = {'token': self.token, 'task': 'task', 'friends_ids': [user_id]}
            worker_data.msg_queue.put(json.dumps(msg))
            del msg['token']
            self.logger.debug("[*] Sent task to Worker-{}: {} ".format(worker_data.worker_id, msg))
        except queue.Empty:
            pass

    def handle_task_timeline(self, worker_data):
        self.logger.debug("[-] Has {} timeline tasks in queue.".format(self.timeline_tasks.qsize()))
        try:
            self.generate_timeline_task()

            timeline_tasks = self.timeline_tasks.get(timeout=0.01)
            msg = {'token': self.token, 'task': 'task', 'timeline_ids': timeline_tasks}
            worker_data.msg_queue.put(json.dumps(msg))
            self.logger.debug(
                "[*] Sent task to Worker-{}: {} ".format(worker_data.worker_id, msg))
        except queue.Empty:
            pass

    def sender(self, worker_data):
        self.logger.debug('[-] Worker-{} connected.'.format(worker_data.worker_id))
        try:
            self.send_stream_task(worker_data)
            while True:
                if not self.is_worker_active(worker_data):
                    break
                self.send_msg_in_queue(worker_data)
        except socket.error as e:
            self.remove_worker(worker_data, e)

    def send_stream_task(self, worker_data):
        msg = {"task": "stream",
               "data": {
                   "locations": self.config.victoria_bbox},
               "token": self.token
               }
        worker_data.sender_conn.send(bytes(json.dumps(msg) + '\n', 'utf-8'))
        self.logger.debug('[*] Sent stream task to Worker-{}.'.format(worker_data.worker_id))

    # @staticmethod
    def send_msg_in_queue(self, worker_data):
        msg = worker_data.msg_queue.get()
        try:
            worker_data.sender_conn.send(bytes(msg + '\n', 'utf-8'))
            # self.logger.debug("[*] Sent to Worker-{}: {} ".format(worker_data.worker_id, msg))
        except socket.error:
            worker_data.msg_queue.put(msg)

    def is_worker_active(self, worker_data):
        # Check if worker in active workers
        self.lock_worker.acquire()
        if worker_data.worker_id not in self.active_workers:
            self.lock_worker.release()
            return False
        self.lock_worker.release()
        return True

    def remove_worker_data(self, worker_data):
        self.lock_worker.acquire()
        if worker_data.worker_id in self.worker_data:
            del self.worker_data[worker_data.worker_id]
        self.lock_worker.release()

    def remove_msg_queue(self, worker_data):
        self.lock_msg_queue_dict.acquire()
        del self.msg_queue_dict[worker_data.worker_id]
        self.lock_msg_queue_dict.release()

    def deactivate_worker(self, worker_data):
        self.lock_worker.acquire()
        self.active_workers.remove(worker_data.worker_id)
        self.api_using.remove(worker_data.api_key_hash)
        remaining = [worker_id for worker_id in self.active_workers]
        self.lock_worker.release()
        return remaining

    @staticmethod
    def close_socket_connection(worker_data):
        # https://stackoverflow.com/questions/409783
        if worker_data.sender_conn is not None:
            worker_data.sender_conn.close()
        if worker_data.receiver_conn is not None:
            worker_data.receiver_conn.close()

    def remove_worker(self, worker_data, e):
        self.remove_worker_data(worker_data)
        self.remove_msg_queue(worker_data)
        remaining = self.deactivate_worker(worker_data)
        self.close_socket_connection(worker_data)
        self.update_worker_info_in_db(worker_data.worker_id, worker_data.api_key_hash, False)
        self.remove_worker_info_from_db(worker_data)
        self.logger.warning("[-] Worker-{} exit: "
                            "{}(remaining active workers:{})".format(worker_data.worker_id, e, remaining))

    def remove_worker_info_from_db(self, worker_data):
        try:
            self.client['control']["worker-" + str(worker_data.worker_id)].delete()
        except Exception:
            traceback.format_exc()

    def registry_msg_handler(self, conn, addr):
        self.logger.info("registry start msg handler")
        data = ''
        while True:
            try:
                data += conn.recv(1024).decode('utf-8')
                while data.find('\n') != -1:
                    first_pos = data.find('\n')
                    recv_json = json.loads(data[:first_pos])
                    if 'token' in recv_json and recv_json['token'] == self.token:
                        # del recv_json['token']
                        if recv_json['action'] == 'init':
                            if recv_json['role'] == 'sender':
                                self.handle_action_init_sender(recv_json, conn, addr)
                            elif recv_json['role'] == 'receiver':
                                self.handle_action_init_receiver(recv_json, conn, addr)
                    data = data[first_pos:]
            except json.JSONDecodeError:
                traceback.format_exc()
                break
            except socket.error:
                traceback.format_exc()
                break
            except Exception as e:
                self.logger.warning(e)
                traceback.format_exc()
                break

    def tasks_generator(self):
        self.logger.info("TaskGenerator started.")
        while True:
            self.generate_timeline_task()
            self.generate_friends_task()
            sleep(5)

    def generate_friends_task(self):
        if not self.friends_tasks.empty():
            return
        self.lock_friends_tasks_updated_time.acquire()
        if not self.friends_tasks.empty():
            return
        if int(time()) - self.friends_tasks_updated_time < 5:
            return
        try:
            self.client.connect()
            if 'users' in self.client.all_dbs():
                count = 0
                result = self.client['users'].get_view_result('_design/tasks', view_name='friends',
                                                              limit=self.config.max_tasks_num).all()
                for doc in result:
                    count += 1
                    self.friends_tasks.put(doc['id'])
                self.logger.debug("Generated {} friends tasks".format(count))
                self.friends_tasks_updated_time = int(time())

        except Exception:
            traceback.format_exc()
        self.lock_friends_tasks_updated_time.release()

    def generate_timeline_task(self):
        if not self.timeline_tasks.empty():
            return
        self.lock_timeline_tasks_updated_time.acquire()
        if not self.timeline_tasks.empty():
            return
        if int(time()) - self.timeline_tasks_updated_time < 5:
            self.lock_timeline_tasks_updated_time.release()
            return
        try:
            self.client.connect()
            if 'users' in self.client.all_dbs():
                count = 0
                result = self.client['users'].get_view_result('_design/tasks', view_name='timeline',
                                                              limit=self.config.max_tasks_num).all()

                for i in range(0, len(result), self.config.max_ids_single_task):
                    timeline_tasks = [[doc['id'], doc['key'][3]] for doc in
                                      result[:self.config.max_tasks_num][i:i + self.config.max_ids_single_task]]
                    count += len(timeline_tasks)
                    self.timeline_tasks.put(timeline_tasks)

                self.timeline_tasks_updated_time = int(time())
                self.logger.debug("Generated {} friends tasks.".format(count))

        except Exception:
            traceback.format_exc()

        self.lock_timeline_tasks_updated_time.release()

    def update_doc(self, db, key, values):
        try:
            if key not in db:
                db.create_document(values)
            else:
                doc = db[key]
                del values['_id']
                for (k, v) in values.items():
                    doc.update_field(action=doc.field_set, field=k, value=v)
            self.logger.debug('Updated {} info in database'.format(key))
        except Exception:
            traceback.format_exc()

    def update_registry_info(self):
        # update registry info
        values = {
            '_id': 'registry',
            'ip': self.ip,
            'port': self.config.registry_port,
            'token': self.token,
            'updated_at': int(time())

        }
        self.update_doc(self.client['control'], 'registry', values)

    def remove_all_worker_info(self):
        try:
            for doc in self.client['control']:
                if 'worker' in doc['_id']:
                    doc.delete()
        except Exception:
            traceback.format_exc()

    def update_worker_info_in_db(self, worker_id, api_key_hash, is_running):
        values = {
            '_id': 'worker-' + str(worker_id),
            'api_key_hash': api_key_hash,
            'is_running': is_running,
            'updated_at': int(time())
        }
        self.update_doc(self.client['control'], 'worker-' + str(worker_id), values)

    def run(self):
        lock = threading.Lock()
        threading.Thread(target=self.conn_handler).start()
        threading.Thread(target=self.tcp_server, args=(lock,)).start()
        lock.acquire()
        lock.release()
        # self.save_pid()
        self.check_dbs()
        self.update_registry_info()
        self.remove_all_worker_info()
        self.check_views()

        threading.Thread(target=self.tasks_generator).start()
