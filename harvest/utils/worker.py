import logging
import socket
import threading
import json
import queue
import tweepy

from os import kill, getpid
from signal import SIGUSR1
from math import ceil
from time import sleep, time
from collections import defaultdict
from urllib3.exceptions import ReadTimeoutError, ProtocolError
from utils.config import config
from utils.database import CouchDB
from utils.crawlers import Crawler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Worker')
logger.setLevel(logging.DEBUG)


class Task:
    def __init__(self, _type, _ids):
        self.type = _type
        self.user_ids = _ids


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
        self.reg_ip, self.reg_port, self.token = self.get_registry()
        self.socket_send, self.socket_recv, valid_api_key_hash, worker_id = self.connect_reg()
        self.crawler.init(valid_api_key_hash)
        self.worker_id = worker_id
        self.task_queue = queue.Queue()
        self.lock_rate_limit = threading.Lock()
        self.active_time = True
        self.lock_active_time = threading.Lock()
        self.has_task = False
        self.lock_exit = threading.Lock()

    def get_registry(self):
        registry = self.client['control']['registry']
        return registry['ip'], registry['port'], registry['token']

    def connect_reg(self):
        reg_ip, reg_port, token = self.reg_ip, self.reg_port, self.token

        try:
            socket_sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket_sender.connect((reg_ip, reg_port))
            msg = {'action': 'init', 'role': 'sender', 'token': config.token,
                   'api_keys_hashes': list(self.crawler.api_keys)}
            socket_sender.send(bytes(json.dumps(msg) + '\n', 'utf-8'))

            data = str(socket_sender.recv(1024), 'utf-8')
            first_pos = data.find('\n')
            if first_pos == -1:
                logger.error(
                    "[!] Cannot connect to {}:{} using token {}. Exit: No \\n found".format(reg_ip, reg_port, token))

            msg_json = json.loads(data[:first_pos])

            if 'token' in msg_json and msg_json['token'] == config.token:
                if msg_json['res'] == 'use_api_key':
                    valid_api_key_hash = msg_json['api_key_hash']
                    socket_receiver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    socket_receiver.connect((reg_ip, reg_port))
                    msg = {'action': 'init', 'role': 'receiver', 'token': config.token,
                           'worker_id': msg_json['worker_id']}
                    socket_receiver.send(bytes(json.dumps(msg) + '\n', 'utf-8'))
                    logger.debug("[*] Worker-{} connected to {}".format(msg_json['worker_id'], (reg_ip, reg_port)))
                    return socket_sender, socket_receiver, valid_api_key_hash, msg_json['worker_id']
                else:
                    logger.info("[!] No valid api key. Exit.")
                    exit(0)
            logger.info("[!] Registry didn't respond correctly. Exit. -> {}".format(msg))
            exit(0)
        except json.decoder.JSONDecodeError as e:
            logger.error("[!] Cannot connect to {}:{} using token {}. Exit: {}".format(reg_ip, reg_port, token, e))

    def msg_receiver(self):
        data = ''
        while True:
            data += str(self.socket_recv.recv(1024), 'utf-8')
            while data.find('\n') != -1:
                first_pos = data.find('\n')
                self.msg_received.put(data[:first_pos])
                data = data[first_pos + 1:]
            sleep(0.01)

    def msg_sender(self):
        while True:
            while not self.msg_to_send.empty():
                msg = self.msg_to_send.get()
                try:
                    self.socket_send.send(bytes(msg + '\n', 'utf-8'))
                except BrokenPipeError as e:
                    self.lock_exit.acquire()
                    self.lock_exit.release()
                    logger.warning("[*] Registry-{}:{} down: {}".format(self.reg_ip, self.reg_port, msg))
                    kill(getpid(), SIGUSR1)
                    break
                # logger.debug("[*] Worker-{} sent: {}".format(self.worker_id, msg))
            sleep(0.01)

    def keep_alive(self):
        msg = {'token': config.token, 'action': 'ping', 'worker_id': self.worker_id}
        self.lock_active_time.acquire()
        self.active_time = int(time())
        self.lock_active_time.release()
        while True:
            self.msg_to_send.put(json.dumps(msg))
            sleep(config.heartbeat_time)

            self.lock_active_time.acquire()
            if int(time()) - self.active_time > config.max_heartbeat_lost_time:
                self.lock_exit.acquire()
                self.lock_exit.release()
                logger.info("[!] Lost heartbeat for {} seconds, exit.".format(config.max_heartbeat_lost_time))
                kill(getpid(), SIGUSR1)
            self.lock_active_time.release()

    def msg_received_handler(self):
        while True:
            if not self.msg_received.empty():
                msg = self.msg_received.get()
                try:
                    msg_json = json.loads(msg)
                    if 'token' in msg_json and msg_json['token'] == config.token:
                        # logger.debug("[*] Worker-{} received: {}".format(self.worker_id, msg))
                        task = msg_json['task']
                        if task == 'stream':
                            # continue
                            threading.Thread(target=self.stream,
                                             args=(msg_json['data']['locations'],)).start()
                        elif task == 'task':
                            logger.debug("Worker-{} got task: {}".format(self.worker_id, msg))
                            if len(msg_json['friends_ids']):
                                task = Task('friends', msg_json['friends_ids'])
                                self.task_queue.put(task)
                            if len(msg_json['timeline_ids']):
                                task = Task('timeline', msg_json['timeline_ids'])
                                self.task_queue.put(task)
                        elif task == 'pong':
                            self.lock_active_time.acquire()
                            self.active_time = int(time())
                            self.lock_active_time.release()

                except json.decoder.JSONDecodeError as e:
                    logger.error("Worker-{} received invalid json: {} \n{}".format(self.worker_id, e, msg))
                except KeyError as e:
                    logger.error("Worker-{} received invalid json; KeyError: {}\n{}".format(self.worker_id, e, msg))
            else:
                sleep(0.01)

    def decrease_rate_limit(self, entry):
        self.lock_rate_limit.acquire()
        if entry == 'friends':
            if self.crawler.rate_limits['resources']['friendships']['/friendships/lookup']['remaining'] > 0:
                self.crawler.rate_limits['resources']['friendships']['/friendships/lookup']['remaining'] -= 1
                self.lock_rate_limit.release()
                return True
            else:
                self.lock_rate_limit.release()
                return False

        elif entry == 'followers':
            if self.crawler.rate_limits['resources']['followers']['/followers/ids']['remaining'] > 0:
                self.crawler.rate_limits['resources']['followers']['/followers/ids']['remaining'] -= 1
                self.lock_rate_limit.release()
                return True
            else:
                self.lock_rate_limit.release()
                return False

        elif entry == 'timeline':
            if self.crawler.rate_limits['resources']['statuses']['/statuses/user_timeline']['remaining'] > 0:
                self.crawler.rate_limits['resources']['statuses']['/statuses/user_timeline']['remaining'] -= 1
                self.lock_rate_limit.release()
                return True
            else:
                self.lock_rate_limit.release()
                return False
        else:
            self.lock_rate_limit.release()
            return False

    def get_rate_limit(self):
        rate_limit = defaultdict(int)
        self.lock_rate_limit.acquire()
        now_timestamp = int(time())

        # https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-friends-ids
        # Requests / 15-min window (user auth)	15
        reset = self.crawler.rate_limits['resources']['friendships']['/friendships/lookup']['reset']
        if now_timestamp - reset > 900:
            self.crawler.rate_limits['resources']['friendships']['/friendships/lookup']['reset'] += ceil(
                (now_timestamp - reset) / 900) * 900
            remaining = self.crawler.rate_limits['resources']['friendships']['/friendships/lookup']['limit']
            self.crawler.rate_limits['resources']['friendships']['/friendships/lookup']['remaining'] = remaining
        else:
            remaining = self.crawler.rate_limits['resources']['friendships']['/friendships/lookup']['remaining']
        rate_limit['friends'] = remaining

        # https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-followers-ids
        # Requests / 15-min window (user auth)	15
        reset = self.crawler.rate_limits['resources']['followers']['/followers/ids']['reset']
        if now_timestamp - reset > 900:
            self.crawler.rate_limits['resources']['followers']['/followers/ids']['reset'] += ceil(
                (now_timestamp - reset) / 900) * 900
            remaining = self.crawler.rate_limits['resources']['followers']['/followers/ids']['limit']
            self.crawler.rate_limits['resources']['followers']['/followers/ids']['remaining'] = remaining
        else:
            remaining = self.crawler.rate_limits['resources']['followers']['/followers/ids']['remaining']
        rate_limit['followers'] = remaining

        # https://developer.twitter.com/en/docs/tweets/timelines/api-reference/get-statuses-user_timeline
        # Requests / 15-min window (user auth)	900
        reset = self.crawler.rate_limits['resources']['statuses']['/statuses/user_timeline']['reset']
        if now_timestamp - reset > 900:
            self.crawler.rate_limits['resources']['statuses']['/statuses/user_timeline']['reset'] += ceil(
                (now_timestamp - reset) / 900) * 900
            remaining = self.crawler.rate_limits['resources']['statuses']['/statuses/user_timeline']['limit']
            self.crawler.rate_limits['resources']['statuses']['/statuses/user_timeline']['remaining'] = remaining
        else:
            remaining = self.crawler.rate_limits['resources']['statuses']['/statuses/user_timeline']['remaining']
        self.lock_rate_limit.release()

        rate_limit['timeline'] = remaining
        return rate_limit

    def task_handler(self):
        has_required = False
        last_time_sent = int(time())
        while True:
            if self.task_queue.empty():
                if not has_required:
                    rate_limit = self.get_rate_limit()
                    msg = dict(rate_limit)
                    quota = 0
                    for key, value in msg.items():
                        quota += value
                    if quota > 0:
                        msg['worker_id'] = self.worker_id
                        msg['token'] = config.token
                        msg['action'] = 'ask_for_task'
                    self.msg_to_send.put(json.dumps(msg))
                    last_time_sent = int(time())
                    has_required = True
                else:
                    if int(time()) - last_time_sent > 5:
                        has_required = False
                    sleep(0.01)
            else:

                while not self.task_queue.empty():
                    self.lock_exit.acquire()
                    task = self.task_queue.get()
                    if task.type == 'timeline':
                        for timeline_user_id in task.user_ids:
                            logger.debug("[-] Getting user-{}'s timeline.".format(timeline_user_id))
                            statuses = self.crawler.get_user_timeline(id=timeline_user_id)
                            for status in statuses:
                                self.save_status(status)

                            self.client['all_users'][timeline_user_id]['timeline_updated_at'] = int(time())
                            self.client['all_users'][timeline_user_id].save()
                            sleep(1)

                    if task.type == 'friends':
                        for stream_user_id in task.user_ids:
                            logger.debug("[-] Getting user-{}'s friends.".format(stream_user_id))
                            follower_ids_set = self.crawler.get_followers_ids(id=stream_user_id)
                            friend_ids_set = self.crawler.get_friends_ids(id=stream_user_id)
                            mutual_follow = list(follower_ids_set.intersection(friend_ids_set))
                            users_res = self.crawler.look_up_users(mutual_follow)
                            for user in users_res:
                                self.save_user(user, 'all_users')

                            self.client['stream_users'][stream_user_id]['friends_updated_at'] = int(time())
                            self.client['stream_users'][stream_user_id].save()
                            sleep(1)
                    self.lock_exit.release()

                self.lock_rate_limit.acquire()
                self.crawler.rate_limits = self.crawler.get_rate_limit_status()
                self.lock_rate_limit.release()
                has_required = False

    def status_handler(self):
        while True:
            if not self.stream_res_queue.empty():
                status = self.stream_res_queue.get()
                self.save_status(status, is_stream=True)
            else:
                sleep(0.01)

    def save_user(self, user_, db_name_):
        if user_.id_str not in self.client[db_name_]:
            user_json = user_._json
            user_json['_id'] = user_.id_str
            user_json['timeline_updated_at'] = 0
            self.client[db_name_].create_document(user_json)
            # prevent proxy err (Qifan's proxy against GFW)
            sleep(0.01)
            logger.debug("[*] Worker-{} saved user to {}: {}".format(self.worker_id, db_name_, user_.id_str))
        else:
            logger.debug("[*] Worker-{} ignored user to {}: {}".format(self.worker_id, db_name_, user_.id_str))

    def save_status(self, status_, is_stream=False):
        # use id_str
        # The string representation of the unique identifier for this Tweet.
        # Implementations should use this rather than the large integer in id
        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
        if status_.id_str not in self.client['statues']:
            status_json = status_._json
            status_json['_id'] = status_.id_str
            self.client['statues'].create_document(status_json)
            # prevent proxy err (Qifan's proxy against GFW)
            sleep(0.01)
            logger.debug("[*] Worker-{} saved status: {}".format(self.worker_id, status_.id_str))
        else:
            logger.debug("[*] Worker-{} ignored status: {}".format(self.worker_id, status_.id_str))
        if is_stream:
            if status_.author.id_str not in self.client['stream_users']:
                self.save_user(status_.author, 'stream_users')
            if status_.author.id_str not in self.client['all_users']:
                self.save_user(status_.author, 'all_users')

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
        threading.Thread(target=self.task_handler).start()
        threading.Thread(target=self.keep_alive).start()
