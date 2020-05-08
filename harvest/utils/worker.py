import logging
import socket
import threading
import json
import queue
import tweepy
import traceback

from os import kill, getpid
from signal import SIGUSR1
from math import ceil
from time import sleep, time, asctime, localtime
from collections import defaultdict
from http.client import RemoteDisconnected
from urllib3.exceptions import MaxRetryError
from requests.exceptions import ProxyError, HTTPError
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


class RunningTask:
    def __init__(self):
        self.count = 0
        self.lock = threading.Lock()

    def get_count(self):
        self.lock.acquire()
        c = self.count
        self.lock.release()
        return c

    def inc(self):
        self.lock.acquire()
        self.count += 1
        self.lock.release()

    def dec(self):
        self.lock.acquire()
        if self.count > 0:
            self.count -= 1
        else:
            self.count = 0
        self.lock.release()


class Worker:
    def __init__(self):
        self.pid = None
        self.couch = CouchDB()
        self.client = self.couch.client
        self.stream_res_queue = queue.Queue()
        self.msg_received = queue.Queue()
        self.msg_to_send = queue.Queue()
        self.crawler = Crawler()
        self.reg_ip, self.reg_port, self.token = self.get_registry()
        self.socket_send, self.socket_recv, valid_api_key_hash, self.worker_id = self.connect_reg()
        self.save_pid()
        self.crawler.init(valid_api_key_hash)
        self.task_queue = queue.Queue()
        self.lock_rate_limit = threading.Lock()
        self.active_time = 0
        self.lock_active_time = threading.Lock()
        self.has_task = False
        self.access_timeline = 0
        self.access_friends = 0
        self.lock_timeline = threading.Lock()
        self.lock_friends = threading.Lock()
        self.running_timeline = RunningTask()
        self.running_friends = RunningTask()

        self.users_queue = queue.Queue()
        self.statuses_queue = queue.Queue()
        self.lock_users_recorder = threading.Lock()
        self.lock_statuses_recorder = threading.Lock()

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
            exit(0)

    def save_pid(self):
        # Record PID for daemon
        self.pid = getpid()
        try:
            f = open('worker-{}.pid'.format(self.worker_id), 'w+')
            f.write(str(self.pid))
            f.close()
            logger.info('[-] Starting Worker-{} PID: {}'.format(self.worker_id, self.pid))
        except Exception:
            logger.error('[!] Exit! \n{}'.format(traceback.format_exc()))

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
                    self.lock_friends.acquire()
                    self.lock_timeline.acquire()
                    self.lock_timeline.release()
                    self.lock_friends.release()
                    logger.warning("[*] Registry-{}:{} was down.".format(self.reg_ip, self.reg_port))
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
                self.lock_active_time.release()
                self.lock_friends.acquire()
                self.lock_timeline.acquire()
                self.lock_users_recorder.acquire()
                self.lock_statuses_recorder.acquire()

                self.lock_users_recorder.release()
                self.lock_statuses_recorder.release()
                self.lock_timeline.release()
                self.lock_friends.release()
                self.lock_active_time.acquire()
                if int(time()) - self.active_time > config.max_heartbeat_lost_time:
                    self.lock_active_time.release()
                    continue
                self.lock_active_time.release()
                # https://www.runoob.com/python/python-date-time.html
                date = asctime(localtime(time()))
                logger.info("[!] {} Lost heartbeat for {} seconds, exit.".format(date, config.max_heartbeat_lost_time))
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
                            if 'friends_ids' in msg_json and len(msg_json['friends_ids']):
                                task = Task('friends', msg_json['friends_ids'])
                                self.task_queue.put(task)
                            if 'timeline_ids' in msg_json and len(msg_json['timeline_ids']):
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
            if self.crawler.rate_limits['resources']['friends']['/friends/ids']['remaining'] > 0:
                self.crawler.rate_limits['resources']['friends']['/friends/ids']['remaining'] -= 1
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

    def refresh_local_rate_limit(self):
        rate_limit = defaultdict(int)
        self.lock_rate_limit.acquire()
        now_timestamp = int(time())

        # https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-friends-ids
        # Requests / 15-min window (user auth)	15
        reset = self.crawler.rate_limits['resources']['friends']['/friends/ids']['reset']
        if now_timestamp - reset > 900:
            self.crawler.rate_limits['resources']['friends']['/friends/ids']['reset'] += ceil(
                (now_timestamp - reset) / 900) * 900
            remaining = self.crawler.rate_limits['resources']['friends']['/friends/ids']['limit']
            self.crawler.rate_limits['resources']['friends']['/friends/ids']['remaining'] = remaining
        else:
            remaining = self.crawler.rate_limits['resources']['friends']['/friends/ids']['remaining']
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

    @staticmethod
    def save_doc(doc, update=None):
        count = 0
        while count < config.max_save_tries:
            try:
                if update is not None:
                    doc[update] = int(time())
                doc.save()
                sleep(0.01)
                break
            except Exception:
                # prevent unexpected err
                count += 1
                sleep(0.01)

    def timeline(self, user_ids):
        self.running_timeline.inc()
        for timeline_user_id in user_ids:
            logger.debug("[-] Worker-{} getting user-{}'s timeline.".format(self.worker_id, timeline_user_id))
            statuses = self.crawler.get_user_timeline(id=timeline_user_id)
            for status in statuses:
                self.statuses_queue.put((status, False, 'Timeline'))

            # Get stream user's timeline first.
            if timeline_user_id in self.client['stream_users']:
                self.save_doc(self.client['stream_users'][timeline_user_id], 'timeline_updated_at')

            if timeline_user_id in self.client['all_users']:
                self.save_doc(self.client['all_users'][timeline_user_id], 'timeline_updated_at')

            logger.debug("[-] Worker-{} finished user-{}'s timeline task.".format(self.worker_id, timeline_user_id))
        self.running_timeline.dec()

        self.lock_rate_limit.acquire()
        self.crawler.update_rate_limit_status()
        self.lock_rate_limit.release()

    def friends(self, user_ids):
        self.running_friends.inc()
        for stream_user_id in user_ids:
            logger.debug("[-] Worker-{} getting user-{}'s friends.".format(self.worker_id, stream_user_id))

            lock_follower = threading.Lock()
            lock_friend = threading.Lock()

            # user wrapper to use variable reference
            # https://stackoverflow.com/questions/986006
            follower_ids_set = [set()]
            friend_ids_set = [set()]
            threading.Thread(target=self.crawler.get_followers_ids,
                             args=(lock_follower, follower_ids_set,),
                             kwargs={'id': stream_user_id}).start()
            threading.Thread(target=self.crawler.get_friends_ids,
                             args=(lock_friend, friend_ids_set,),
                             kwargs={'id': stream_user_id}).start()
            # self.crawler.get_followers_ids(lock_follower, follower_ids_set, id=stream_user_id)
            # self.crawler.get_friends_ids(lock_friend, friend_ids_set, id=stream_user_id)

            # use config.friends_max_ids to limit users growing
            lock_follower.acquire()
            lock_friend.acquire()
            mutual_follow = list(follower_ids_set[0].intersection(friend_ids_set[0]))[:config.friends_max_ids]
            lock_follower.release()
            lock_friend.release()
            users_res = self.crawler.lookup_users(mutual_follow)
            for user in users_res:
                self.users_queue.put((user, 'all_users', 'Friends'))
            self.save_doc(self.client['stream_users'][stream_user_id], 'friends_updated_at')
            self.client['all_users'][stream_user_id]['follower_ids'] = list(follower_ids_set)
            self.client['all_users'][stream_user_id]['friend_ids'] = list(friend_ids_set)
            self.client['all_users'][stream_user_id]['mutual_follow_ids'] = list(mutual_follow)
            self.save_doc(self.client['all_users'][stream_user_id])
            logger.debug("[-] Worker-{} finished user-{}'s friends task.".format(self.worker_id, stream_user_id))
        self.running_friends.dec()

        self.lock_rate_limit.acquire()
        self.crawler.update_rate_limit_status()
        self.lock_rate_limit.release()

    def task_requester_and_handler(self):
        last_time_sent = int(time())

        while True:
            if self.task_queue.empty():
                if int(time()) - last_time_sent > 5:

                    if self.running_timeline.get_count() <= 1:
                        rate_limit = self.refresh_local_rate_limit()
                        timeline_remaining = rate_limit['timeline'] - self.running_timeline.get_count()
                        for i in range(config.max_running_timeline):
                            timeline_remaining -= 1
                            if timeline_remaining < 0:
                                break
                            msg = {'timeline': timeline_remaining,
                                   'worker_id': self.worker_id,
                                   'token': config.token,
                                   'action': 'ask_for_task'}
                            self.msg_to_send.put(json.dumps(msg))

                        last_time_sent = int(time())

                    if self.running_friends.get_count() < 1:
                        rate_limit = self.refresh_local_rate_limit()
                        running_num = self.running_friends.get_count()
                        friends_remaining = rate_limit['friends'] - running_num
                        followers_remaining = rate_limit['followers'] - running_num
                        for i in range(config.max_running_friends):
                            followers_remaining -= 1
                            friends_remaining -= 1
                            if friends_remaining < 0 or followers_remaining < 0:
                                break
                            msg = {'friends': friends_remaining,
                                   'followers': followers_remaining,
                                   'worker_id': self.worker_id,
                                   'token': config.token,
                                   'action': 'ask_for_task'}
                            self.msg_to_send.put(json.dumps(msg))
                            last_time_sent = int(time())
                else:
                    sleep(0.01)
            else:

                while not self.task_queue.empty():
                    task = self.task_queue.get()
                    if task.type == 'timeline':
                        threading.Thread(target=self.timeline, args=(task.user_ids,)).start()
                        # self.timeline(task.user_ids)

                    if task.type == 'friends':
                        threading.Thread(target=self.friends, args=(task.user_ids,)).start()
                        # self.friends(task.user_ids)

    def stream_status_handler(self):
        while True:
            if not self.stream_res_queue.empty():
                status = self.stream_res_queue.get()
                self.statuses_queue.put((status, True, 'Stream'))
            else:
                sleep(0.01)

    def save_user(self, user_, db_name_, err_count=0, caller=None):
        if err_count > config.max_network_err:
            logger.debug("[*] Worker-{} save user err {} times, exit".format(self.worker_id, config.max_network_err))
            kill(getpid(), SIGUSR1)
        try:
            if user_.id_str not in self.client[db_name_]:
                user_json = user_._json
                user_json['_id'] = user_.id_str
                if db_name_ == 'stream_users':
                    user_json['friends_updated_at'] = 0
                user_json['timeline_updated_at'] = 0
                self.client[db_name_].create_document(user_json)
                sleep(0.001)
                # logger.debug(
                #     "[*] Worker-{}-{} saved user to {}: {}".format(self.worker_id, caller, db_name_, user_.id_str))
            else:
                # logger.debug(
                #     "[*] Worker-{}-{} ignored user to {}: {}".format(self.worker_id, caller, db_name_, user_.id_str))
                pass
        except Exception as e:
            # prevent proxy err (mainly for Qifan's proxy against GFW)
            # https://stackoverflow.com/questions/4990718/
            logger.warning("[!] Save user err: {}".format(traceback.format_exc()))
            sleep(config.network_err_reconnect_time)
            self.save_user(user_=user_, db_name_=db_name_, err_count=err_count + 1, caller=caller)

    def save_status(self, status_, is_stream=False, err_count=0, caller=None):
        if err_count > config.max_network_err:
            logger.debug("[*] Worker-{}-{} save status err {} times, exit".format(self.worker_id, caller,
                                                                                  config.max_network_err))
            kill(getpid(), SIGUSR1)
        # use id_str
        # The string representation of the unique identifier for this Tweet.
        # Implementations should use this rather than the large integer in id
        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
        try:
            if status_.id_str not in self.client['statuses']:
                status_json = status_._json
                status_json['_id'] = status_.id_str
                self.client['statuses'].create_document(status_json)
                sleep(0.001)
                # logger.debug("[*] Worker-{}-{} saved status: {}".format(self.worker_id, caller, status_.id_str))
            else:
                # logger.debug("[*] Worker-{}-{} ignored status: {}".format(self.worker_id, caller, status_.id_str))
                pass
            if is_stream:
                self.users_queue.put((status_.author, 'all_users', caller))
                self.users_queue.put((status_.author, 'stream_users', caller))

        except Exception as e:
            # prevent proxy err (mainly for Qifan's proxy against GFW)
            # https://stackoverflow.com/questions/4990718/
            logger.warning("[!] Save status err: {}".format(traceback.format_exc()))
            sleep(config.network_err_reconnect_time)
            self.save_status(status_=status_, is_stream=is_stream, err_count=err_count + 1, caller=caller)

    def check_db(self):
        if 'statuses' not in self.client.all_dbs():
            self.client.create_database('statuses')
            logger.debug("[*] Statuses table not in database; created.")
        if 'stream_users' not in self.client.all_dbs():
            self.client.create_database('stream_users')
            logger.debug("[*] Stream_users table not in database; created.")

        if 'all_users' not in self.client.all_dbs():
            self.client.create_database('all_users')
            logger.debug("[*] All_users table not in database; created.")

    def stream(self, bbox_, count=1):
        if count > 5:
            logger.warning("Worker-{} stream failed {} times, worker exit.".format(self.worker_id, count))
            kill(getpid(), SIGUSR1)
        else:
            try:
                self.crawler.stream_filter(self.worker_id, self.stream_res_queue, languages='en', locations=bbox_)
                # threading.Thread(target=self.crawler.stream_filter,
                #                  args=(self.worker_id, self.stream_res_queue,),
                #                  kwargs={'languages': ['en'],
                #                          'locations': bbox_}
                #                  ).start()
            except Exception:
                logger.warning("Worker-{} stream err: {}".format(self.worker_id, traceback.format_exc()))
                sleep(count ** 2)
                self.stream(bbox_, count + 1)

    def users_recorder(self):
        while True:
            self.lock_users_recorder.acquire()
            while not self.users_queue.empty():
                (user_, db_name_, caller) = self.users_queue.get()
                self.save_user(user_=user_, db_name_=db_name_, caller=caller)
            self.lock_users_recorder.release()
            sleep(0.01)

    def statuses_recorder(self):
        while True:
            self.lock_statuses_recorder.acquire()
            while not self.statuses_queue.empty():
                (status_, is_stream, caller) = self.statuses_queue.get()
                self.save_status(status_=status_, is_stream=is_stream, caller=caller)
            self.lock_statuses_recorder.release()
            sleep(0.01)
        pass

    def run(self):
        self.check_db()
        # start a stream listener, statuses will be put in to a res queue
        threading.Thread(target=self.msg_receiver).start()
        threading.Thread(target=self.msg_received_handler).start()
        threading.Thread(target=self.stream_status_handler).start()
        threading.Thread(target=self.msg_sender).start()
        threading.Thread(target=self.msg_receiver).start()
        threading.Thread(target=self.msg_received_handler).start()
        threading.Thread(target=self.task_requester_and_handler).start()
        threading.Thread(target=self.keep_alive).start()
        threading.Thread(target=self.users_recorder).start()
        threading.Thread(target=self.statuses_recorder).start()
