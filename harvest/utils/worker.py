import logging
import socket
import threading
import json
import queue
import traceback
import os

from os import getpid
from math import ceil
from time import sleep, time
from collections import defaultdict
from utils.config import config
from utils.database import CouchDB
from utils.crawlers import Crawler
from utils.logger import get_logger

logger = get_logger('Worker', logging.DEBUG)


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
        # self.lock_users_recorder = threading.Lock()
        # self.lock_statuses_recorder = threading.Lock()
        self.lock_active_time = threading.Lock()
        self.lock_rate_limit = threading.Lock()

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
        self.crawler.init(valid_api_key_hash, self.worker_id)
        self.task_queue = queue.Queue()
        self.active_time = 0
        self.has_task = False
        self.access_timeline = 0
        self.access_friends = 0

        self.running_timeline = RunningTask()
        self.running_friends = RunningTask()

        self.users_queue = queue.Queue()
        self.statuses_queue = queue.Queue()

    def get_registry(self):
        registry = self.client['control']['registry']
        return registry['ip'], registry['port'], registry['token']

    def connect_reg(self):
        reg_ip, reg_port, token = self.reg_ip, self.reg_port, self.token

        try:
            socket_sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket_sender.connect((reg_ip, reg_port))
            msg = {'action': 'init', 'role': 'sender', 'token': self.token,
                   'api_keys_hashes': list(self.crawler.api_keys)}
            socket_sender.send(bytes(json.dumps(msg) + '\n', 'utf-8'))

            data = socket_sender.recv(1024).decode('utf-8')
            if len(data):
                first_pos = data.find('\n')
                if first_pos == -1:
                    self.exit("[!] Cannot connect to {}:{} using token {}. Exit: No \\n found".format(reg_ip, reg_port,
                                                                                                      token))
                msg_json = json.loads(data[:first_pos])
                if 'token' in msg_json and msg_json['token'] == self.token:
                    del msg_json['token']
                    self.update_active_time()
                    if msg_json['res'] == 'use_api_key':
                        valid_api_key_hash = msg_json['api_key_hash']
                        socket_receiver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        socket_receiver.connect((reg_ip, reg_port))
                        msg = {'action': 'init', 'role': 'receiver', 'token': self.token,
                               'worker_id': msg_json['worker_id']}
                        socket_receiver.send(bytes(json.dumps(msg) + '\n', 'utf-8'))
                        logger.debug("[{}] connected to {}".format(msg_json['worker_id'], (reg_ip, reg_port)))
                        self.update_active_time()
                        return socket_sender, socket_receiver, valid_api_key_hash, msg_json['worker_id']
                    else:
                        self.exit("[!] No valid api key. Exit.")

                self.exit("[!] Registry didn't respond correctly. Exit. -> {}".format(msg))
            else:
                self.exit("[!] Cannot connect to {}:{}: Empty respond".format(reg_ip, reg_port))
        except json.decoder.JSONDecodeError as e:
            self.exit("[!] Cannot connect to {}:{} using token {}. Exit: {}".format(reg_ip, reg_port, token, e))

    def update_active_time(self):
        self.lock_active_time.acquire()
        self.active_time = int(time())
        self.lock_active_time.release()

    def save_pid(self):
        # Record PID for daemon
        self.pid = getpid()
        try:
            f = open('worker-{}.pid'.format(self.worker_id), 'w+')
            f.write(str(self.pid))
            f.close()
            logger.info('[{}] Starting with PID: {}'.format(self.worker_id, self.pid))
        except Exception:
            logger.error('[!] Exit! \n{}'.format(traceback.format_exc()))

    def msg_receiver(self):
        data = ''
        while True:
            data += self.socket_recv.recv(1024).decode('utf-8')
            while data.find('\n') != -1:
                first_pos = data.find('\n')
                self.msg_received.put(data[:first_pos])
                data = data[first_pos + 1:]
            sleep(0.01)

    @staticmethod
    def exit(log):
        # self.lock_users_recorder.acquire()
        # self.lock_statuses_recorder.acquire()
        #
        # self.lock_users_recorder.release()
        # self.lock_statuses_recorder.release()
        # if hasattr(self, 'socket_send') and hasattr(self, 'socket_recv'):
        #     # https://stackoverflow.com/questions/409783
        #     self.socket_send.close()
        #     self.socket_recv.close()
        #     while self.socket_send._io_refs > 0:
        #         sleep(0.01)
        #     while self.socket_recv._io_refs > 0:
        #         sleep(0.01)
        logger.error(log)
        os._exit(1)

    def msg_sender(self):
        while True:
            while not self.msg_to_send.empty():
                msg = self.msg_to_send.get()
                try:
                    self.socket_send.send(bytes(msg + '\n', 'utf-8'))
                except Exception:
                    self.exit("[*] Registry-{}:{} was down.".format(self.reg_ip, self.reg_port))
                # logger.debug("[{}] sent: {}".format(self.worker_id, msg))
            sleep(0.01)

    def keep_alive(self):
        msg_json_str = json.dumps({'token': self.token, 'action': 'ping', 'worker_id': self.worker_id})
        self.update_active_time()
        while True:
            if self.running_friends.get_count() or self.running_timeline.get_count():
                sleep(config.heartbeat_time)
                continue
            else:
                self.msg_to_send.put(msg_json_str)
                sleep(config.heartbeat_time)

                self.lock_active_time.acquire()
                if int(time()) - self.active_time > config.max_heartbeat_lost_time:
                    self.lock_active_time.release()
                    self.exit("[!] Lost heartbeat for {} seconds, exit.".format(config.max_heartbeat_lost_time))
                else:
                    self.lock_active_time.release()

    def msg_received_handler(self):
        while True:
            if not self.msg_received.empty():
                msg = self.msg_received.get()
                try:
                    msg_json = json.loads(msg)
                    if 'token' in msg_json and msg_json['token'] == self.token:
                        self.update_active_time()
                        del msg_json['token']
                        # logger.debug("[{}] received: {}".format(self.worker_id, msg))
                        task = msg_json['task']
                        if task == 'stream':
                            # continue
                            self.stream(msg_json['data']['locations'])
                        elif task == 'task':
                            logger.debug("[{}] got task: {}".format(self.worker_id, msg_json))
                            if 'friends_ids' in msg_json and len(msg_json['friends_ids']):
                                task = Task('friends', msg_json['friends_ids'])
                                self.task_queue.put(task)
                            if 'timeline_ids' in msg_json and len(msg_json['timeline_ids']):
                                task = Task('timeline', msg_json['timeline_ids'])
                                self.task_queue.put(task)
                        elif task == 'pong':
                            pass

                except json.decoder.JSONDecodeError as e:
                    logger.error("[{}] received invalid json: {} \n{}".format(self.worker_id, e, msg))
                except KeyError as e:
                    logger.error("[{}] received invalid json; KeyError: {}\n{}".format(self.worker_id, e, msg))
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

    def stream(self, bbox_, count=0):
        lock = threading.Lock()
        if count > 5:
            self.exit("[{}] stream failed {} times, worker exit.".format(self.worker_id, count))
        else:
            lock.acquire()
            threading.Thread(target=self.crawler.stream_filter,
                             args=(self.worker_id, self.stream_res_queue, lock,),
                             kwargs={'languages': ['en'],
                                     'locations': bbox_}
                             ).start()
            lock.acquire()
            lock.release()
            to_sleep = count ** 2
            logger.warning(
                "[{}] stream err, sleep {} seconds: {}".format(self.worker_id, to_sleep, traceback.format_exc()))
            sleep(to_sleep)
            self.stream(bbox_, count + 1)

    def timeline(self, timeline_task):
        self.running_timeline.inc()
        try:
            (user_id, is_stream_code) = timeline_task
            logger.debug("[{}] is getting user timeline: {}(stream:{}), running timeline task num: {}".format(self.worker_id, user_id, is_stream_code,
                                                                     self.running_timeline.get_count()))
            statuses = self.crawler.get_user_timeline(id=user_id, err_count=0)
            for status in statuses:
                self.statuses_queue.put((status, is_stream_code))
            self.client['users'][user_id]['timeline_updated_at'] = int(time())
            self.client['users'][user_id].save()
            logger.debug("[{}] finished user timeline: {}(stream:{}), running timeline task num: {}".format(self.worker_id, user_id, is_stream_code,
                                                                     self.running_timeline.get_count()))
            self.running_timeline.dec()
            self.crawler.update_rate_limit_status()
        except BaseException:
            self.running_timeline.dec()

    def friends(self, stream_user_id):
        self.running_friends.inc()
        try:
            logger.debug("[{}] getting friends: {}, running friends task num: {}".format(self.worker_id, stream_user_id,
                                                                    self.running_friends.get_count()))

            lock_follower = threading.Lock()
            lock_friend = threading.Lock()

            # user wrapper to use variable reference
            # https://stackoverflow.com/questions/986006
            follower_ids_set = [set(), 0]
            friend_ids_set = [set(), 0]

            threading.Thread(target=self.crawler.get_followers_ids,
                             args=(lock_follower, follower_ids_set, 0,),
                             kwargs={'id': stream_user_id}).start()

            threading.Thread(target=self.crawler.get_friends_ids,
                             args=(lock_friend, friend_ids_set, 0,),
                             kwargs={'id': stream_user_id}).start()

            max_sleep = 5 * 60
            while max_sleep:
                lock_follower.acquire()
                lock_friend.acquire()
                if follower_ids_set[1] == -1 or friend_ids_set[1] == -1:
                    raise BaseException
                elif follower_ids_set[1] and friend_ids_set[1]:
                    break
                else:
                    sleep(0.01)
                    max_sleep -= 0.01
                lock_follower.release()
                lock_friend.release()

            mutual_follow = list(follower_ids_set[0].intersection(friend_ids_set[0]))[:config.friends_max_ids]
            users_res = self.crawler.lookup_users(mutual_follow, err_count=0)
            for user in users_res:
                user = user._json
                user['stream_user'] = False
                self.users_queue.put(user)
            self.client['users'][stream_user_id]['follower_ids'] = list(follower_ids_set)
            self.client['users'][stream_user_id]['friend_ids'] = list(friend_ids_set)
            self.client['users'][stream_user_id]['mutual_follow_ids'] = list(mutual_follow)
            self.client['users'][stream_user_id]['friends_updated_at'] = int(time())
            self.client['users'][stream_user_id].save()
            logger.debug("[{}] finished friends: {}, running friends task num: {}".format(self.worker_id, stream_user_id,
                                                                    self.running_friends.get_count()))

            self.running_friends.dec()
            self.crawler.update_rate_limit_status()
        except BaseException:
            self.running_friends.dec()
        except Exception as e:
            logger.warning(e)
            pass

    def task_requester_and_handler(self):
        last_time_sent = int(time())

        while True:
            if self.task_queue.empty():
                if int(time()) - last_time_sent > 5:
                    # print("running_timeline===> {}".format(self.running_timeline.get_count()))
                    if self.running_timeline.get_count() < config.max_running_timeline:
                        rate_limit = self.refresh_local_rate_limit()
                        timeline_remaining = rate_limit['timeline'] - self.running_timeline.get_count()
                        for i in range(config.max_running_timeline):
                            timeline_remaining -= 1
                            if timeline_remaining < 1:
                                break
                            msg = {'timeline': timeline_remaining,
                                   'worker_id': self.worker_id,
                                   'token': self.token,
                                   'action': 'ask_for_task'}
                            self.msg_to_send.put(json.dumps(msg))
                        last_time_sent = int(time())

                    # print("running_friends===> {}".format(self.running_friends.get_count()))
                    if self.running_friends.get_count() < config.max_running_friends:
                        rate_limit = self.refresh_local_rate_limit()
                        running_num = self.running_friends.get_count()
                        friends_remaining = rate_limit['friends'] - running_num
                        followers_remaining = rate_limit['followers'] - running_num
                        for i in range(config.max_running_friends):
                            followers_remaining -= 1
                            friends_remaining -= 1
                            if friends_remaining < 1 or followers_remaining < 1:
                                break
                            msg = {'friends': friends_remaining,
                                   'followers': followers_remaining,
                                   'worker_id': self.worker_id,
                                   'token': self.token,
                                   'action': 'ask_for_task'}
                            self.msg_to_send.put(json.dumps(msg))
                            last_time_sent = int(time())
                    sleep(5)
                else:
                    sleep(0.01)
            else:

                while not self.task_queue.empty():
                    task = self.task_queue.get()
                    for user_id in task.user_ids:
                        if task.type == 'timeline':
                            threading.Thread(target=self.timeline, args=(user_id,)).start()

                        if task.type == 'friends':
                            threading.Thread(target=self.friends, args=(user_id,)).start()
                            # self.friends(task.user_ids)

    def stream_status_handler(self):
        while True:
            if not self.stream_res_queue.empty():
                status = self.stream_res_queue.get()
                self.statuses_queue.put((status, 1))
            else:
                sleep(0.01)

    def save_user(self, user_json, err_count=0):
        if err_count > config.max_network_err:
            self.exit("[{}] save user err {} times, exit".format(self.worker_id, config.max_network_err))
            return False
        try:
            if user_json['id_str'] not in self.client['users']:
                user_json['_id'] = user_json['id_str']
                user_json['timeline_updated_at'] = 0
                if user_json['stream_user']:
                    user_json['friends_updated_at'] = 0
                self.client['users'].create_document(user_json)
                sleep(0.001)
                # logger.debug("[{}] saved user: {}".format(self.worker_id, user_json['id_str']))
                return True
            else:
                if user_json['stream_user']:
                    if 'friends_updated_at' not in self.client['users'][user_json['id_str']]:
                        self.client['users'][user_json['id_str']]['friends_updated_at'] = 0
                    self.client['users'][user_json['id_str']]['stream_user'] = True
                    self.client['users'][user_json['id_str']].save()
                    # logger.debug("[{}] marked user as stream user: {}(stream:{})".format(self.worker_id,
                    #                                                                      user_json['id_str'],
                    #                                                                      user_json['stream_user']))
                    return True
                else:
                    # logger.debug("[{}] saved user: {}(stream:{})".format(self.worker_id, user_json['id_str'],
                    #                                                      user_json['stream_user']))
                    return False
        except Exception as e:
            # prevent proxy err (mainly for Qifan's proxy against GFW)
            # https://stackoverflow.com/questions/4990718/
            logger.error("[!] Save user err: {}".format(traceback.format_exc()))
            sleep(config.network_err_reconnect_time)
            return self.save_user(user_json=user_json, err_count=err_count + 1)

    def save_status(self, status, is_stream_code, err_count=0):
        if err_count > config.max_network_err:
            self.exit("[{}] save status err {} times, exit: {}({})".format(self.worker_id,
                                                                           status.id_str,
                                                                           is_stream_code,
                                                                           config.max_network_err))
            return False
        # use id_str
        # The string representation of the unique identifier for this Tweet.
        # Implementations should use this rather than the large integer in id
        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
        try:
            if status.id_str not in self.client['statuses']:
                status_json = status._json
                status_json['_id'] = status.id_str
                if is_stream_code == 0:
                    status_json['stream_status'] = False
                    status_json['direct_stream'] = False
                elif is_stream_code == 1:
                    status_json['stream_status'] = True
                    status_json['direct_stream'] = True
                elif is_stream_code == 2:
                    status_json['stream_status'] = True
                    status_json['direct_stream'] = False
                else:
                    return False
                self.client['statuses'].create_document(status_json)
                if is_stream_code == 1:
                    user = status.author._json
                    user['stream_user'] = True
                    self.users_queue.put(user)
                sleep(0.001)
                # logger.debug("[{}] saved status: {}(stream:{})".format(self.worker_id, status.id_str, is_stream_code))
                return True
            else:
                # logger.debug("[{}] ignored status: {}(stream:{})".format(self.worker_id, status.id_str, is_stream_code))
                return False
        except Exception as e:
            # prevent proxy err (mainly for Qifan's proxy against GFW)
            # https://stackoverflow.com/questions/4990718/
            logger.warning("[!] Save status err: {}".format(traceback.format_exc()))
            sleep(config.network_err_reconnect_time)
            return self.save_status(status=status, is_stream_code=is_stream_code, err_count=err_count + 1)

    def check_db(self):
        if 'statuses' not in self.client.all_dbs():
            self.client.create_database('statuses')
            logger.debug("[*] Statuses db is not in database; Created.")

        if 'users' not in self.client.all_dbs():
            self.client.create_database('users')
            logger.debug("[*] Users db is not in database; Created.")

    def users_recorder(self):
        count = 0
        while True:
            while not self.users_queue.empty():
                user_json = self.users_queue.get()
                if self.save_user(user_json=user_json):
                    count += 1
                    if count > config.print_log_when_saved:
                        count = 0
                        logger.info("Saved {} new users".format(config.print_log_when_saved))
            sleep(1)

    def statuses_recorder(self):
        count = 0

        while True:
            # self.lock_statuses_recorder.acquire()
            while not self.statuses_queue.empty():
                (status, is_stream_code) = self.statuses_queue.get()
                if self.save_status(status=status, is_stream_code=is_stream_code):
                    count += 1
                    if count > config.print_log_when_saved:
                        count = 0
                        logger.info("Saved {} new statuses".format(config.print_log_when_saved))
            # self.lock_statuses_recorder.release()
            sleep(1)
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
        threading.Thread(target=self.users_recorder).start()
        threading.Thread(target=self.statuses_recorder).start()
        threading.Thread(target=self.keep_alive).start()
