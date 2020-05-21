import logging
import socket
import threading
import json
import queue
import traceback
import os

from math import ceil
from time import sleep, time
from collections import defaultdict
from utils.config import Config
from utils.database import CouchDB
from utils.crawlers import Crawler
from utils.logger import get_logger

logger = get_logger('Worker', logging.DEBUG)


class Task:
    def __init__(self, _type, _ids):
        self.type = _type
        self.user_ids = _ids


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


class Status:
    def __init__(self):
        self.active = False
        self.lock = threading.Lock()

    def set(self, is_active):
        self.lock.acquire()
        self.active = is_active
        self.lock.release()

    def is_active(self):
        self.lock.acquire()
        status = self.active
        self.lock.release()
        return status


class Worker:
    def __init__(self):
        self.active = Status()
        self.config = Config()
        # self.lock_users_recorder = threading.Lock()
        # self.lock_statuses_recorder = threading.Lock()
        self.lock_rate_limit = threading.Lock()

        self.pid = None
        self.couch = CouchDB()
        self.client = self.couch.client
        self.areas_collection = self.preprocess_areas()

        self.stream_res_queue = queue.Queue(maxsize=self.config.max_queue_size)
        self.msg_received = queue.Queue(maxsize=self.config.max_queue_size)
        self.msg_to_send = queue.Queue(maxsize=self.config.max_queue_size)
        self.crawler = Crawler()
        self.reg_ip, self.reg_port, self.token = self.get_registry()
        self.socket_send, self.socket_recv, valid_api_key_hash, self.worker_id = self.connect_reg()
        self.crawler.init(valid_api_key_hash, self.worker_id)

        self.task_queue = queue.Queue(maxsize=self.config.max_queue_size)
        self.has_task = False
        self.access_timeline = 0
        self.access_friends = 0

        self.running_timeline = RunningTask()
        self.running_friends = RunningTask()

        self.users_queue = queue.Queue(maxsize=self.config.max_queue_size)
        self.statuses_queue = queue.Queue(maxsize=self.config.max_queue_size)

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
            buffer = bytes(json.dumps(msg) + '\n', 'utf-8')
            socket_sender.send(buffer)
            data = socket_sender.recv(1024).decode('utf-8')
            if len(data):
                first_pos = data.find('\n')
                while first_pos == -1:
                    data += socket_sender.recv(1024).decode('utf-8')
                    first_pos = data.find('\n')
                msg_json = json.loads(data[:first_pos])
                if 'token' in msg_json and msg_json['token'] == self.token:
                    del msg_json['token']
                    self.active.set(True)
                    if msg_json['res'] == 'use_api_key':
                        valid_api_key_hash = msg_json['api_key_hash']
                        socket_receiver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        socket_receiver.connect((reg_ip, reg_port))
                        msg = {'action': 'init', 'role': 'receiver', 'token': self.token,
                               'worker_id': msg_json['worker_id']}
                        socket_receiver.send(bytes(json.dumps(msg) + '\n', 'utf-8'))
                        logger.debug(
                            "[{}] Connected to Registry -> {}".format(msg_json['worker_id'], (reg_ip, reg_port)))
                        return socket_sender, socket_receiver, valid_api_key_hash, msg_json['worker_id']
                    else:
                        self.exit("[!] No valid api key. Exit.")

                self.exit("[!] Registry didn't respond correctly. Exit. -> {}".format(msg))
            else:
                self.exit("[!] Cannot connect to {}:{}: Empty respond".format(reg_ip, reg_port))
        except Exception as e:
            self.exit("[!] Cannot connect to {}:{} using token {}. Exit: {}".format(reg_ip, reg_port, token, e))

    def msg_receiver(self):
        data = ''
        while True:
            data += self.socket_recv.recv(1024).decode('utf-8')
            while data.find('\n') != -1:
                first_pos = data.find('\n')
                self.msg_received.put(data[:first_pos])
                logger.debug("Put into received msg: {}".format(data[:first_pos]))
                data = data[first_pos + 1:]
            if len(data) > 10240:
                data = ''

    @staticmethod
    def exit(log):
        logger.error(log)
        os._exit(1)

    def msg_sender(self):
        while True:
            msg = self.msg_to_send.get()
            try:
                self.socket_send.send(bytes(msg + '\n', 'utf-8'))
            except Exception:
                self.exit("[*] Registry-{}:{} disconnected.".format(self.reg_ip, self.reg_port))
            logger.debug("[{}] sent: {}".format(self.worker_id, msg))

            del msg

    def keep_alive(self):
        msg_json_str = json.dumps({'token': self.token, 'action': 'ping', 'worker_id': self.worker_id})
        self.active.set(True)
        to_sleep = self.config.max_heartbeat_lost_time
        while True:

            self.msg_to_send.put(msg_json_str)
            sleep(5)
            # clear local cache
            self.client.clear()
            to_sleep -= 5
            if to_sleep < 0:
                to_sleep = self.config.max_heartbeat_lost_time
                if not self.active.is_active():
                    self.exit("[!] No running task and Lost heartbeat for {} seconds, exit.".format(
                        self.config.max_heartbeat_lost_time))
                self.active.set(False)

    def msg_received_handler(self):
        while True:
            msg = self.msg_received.get()
            logger.debug("[{}] received: {}".format(self.worker_id, msg))

            try:
                msg_json = json.loads(msg)
                if 'token' in msg_json and msg_json['token'] == self.token:
                    self.active.set(True)
                    # del msg_json['token']
                    task = msg_json['task']
                    if task == 'stream':
                        # continue
                        self.stream(msg_json['data']['locations'])
                    elif task == 'task':
                        self.handle_tasks(msg_json)
                    elif task == 'pong':
                        pass

            except json.decoder.JSONDecodeError as e:
                logger.error("[{}] received invalid json: {} \n{}".format(self.worker_id, e, msg))
            except KeyError as e:
                logger.error("[{}] received invalid json; KeyError: {}\n{}".format(self.worker_id, e, msg))
            except Exception as e:
                logger.warning(e)
            del msg

    def handle_tasks(self, msg_json):
        logger.debug("[{}] got task: {}".format(self.worker_id, msg_json))
        if 'friends_ids' in msg_json and len(msg_json['friends_ids']):
            task = Task('friends', msg_json['friends_ids'])
            self.task_queue.put(task)
        if 'timeline_ids' in msg_json and len(msg_json['timeline_ids']):
            task = Task('timeline', msg_json['timeline_ids'])
            self.task_queue.put(task)

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
        if count > 5:
            self.exit("[{}] stream failed {} times, worker exit.".format(self.worker_id, count))
        else:
            threading.Thread(target=self.crawler.stream_filter,
                             args=(self.worker_id, self.stream_res_queue,),
                             kwargs={'languages': ['en'],
                                     'locations': bbox_}
                             ).start()
            # to_sleep = count ** 2
            # logger.warning(
            #     "[{}] stream err, sleep {} seconds:{}".format(self.worker_id, to_sleep, traceback.format_exc()))
            # sleep(to_sleep)
            # self.stream(bbox_, count + 1)
            logger.info("Started stream task")

    def timeline(self, timeline_task):
        self.running_timeline.inc()
        (user_id, is_stream_user) = timeline_task
        try:
            logger.debug("[{}] is getting user timeline:{}(stream:{}), "
                         "running timeline task num: {}".format(self.worker_id,
                                                                user_id,
                                                                is_stream_user,
                                                                self.running_timeline.get_count()))

            real_user_id = user_id[user_id.find(':') + 1:]
            statuses = self.crawler.get_user_timeline(user_id=real_user_id, err_count=0)
            for status in statuses:
                self.statuses_queue.put((status, 2))
            doc = self.client['users'][user_id]

            doc.update_field(action=doc.field_set, field='timeline_authorized', value=True)
            doc.update_field(action=doc.field_set, field='timeline_updated_at', value=int(time()))
            logger.debug("[{}] finished user timeline:{}(stream:{}), "
                         "running timeline task num: {}".format(self.worker_id,
                                                                user_id,
                                                                is_stream_user,
                                                                self.running_timeline.get_count()))
            self.active.set(True)
            self.running_timeline.dec()
            self.crawler.update_rate_limit_status()
        except Exception as e:
            self.active.set(True)
            self.running_timeline.dec()
            doc = self.client['users'][user_id]
            doc.update_field(action=doc.field_set, field='timeline_authorized', value=False)
            doc.update_field(action=doc.field_set, field='timeline_updated_at', value=int(time()))
            logger.warning(e)
            logger.debug("[{}] Exciption! user timeline:{}(stream:{}), "
                         "current task : {}".format(self.worker_id, user_id, is_stream_user,
                                                    self.running_timeline.get_count()))

    def friends(self, stream_user_id):
        self.running_friends.inc()
        try:
            logger.debug("[{}] getting friends: {}, current task:{}".format(self.worker_id,
                                                                            stream_user_id,
                                                                            self.running_friends.get_count()))

            lock_follower = threading.Lock()
            lock_friend = threading.Lock()

            # user wrapper to use variable reference
            # https://stackoverflow.com/questions/986006
            follower_ids_set = [set(), 0]
            friend_ids_set = [set(), 0]

            real_user_id = stream_user_id[stream_user_id.find(':') + 1:]
            threading.Thread(target=self.crawler.get_followers_ids,
                             args=(lock_follower, follower_ids_set,),
                             kwargs={'user_id': real_user_id}).start()

            threading.Thread(target=self.crawler.get_friends_ids,
                             args=(lock_friend, friend_ids_set,),
                             kwargs={'user_id': real_user_id}).start()

            max_sleep = 5 * 60
            while max_sleep:
                lock_follower.acquire()
                lock_friend.acquire()
                if follower_ids_set[1] and friend_ids_set[1]:
                    break
                else:
                    sleep(1)
                    max_sleep -= 1
                lock_follower.release()
                lock_friend.release()

            mutual_follow = list(follower_ids_set[0].intersection(friend_ids_set[0]))[:self.config.friends_max_ids]
            users_res = self.crawler.lookup_users(mutual_follow)
            for user in users_res:
                user_json = user._json
                user_json['stream_user'] = False
                self.users_queue.put(user_json)
            doc = self.client['users'][stream_user_id]
            doc.update_field(action=doc.field_set, field='follower_ids', value=list(follower_ids_set[0]))
            doc.update_field(action=doc.field_set, field='friend_ids', value=list(friend_ids_set[0]))
            doc.update_field(action=doc.field_set, field='mutual_follow_ids', value=list(mutual_follow))
            doc.update_field(action=doc.field_set, field='friends_updated_at', value=int(time()))
            doc.update_field(action=doc.field_set, field='friends_authorized', value=True)
            logger.debug(
                "[{}] finished friends: {}, current task:{}".format(self.worker_id, stream_user_id,
                                                                    self.running_friends.get_count()))
            self.active.set(True)
            self.running_friends.dec()
            self.crawler.update_rate_limit_status()

            del follower_ids_set
            del friend_ids_set

        except Exception as e:
            self.active.set(True)
            self.running_friends.dec()
            doc = self.client['users'][stream_user_id]
            doc.update_field(action=doc.field_set, field='friends_updated_at', value=int(time()))
            doc.update_field(action=doc.field_set, field='friends_authorized', value=False)
            logger.warning(e)
            logger.debug(
                "[{}] Exception! running friends: {}, current task:{}".format(self.worker_id, stream_user_id,
                                                                              self.running_friends.get_count()))

    def task_requester(self):
        timeline_last_time_sent = int(time())
        friends_last_time_sent = int(time())
        while True:
            if int(time()) - timeline_last_time_sent > 10 \
                    and self.running_timeline.get_count() < self.config.max_running_timeline:
                rate_limit = self.refresh_local_rate_limit()
                timeline_remaining = rate_limit['timeline'] - self.running_timeline.get_count()
                # for i in range(self.config.max_running_timeline):
                for i in range(1, 10 if 10 < timeline_remaining else timeline_remaining):
                    msg = {'timeline': timeline_remaining,
                           'worker_id': self.worker_id,
                           'token': self.token,
                           'action': 'ask_for_task'}
                    self.msg_to_send.put(json.dumps(msg))
                    timeline_remaining -= 1
                    del msg

                timeline_last_time_sent = int(time())

                del rate_limit
                del timeline_remaining

            if int(time()) - friends_last_time_sent > 5 \
                    and self.running_friends.get_count() < self.config.max_running_friends:
                rate_limit = self.refresh_local_rate_limit()
                running_num = self.running_friends.get_count()
                friends_remaining = rate_limit['friends'] - running_num
                followers_remaining = rate_limit['followers'] - running_num
                for i in range(self.config.max_running_friends):
                    friends_remaining -= 1
                    if friends_remaining < 1 or followers_remaining < 1:
                        break
                    msg = {'friends': friends_remaining,
                           'followers': followers_remaining,
                           'worker_id': self.worker_id,
                           'token': self.token,
                           'action': 'ask_for_task'}
                    self.msg_to_send.put(json.dumps(msg))
                    friends_last_time_sent = int(time())
                    del msg
                del rate_limit
                del running_num
                del friends_remaining
                del followers_remaining

            sleep(5)

    def task_handler(self):
        while True:
            task = self.task_queue.get()
            for user_id in task.user_ids:
                if task.type == 'timeline':
                    threading.Thread(target=self.timeline, args=(user_id,)).start()
                elif task.type == 'friends':
                    threading.Thread(target=self.friends, args=(user_id,)).start()
            del task

    def stream_status_handler(self):
        while True:
            status = self.stream_res_queue.get()
            self.statuses_queue.put((status, 1))
            del status

    def save_user(self, user_json, err_count=0):
        if err_count > self.config.max_network_err:
            self.exit("[{}] save user err {} times, exit".format(self.worker_id, self.config.max_network_err))
            return False
        try:
            # https://developer.twitter.com/en/docs/basics/twitter-ids
            if 'id' in user_json:
                del user_json['id']
            # generate partitioned id
            if user_json['stream_user']:
                user_json['friends_updated_at'] = 0
                user_json['_id'] = "{}:{}".format('stream', user_json['id_str'])
            else:
                user_json['_id'] = "{}:{}".format('not_stream', user_json['id_str'])

            if user_json['_id'] not in self.client['users']:
                # if user dose not exist in db
                user_json['timeline_updated_at'] = 0
                user_json['inserted_time'] = int(time())
                self.client['users'].create_document(user_json)
                sleep(0.001)
                # logger.debug("[{}] saved user: {}".format(self.worker_id, user_json['id_str']))
                return True
            else:
                # if user exists in db
                if user_json['stream_user']:
                    # and is stream user, check existed doc
                    doc = self.client['users'][user_json['_id']]
                    if not doc['stream_user']:
                        # if exist doc is not stream user
                        doc.update_field(
                            action=doc.field_set,
                            field='friends_updated_at',
                            value=0
                        )
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
            sleep(self.config.network_err_reconnect_time)
            return self.save_user(user_json=user_json, err_count=err_count + 1)

    @staticmethod
    def read_areas(path):
        areas_collection = {}
        filenames = os.listdir(path)
        for filename in filenames:
            abs_path = os.path.join(path, filename)
            if os.path.isfile(abs_path):
                with open(abs_path) as f:
                    areas_f = json.loads(f.read())
                    areas = areas_f['features']
                    f.close()
                    state_name = filename[:filename.find('.')]
                    areas_collection[state_name] = areas
        return areas_collection

    @staticmethod
    def calc_bbox_of_polygon(polygon):
        import numpy as np
        polygon_arr = np.asarray(polygon)

        min_x = np.min(polygon_arr[:, 0], axis=0)
        min_y = np.min(polygon_arr[:, 1], axis=0)
        max_x = np.max(polygon_arr[:, 0], axis=0)
        max_y = np.max(polygon_arr[:, 1], axis=0)
        return [min_x, min_y, max_x, max_y]

    @staticmethod
    def calc_bbox_of_state(areas):
        import numpy as np
        bboxes = []
        for area in areas:
            for bbox in area['bboxes']:
                bboxes.append(bbox)
        bboxes = np.asarray(bboxes)

        min_x = np.min(bboxes[:, 0], axis=0)
        min_y = np.min(bboxes[:, 1], axis=0)
        max_x = np.max(bboxes[:, 2], axis=0)
        max_y = np.max(bboxes[:, 3], axis=0)

        return [min_x, min_y, max_x, max_y]

    def preprocess_areas(self):
        new_sa2_2016 = self.read_areas(self.config.australia_lga2016_path)

        states = [{"hit": 0, "state_name": None, "bbox": [], "areas": []} for _ in range(len(new_sa2_2016))]
        # calc bbox for each area and store it in its state
        count = 0
        for key, areas in new_sa2_2016.items():
            states[count]["state_name"] = key
            area_id = 0
            for area in areas:
                if 'geometry' in area:
                    states[count]["areas"].append({'hit': 0,
                                                   "feature_code": area['properties']["feature_code"],
                                                   'feature_name': area['properties']['feature_name'],
                                                   'coordinates': area['geometry']['coordinates'],
                                                   'bboxes': []})
                    for i, polygons in enumerate(area['geometry']['coordinates']):
                        for j, polygon in enumerate(polygons):
                            states[count]["areas"][area_id]["bboxes"].append(
                                self.calc_bbox_of_polygon(polygon))
                    area_id += 1
                # use the left value to record its hit times
            count += 1

        for key, v in enumerate(states):
            states[key]["bbox"] = self.calc_bbox_of_state(v['areas'])

        return states

    def rank_areas(self, state_idx, area_idx):
        # manipulate the rank of areas to allow iterator find the location faster
        self.areas_collection[state_idx]['areas'][area_idx]['hit'] += 1
        source_idx = area_idx
        while area_idx and \
                self.areas_collection[state_idx]['areas'][area_idx - 1]['hit'] < \
                self.areas_collection[state_idx]['areas'][source_idx]['hit']:
            area_idx -= 1
        # switch
        if source_idx != area_idx:
            tmp = self.areas_collection[state_idx]['areas'][area_idx]
            self.areas_collection[state_idx]['areas'][area_idx] = self.areas_collection[state_idx]['areas'][source_idx]
            self.areas_collection[state_idx]['areas'][source_idx] = tmp

        self.areas_collection[state_idx]['hit'] += 1
        source_idx = state_idx
        while state_idx and \
                self.areas_collection[state_idx - 1]['hit'] < \
                self.areas_collection[source_idx]['hit']:
            state_idx -= 1
        # switch
        if source_idx != state_idx:
            tmp = self.areas_collection[state_idx]
            self.areas_collection[state_idx] = self.areas_collection[source_idx]
            self.areas_collection[source_idx] = tmp

    def retrieve_statuses_areas(self, status):
        # https://stackoverflow.com/questions/217578
        doc = status._json
        del status
        doc['_id'] = "australia:" + doc["id_str"]
        doc['sa2_2016_lv12_code'] = 'australia'
        doc['sa2_2016_lv12_name'] = 'In Australia BBox But No Specific Location'
        if doc['coordinates'] is not None and doc['coordinates']['type'] == 'Point':
            doc['_id'] = "out_of_australia:" + doc["id_str"]
            doc['sa2_2016_lv12_code'] = 'out_of_australia'
            doc['sa2_2016_lv12_name'] = 'Out of Australia'
            point_x, point_y = doc['coordinates']['coordinates']

            for i in range(len(self.areas_collection)):
                # state
                if point_x < self.areas_collection[i]['bbox'][0] \
                        or point_y < self.areas_collection[i]['bbox'][1] \
                        or point_x > self.areas_collection[i]['bbox'][2] \
                        or point_y > self.areas_collection[i]['bbox'][3]:
                    continue
                for j in range(len(self.areas_collection[i]['areas'])):
                    # areas
                    is_inside = False
                    for k in range(len(self.areas_collection[i]['areas'][j]["coordinates"])):
                        # polygons
                        for m in range(len(self.areas_collection[i]['areas'][j]["coordinates"][k])):
                            # coordinates of one polygon
                            min_x, min_y, max_x, max_y = self.areas_collection[i]['areas'][j]["bboxes"][k]
                            if point_x < min_x or point_y < min_y or point_x > max_x or point_y > max_y:
                                continue
                            l = - 1
                            n = 0
                            length = len(self.areas_collection[i]['areas'][j]["coordinates"][k][m])
                            while n < length:
                                if (self.areas_collection[i]['areas'][j]["coordinates"][k][m][n][1] > point_y) \
                                        != (self.areas_collection[i]['areas'][j]["coordinates"][k][m][l][1] > point_y) \
                                        and point_x < \
                                        (self.areas_collection[i]['areas'][j]["coordinates"][k][m][l][0] -
                                         self.areas_collection[i]['areas'][j]["coordinates"][k][m][n][0]) * \
                                        (point_y - self.areas_collection[i]['areas'][j]["coordinates"][k][m][n][1]) / \
                                        (self.areas_collection[i]['areas'][j]["coordinates"][k][m][l][1] -
                                         self.areas_collection[i]['areas'][j]["coordinates"][k][m][n][1]) \
                                        + self.areas_collection[i]['areas'][j]["coordinates"][k][m][n][0]:
                                    is_inside = not is_inside
                                n += 1
                                l += 1
                    if is_inside:
                        doc['sa2_2016_lv12_code'] = self.areas_collection[i]['areas'][j]['feature_code']
                        doc['sa2_2016_lv12_name'] = self.areas_collection[i]['areas'][j]['feature_name']
                        doc['sa2_2016_lv12_state'] = self.areas_collection[i]['state_name']
                        doc['_id'] = doc['sa2_2016_lv12_code'] + doc['_id'][doc['_id'].find(':'):]
                        self.rank_areas(i, j)
                        return doc

        return doc

    def save_status(self, status, is_stream_code, err_count=0):
        if err_count > self.config.max_network_err:
            self.exit("[{}] save status err {} times, exit: {}({})".format(self.worker_id,
                                                                           status.id,
                                                                           is_stream_code,
                                                                           self.config.max_network_err))
            return False
        # use id_str
        # The string representation of the unique identifier for this Tweet.
        # Implementations should use this rather than the large integer in id
        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
        try:
            status_json = self.retrieve_statuses_areas(status)
            if self.config.ignore_statuses_out_of_australia:
                if status_json['sa2_2016_lv12_code'] in {'australia', 'out_of_australia'} and is_stream_code != 1:
                    return False
            if status_json['_id'] not in self.client['statuses']:
                if is_stream_code == 0:
                    status_json['stream_status'] = False
                elif is_stream_code == 1:
                    status_json['stream_status'] = True
                    status_json['direct_stream'] = True
                elif is_stream_code == 2:
                    status_json['stream_status'] = True
                    status_json['direct_stream'] = False
                else:
                    return False
                status_json['inserted_time'] = int(time())
                if self.config.ignore_statuses_out_of_australia:
                    if status_json['sa2_2016_lv12_code'] not in {'0', '1'}:
                        self.client['statuses'].create_document(status_json)
                else:
                    self.client['statuses'].create_document(status_json)
                if is_stream_code == 1:
                    user_json = status.author._json
                    user_json['stream_user'] = True
                    self.users_queue.put(user_json)
                sleep(0.001)
                # logger.debug("[{}] saved status: {}(stream:{})".format(self.worker_id, status.id, is_stream_code))
                return True
            else:
                # logger.debug("[{}] ignored status: {}(stream:{})".format(self.worker_id, status.id, is_stream_code))
                return False
        except Exception as e:
            # prevent proxy err (mainly for Qifan's proxy against GFW)
            # https://stackoverflow.com/questions/4990718/
            logger.warning("[!] Save status err: {}".format(traceback.format_exc()))
            sleep(self.config.network_err_reconnect_time)
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
            user_json = self.users_queue.get()
            if self.save_user(user_json=user_json):
                count += 1
                if count % self.config.print_log_when_saved == 0:
                    logger.info("Saved {} new users in total".format(count))
            del user_json

    def statuses_recorder(self):
        count = 0
        while True:
            (status, is_stream_code) = self.statuses_queue.get()
            if self.save_status(status=status, is_stream_code=is_stream_code):
                count += 1
                if count % self.config.print_log_when_saved == 0:
                    logger.info("Saved {} new statuses in total".format(count))
            del status
            del is_stream_code

    def run(self):
        self.check_db()
        # start a stream listener, statuses will be put in to a res queue
        threading.Thread(target=self.msg_receiver).start()
        threading.Thread(target=self.msg_received_handler).start()
        threading.Thread(target=self.stream_status_handler).start()
        threading.Thread(target=self.msg_sender).start()
        threading.Thread(target=self.msg_receiver).start()
        threading.Thread(target=self.task_requester).start()
        threading.Thread(target=self.task_handler).start()
        threading.Thread(target=self.users_recorder).start()
        threading.Thread(target=self.statuses_recorder).start()
        threading.Thread(target=self.keep_alive).start()
