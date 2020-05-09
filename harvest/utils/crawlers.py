"""
@author Qifan github.com/pancak3
@time Created at: 28/4/20 6:51 pm
"""
import tweepy
import logging
import hashlib
import traceback

from math import ceil
from os import kill, getpid
from signal import SIGUSR1
from threading import Lock
from collections import defaultdict
from time import sleep, time
from utils.config import config
from utils.logger import get_logger

logger = get_logger('Crawler', logging.DEBUG)


class APIStatus:
    def __init__(self):
        pass


class Crawler:

    def __init__(self):
        self.id = None
        self.api_keys = {}
        for credential in config.twitter:
            api_key = credential.api_key
            api_secret_key = credential.api_secrete_key
            access_token = credential.access_token
            access_token_secret = credential.access_token_secret
            api_key_hash = self.hash(api_key)
            self.api_keys[api_key_hash] = (api_key, api_secret_key, access_token, access_token_secret)

        self.api = None
        self.rate_limits = None
        self.rate_limits_updated_at = 0
        self.lock_rate_limits = Lock()

        self.access_user_timeline = 0
        self.lock_user_timeline = Lock()
        self.access_friends = 0
        self.lock_friends = Lock()
        self.access_followers = 0
        self.lock_followers = Lock()
        self.access_lookup_users = 0
        self.lock_lookup_users = Lock()

    def init(self, hash_, id_):
        self.id = id_
        (api_key, api_secret_key, access_token, access_token_secret) = self.api_keys[hash_]
        auth = tweepy.OAuthHandler(api_key, api_secret_key)
        auth.set_access_token(access_token, access_token_secret)
        self.api = tweepy.API(auth)
        self.update_rate_limit_status()

    def stream_filter(self, id_, q, **kwargs):
        stream_listener = StreamListener(id_, q)
        stream_ = tweepy.Stream(auth=self.api.auth, listener=stream_listener)
        logger.debug("[{}] stream filter locations: {}".format(id_, kwargs.get('locations')))
        # blocking method
        try:
            stream_.filter(**kwargs)
        except Exception:
            raise BaseException

    def update_rate_limit_status(self, err_count=0):
        if err_count > config.max_network_err:
            logger.debug("[*] Err {} times, exit".format(config.max_network_err))
            kill(getpid(), SIGUSR1)

        self.lock_rate_limits.acquire()
        now_time = int(time())
        time_diff = now_time - self.rate_limits_updated_at
        if time_diff <= 2:
            sleep(time_diff)
        self.rate_limits_updated_at = now_time
        try:
            self.rate_limits = self.api.rate_limit_status()
            self.lock_rate_limits.release()
            logger.debug("[{}] Updated rate limit status.".format(self.id))
        except tweepy.TweepError:
            if self.rate_limits is None:
                logger.warning("Get rate limit occurs rate limit error, sleep 15 minutes and try again.")
                sleep(15 * 60)
            else:
                reset = self.rate_limits['resources']['application']['/application/rate_limit_status']['reset']
                now_timestamp = int(time())
                to_sleep = abs(ceil((now_timestamp - reset) / 900) * 900)
                if to_sleep <= 10:
                    to_sleep = 10
                logger.warning("TweepError, sleep {} seconds and try again.".format(to_sleep))
                sleep(to_sleep)
            self.lock_rate_limits.release()
            self.update_rate_limit_status(err_count + 1)

    def limit_handled(self, cursor, cursor_type):
        # http://docs.tweepy.org/en/v3.8.0/code_snippet.html?highlight=rate%20limits#handling-the-rate-limit-using-cursors
        while True:
            try:
                yield cursor.next()
            except tweepy.TweepError:
                # Inherits from TweepError, so except TweepError will catch a RateLimitError too.
                if self.rate_limits is None:
                    logger.warning("Handle cursor occurs rate limit error, sleep 15 minutes and try again.")
                    sleep(15 * 60)
                else:
                    # Need to update reset time frequently, otherwise this part won't work
                    # Worker updates it.
                    self.update_rate_limit_status()

                    if cursor_type == 'friends':
                        remaining = self.rate_limits['resources']['friends']['/friends/ids']['remaining']
                        to_sleep = self.rate_limits['resources']['friends']['/friends/ids']['reset'] - int(time())
                    elif cursor_type == 'followers':
                        remaining = self.rate_limits['resources']['followers']['/followers/ids']['remaining']
                        to_sleep = self.rate_limits['resources']['followers']['/followers/ids']['reset'] - int(time())
                    # elif cursor_type == 'timeline':
                    else:
                        remaining = self.rate_limits['resources']['statuses']['/statuses/user_timeline']['remaining']
                        to_sleep = self.rate_limits['resources']['statuses']['/statuses/user_timeline']['reset'] - int(
                            time())
                    if remaining:
                        break
                    logger.warning(
                        "Handle {} cursor occurs TweepError, sleep {} seconds and try again.".format(cursor_type,
                                                                                                     abs(to_sleep)))
                    sleep(abs(to_sleep))
            except StopIteration:
                # https://stackoverflow.com/questions/51700960
                break

    def get_followers_ids(self, lock, out, **kwargs):
        # lock this func in case of occurring rate limit err
        now_time = int(time())
        self.lock_followers.acquire()
        time_diff = now_time - self.access_followers
        if time_diff <= 5:
            sleep(time_diff)
        self.access_followers = now_time

        # up to a maximum of 5,000 per distinct request
        # https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-followers-ids

        # user wrapper to use variable reference
        # https://stackoverflow.com/questions/986006
        lock.acquire()
        # follower_ids_set = set()
        # for follower_id in self.limit_handled(
        #         tweepy.Cursor(self.api.followers_ids, **kwargs).items(5000), 'followers'):
        #     follower_ids_set.add(follower_id)
        try:
            out[0] = set(self.api.followers_ids(count=5000, **kwargs))
            lock.release()
            self.lock_followers.release()
        except Exception:
            out[0] = set()
            lock.release()
            self.lock_followers.release()

    def get_friends_ids(self, lock, out, **kwargs):
        # lock this func in case of occurring rate limit err
        now_time = int(time())
        self.lock_friends.acquire()
        time_diff = now_time - self.access_friends
        if time_diff <= 5:
            sleep(time_diff)
        self.access_friends = now_time

        # up to a maximum of 5,000 per distinct request
        # https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-friends-ids

        # user wrapper to use variable reference
        # https://stackoverflow.com/questions/986006
        lock.acquire()
        # friends_ids_set = set()
        # for friend_id in self.limit_handled(tweepy.Cursor(self.api.friends_ids, **kwargs).items(5000),
        #                                     'friends'):
        #     friends_ids_set.add(friend_id)
        try:
            out[0] = set(self.api.friends_ids(count=5000, **kwargs))
            lock.release()
            self.lock_friends.release()
        except Exception:
            out[0] = set()
            lock.release()
            self.lock_friends.release()

    def get_user_timeline(self, **kwargs):
        # lock this func in case of occurring rate limit err
        now_time = int(time())
        self.lock_user_timeline.acquire()
        time_diff = now_time - self.access_user_timeline
        if time_diff <= 5:
            sleep(time_diff)
        self.access_user_timeline = now_time

        # up to a maximum of 200 per distinct request
        # https://developer.twitter.com/en/docs/tweets/timelines/api-reference/get-statuses-user_timeline
        # statuses = []
        # for status in self.limit_handled(
        #         tweepy.Cursor(self.api.user_timeline, **kwargs).items(config.user_timeline_max_statues), 'timeline'):
        #     statuses.append(status)
        # kwargs.get('count', config.user_timeline_max_statues)
        try:
            statuses = self.api.user_timeline(count=config.user_timeline_max_statues, **kwargs)
            self.lock_user_timeline.release()
            return statuses
        except Exception:
            return []

    def lookup_users(self, users_ids, err_count=0):
        if err_count > config.max_network_err:
            logger.debug("[*] Err {} times, exit".format(config.max_network_err))
            kill(getpid(), SIGUSR1)
        # lock this func in case of occurring rate limit err
        now_time = int(time())
        self.lock_lookup_users.acquire()
        time_diff = now_time - self.access_lookup_users
        if time_diff <= 1:
            sleep(time_diff)
        self.access_lookup_users = now_time

        # Note, this method is not in tweepy official doc but in its source file
        # https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-users-lookup
        # Requests / 15-min window (user auth)	900
        users = []
        for i in range(0, len(users_ids), 100):
            try:
                users_res = self.api.lookup_users(user_ids=users_ids[i:i + 100])
                users += users_res
            except Exception:
                logger.warning("Occurs exceptions, sleep 10 seconds and try again. \n{}".format(
                    traceback.format_exc()))
                return users + self.lookup_users(users_ids[i:], err_count + 1)
        self.lock_lookup_users.release()
        return users

    @staticmethod
    def hash(api_key_):
        h = hashlib.new(config.hash_algorithm)
        h.update(bytes(api_key_, 'utf-8'))
        return h.hexdigest()


class StreamListener(tweepy.StreamListener):
    def __init__(self, id_, res_queue, **kw):
        self.id = id_
        self.res_queue = res_queue
        self.err_count = 0
        super(StreamListener, self).__init__(**kw)

    def on_status(self, status):
        self.res_queue.put(status)
        logger.debug("[{}] got stream status: {}".format(self.id, status._json['id_str']))

    def on_error(self, status_code):
        self.err_count += 1
        wait_for = self.err_count ** 5
        if wait_for < 120:
            logger.warning(
                "[{}] error: {}. Sleep {} seconds".format(self.id, status_code, wait_for))
            sleep(wait_for)
            raise BaseException
        else:
            logger.warning(
                "[{}] error: {}. {} errs happened, exit.".format(self.id, status_code, self.err_count))
            raise BaseException

    def on_connect(self):
        logger.debug("[{}] is listening stream.".format(self.id))


if __name__ == '__main__':
    from pprint import pprint

    crawler = Crawler()
    for item in crawler.api_keys.items():
        crawler.init(item[0], 0)
        break
    # crawler.get_followers_ids(user_id=25042316)
    # crawler.get_friends_ids(user_id=25042316)
    # crawler.get_user_timeline(user_id=25042316)
    crawler.lookup_users([25042316])
    # users = crawler.get_followers(screen_name='ronaldolatabo', cursor=-1)
    print()
