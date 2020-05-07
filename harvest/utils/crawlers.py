"""
@author Qifan github.com/pancak3
@time Created at: 28/4/20 6:51 pm
"""
import tweepy
import logging
import hashlib

from math import ceil
from threading import Lock
from collections import defaultdict
from time import sleep, time
from utils.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Crawler')
logger.setLevel(logging.INFO)


class APIStatus:
    def __init__(self):
        pass


class Crawler:

    def __init__(self):
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
        self.locks = defaultdict(Lock)

    def init(self, hash_):
        (api_key, api_secret_key, access_token, access_token_secret) = self.api_keys[hash_]
        auth = tweepy.OAuthHandler(api_key, api_secret_key)
        auth.set_access_token(access_token, access_token_secret)
        self.api = tweepy.API(auth)
        self.rate_limits = self.get_rate_limit_status()

    def stream_filter(self, process_name, q, **kwargs):
        stream_listener = StreamListener(process_name, q)
        stream_ = tweepy.Stream(auth=self.api.auth, listener=stream_listener)
        stream_.filter(**kwargs)

    def get_rate_limit_status(self):
        sleep(1)
        try:
            return self.api.rate_limit_status()
        except tweepy.RateLimitError:
            if self.rate_limits is None:
                logger.warning("Get rate limit occurs rate limit error, sleep 15 minutes and try again.")
                sleep(15 * 60)
            else:
                reset = self.rate_limits['resources']['application']['/application/rate_limit_status']['reset']
                now_timestamp = int(time())
                to_sleep = ceil((now_timestamp - reset) / 900) * 900 - now_timestamp
                logger.warning(
                    "Get rate limit occurs rate limit error, sleep {} seconds and try again.".format(to_sleep))
                sleep(to_sleep)
            return self.get_rate_limit_status()
        except tweepy.error.TweepError:
            logger.warning("Get rate limit occurs TweepError, sleep 15 minutes and try again.")
            sleep(15 * 60)

    def limit_handled(self, cursor, cursor_type):
        # http://docs.tweepy.org/en/v3.8.0/code_snippet.html?highlight=rate%20limits#handling-the-rate-limit-using-cursors
        while True:
            try:
                yield cursor.next()
            except tweepy.RateLimitError:
                if self.rate_limits is None:
                    logger.warning("Handle cursor occurs rate limit error, sleep 15 minutes and try again.")
                    sleep(15 * 60)
                else:
                    # Need to update reset time frequently, otherwise this part won't work
                    # Worker updates it.
                    if cursor_type == 'friends':
                        to_sleep = int(time()) - \
                                   self.rate_limits['resources']['friendships']['/friendships/lookup']['reset']
                    elif cursor_type == 'followers':
                        to_sleep = int(time()) - \
                                   self.rate_limits['resources']['followers']['/followers/ids']['reset']
                    elif cursor_type == 'timeline':
                        to_sleep = int(time()) - \
                                   self.rate_limits['resources']['statuses']['/statuses/user_timeline']['reset']
                    else:
                        to_sleep = 15 * 60
                    logger.warning(
                        "Handle {} cursor occurs rate limit error, sleep {} seconds and try again.".format(cursor_type,
                                                                                                           to_sleep))
                    sleep(to_sleep)
            except tweepy.error.TweepError as e:
                # http://docs.tweepy.org/en/latest/api.html#tweepy-error-exceptions
                logger.error("[!] Tweep Error in limit handler: {}".format(e))
                break
            except StopIteration:
                # https://stackoverflow.com/questions/51700960
                break

    def get_followers_ids(self, **kwargs):
        follower_ids_set = set()
        for follower_id in self.limit_handled(tweepy.Cursor(self.api.followers_ids, **kwargs).items(), 'followers'):
            follower_ids_set.add(follower_id)
        return follower_ids_set

    def get_friends_ids(self, **kwargs):
        friends_ids_set = set()
        for friend_id in self.limit_handled(tweepy.Cursor(self.api.friends_ids, **kwargs).items(), 'friends'):
            friends_ids_set.add(friend_id)
        return friends_ids_set

    def get_user_timeline(self, **kwargs):
        statuses = []
        for status in self.limit_handled(
                tweepy.Cursor(self.api.user_timeline, **kwargs).items(config.user_timeline_max_statues), 'timeline'):
            statuses.append(status)
        return statuses

    def look_up_users(self, users_ids):
        # Note, this method is not in tweepy official doc but in its source file
        # https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-users-lookup
        # Requests / 15-min window (user auth)	900
        users = []
        for i in range(0, len(users_ids), 100):
            try:
                users_res = self.api.lookup_users(user_ids=users_ids[i:i + 100])
                users += users_res
                sleep(1)  # 900/900
            except tweepy.RateLimitError:
                logger.warning("Lookup users occurs rate limit error, sleep 15 minutes and try again.")
                sleep(900)
                i -= 1
        return users

    @staticmethod
    def hash(api_key_):
        h = hashlib.new(config.hash_algorithm)
        h.update(bytes(api_key_, 'utf-8'))
        return h.hexdigest()


class StreamListener(tweepy.StreamListener):
    def __init__(self, process_name, res_queue, **kw):
        self.process_name = process_name
        self.res_queue = res_queue
        self.err_count = 0
        super(StreamListener, self).__init__(**kw)

    def on_status(self, status):
        self.res_queue.put(status)
        logger.debug("[*]  {}, status: {}".format(self.process_name, status._json))

    def on_error(self, status_code):
        self.err_count += 1
        wait_for = self.err_count ** 5
        if wait_for < 120:
            logger.warning(
                "[*]  Worker-{}, error: {}. Sleep {} seconds".format(self.process_name, status_code, wait_for))
            sleep(wait_for)
            exit(1)
        else:
            logger.warning(
                "[*]  Worker-{}, error: {}. {} errs happened, exit.".format(self.process_name, status_code, wait_for,
                                                                            self.err_count))
            exit(1)

    def on_connect(self):
        logger.debug("[*] Worker-{} stream connected.".format(self.process_name))


if __name__ == '__main__':
    from pprint import pprint

    crawler = Crawler()
    for item in crawler.api_keys.items():
        crawler.init(item[0])
        break
    # crawler.get_followers_ids(user_id=25042316)
    # crawler.get_friends_ids(user_id=25042316)
    # crawler.get_user_timeline(user_id=25042316)
    crawler.look_up_users([25042316])
    # users = crawler.get_followers(screen_name='ronaldolatabo', cursor=-1)
    print()
