"""
@author Qifan github.com/pancak3
@time Created at: 28/4/20 6:51 pm
"""
import tweepy
import logging
import hashlib
from threading import Lock
from collections import defaultdict
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
        self.rate_limits = self.api.rate_limit_status()

    def reverse_geocode(self, **kwargs):
        return self.api.reverse_geocode(**kwargs)

    def search(self, **kwargs):
        return self.api.search(**kwargs)

    def stream_filter(self, process_name, q, **kwargs):
        stream_listener = StreamListener(process_name, q)
        stream_ = tweepy.Stream(auth=self.api.auth, listener=stream_listener)
        stream_.filter(**kwargs)

    def user_timeline(self, **kwargs):
        return self.api.user_timeline(**kwargs)

    def get_followers(self, **kwargs):
        # Requests / 15-min window (user auth)	15
        return self.api.followers(**kwargs)

    def rate_limit_status(self):
        # Requests / 15-min window (user auth)	180
        # https://developer.twitter.com/en/docs/developer-utilities/rate-limit-status/api-reference/get-application-rate_limit_status
        return self.api.rate_limit_status()

    @staticmethod
    def hash(api_key_):
        h = hashlib.new(config.hash_algorithm)
        h.update(bytes(api_key_, 'utf-8'))
        return h.hexdigest()


class StreamListener(tweepy.StreamListener):
    def __init__(self, process_name, res_queue, **kw):
        self.process_name = process_name
        self.res_queue = res_queue
        super(StreamListener, self).__init__(**kw)

    def on_status(self, status):
        self.res_queue.put(status)
        logger.debug("[*]  {}, status: {}".format(self.process_name, status._json))

    def on_error(self, status_code):
        logger.warning("[*]  {}, error: {}".format(self.process_name, status_code))

    def on_connect(self):
        logger.debug("[*] Worker-{} stream connected.".format(self.process_name))


if __name__ == '__main__':
    from pprint import pprint

    crawler = Crawler()

    rate_limit = crawler.rate_limit_status()
    users = crawler.get_followers(screen_name='ronaldolatabo', cursor=-1)
    print()
