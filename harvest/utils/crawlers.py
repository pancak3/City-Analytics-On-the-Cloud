"""
@author Qifan github.com/pancak3
@time Created at: 28/4/20 6:51 pm
"""
import tweepy
import logging
from queue import Queue
from utils.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Crawler')
logger.setLevel(logging.INFO)


class Crawler:

    def __init__(self):
        self.apis = Queue()
        for credential in config.twitter:
            api_key = credential.api_key
            api_secret_key = credential.api_secrete_key
            access_token = credential.access_token
            access_token_secret = credential.access_token_secret
            auth = tweepy.OAuthHandler(api_key, api_secret_key)
            auth.set_access_token(access_token, access_token_secret)
            api_ = tweepy.API(auth)
            self.apis.put(api_)

    def reverse_geocode(self, **kwargs):
        api_ = self.apis.get()
        geo_details_ = api_.reverse_geocode(**kwargs)
        self.apis.put(api_)
        return geo_details_

    def search(self, **kwargs):
        api_ = self.apis.get()
        statuses_ = api_.search(**kwargs)
        self.apis.put(api_)
        return statuses_

    def stream_filter(self, process_name, q, **kwargs):
        stream_listener = StreamListener(process_name, q)
        api_ = self.apis.get()
        stream_ = tweepy.Stream(auth=api_.auth, listener=stream_listener)
        self.apis.put(api_)
        stream_.filter(**kwargs)

    def user_timeline(self, **kwargs):
        api_ = self.apis.get()
        statuses_ = api_.user_timeline(**kwargs)
        self.apis.put(api_)
        return statuses_


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
    # geo_details = crawler.reverse_geocode(lat='-37.819',
    #                                       long='144.968')
    # # Melbourne geo_id: 01864a8a64df9dc4
    # pprint(geo_details)

    # statuses = crawler.search(lang='en',
    #                           geocode='-37.819,144.968,30km',
    #                           until='2020-04-28')
    # f = open("res.json", "w+")
    # import json
    #
    # for status in statuses:
    #     f.write(json.dumps(status._json) + '\n')
    # f.close()
    # api = crawler.apis.get()
    # myStreamListener = StreamListener(1)
    # myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    # # myStream.filter(languages=['en'],
    # #                 locations=[144.593741856, -38.433859306, 144.593741856, -37.5112737225, 145.512528832,
    # #                            -37.5112737225, 145.512528832, -38.433859306, 144.593741856, -38.433859306])
    # myStream.filter(languages=['en'],
    #                 locations=[144.0375, -38.6788, 146.1925, -36.9582])
    # crawler.apis.put(api)
    # statuses = crawler.user_timeline(id=47856457)

    crawler.stream_filter(1, languages=['en'],
                          locations=[144.0375, -38.6788, 146.1925, -36.9582])
