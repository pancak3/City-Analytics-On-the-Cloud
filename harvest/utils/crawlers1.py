"""
@author Qifan github.com/pancak3
@time Created at: 28/4/20 6:51 pm
"""
import tweepy
from queue import Queue
from config import config


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


if __name__ == '__main__':
    from pprint import pprint

    crawler = Crawler()
    geo_details = crawler.reverse_geocode(lat='-37.819',
                                          long='144.968')
    # Melbourne geo_id: 01864a8a64df9dc4
    pprint(geo_details)
