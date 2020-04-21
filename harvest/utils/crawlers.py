import twitter
from queue import Queue
from config import config


class Crawler:

    def __init__(self):
        self.apis = Queue()
        for credential in config.twitter:
            api_key = credential.api_key
            api_secrete_key = credential.api_secrete_key
            access_token = credential.access_token
            access_token_secret = credential.access_token_secret
            api = twitter.Api(consumer_key=api_key,
                              consumer_secret=api_secrete_key,
                              access_token_key=access_token,
                              access_token_secret=access_token_secret)
            self.apis.put(api)

    def get_user_timeline(self, **kwargs):
        api = self.apis.get()
        statuses = api.GetUserTimeline(**kwargs)
        self.apis.put(api)
        return statuses

    def get_stream_filter(self, **kwargs):
        api = self.apis.get()
        stream = api.GetStreamFilter(**kwargs)

        return api, stream,


if __name__ == '__main__':
    from pprint import pprint

    crawler = Crawler()

    # Timeline example
    tl = crawler.get_user_timeline(screen_name='lalaIalisaa_m')
    pprint(tl)

    # Stream example
    api, s = crawler.get_stream_filter(languages=['en'], locations=['-122.75,36.8', '-121.75,37.8'],
                                       track=['pizza', 'cat'])
    for i in range(3):
        pprint(next(s))
    crawler.apis.put(api)
