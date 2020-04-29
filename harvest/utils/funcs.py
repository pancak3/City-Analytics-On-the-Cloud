"""
@author Qifan github.com/pancak3
@time Created at: 28/4/20 6:57 pm
"""
import tweepy
import numpy as np

from utils.boundaries import Boundaries
from utils.crawlers import StreamListener, Crawler


def bds_to_bboxes(bds_):
    bboxes_ = []
    for polygon in bds_.bds:
        polygon = np.array(polygon)
        long_min = min(polygon[:, 0])
        long_max = max(polygon[:, 0])
        lat_min = min(polygon[:, 1])
        lat_max = max(polygon[:, 1])
        bbox = [long_min, lat_min, long_max, lat_max]
        bboxes_.append(bbox)
    return bboxes_


def extract_status(status_):
    extracted_status_ = {"id": status_.id,
                         'in_reply_to_status_id': status_.in_reply_to_status_id,
                         'in_reply_to_user_id': status_.in_reply_to_user_id,
                         'place': status_.place,
                         'retweet_count': status_.retweet_count,
                         'retweeted': status_.retweeted,
                         'source': status_.source,
                         'text': status_.text,
                         'truncated': status_.truncated,
                         'geo': status_.geo,
                         'coordinates': status_.coordinates,
                         'hashtags': status_.entities['hashtags'],
                         'symbols': status_.entities['symbols'],
                         'user_mentions': [user['id'] for user in status_.entities['user_mentions']]}
    return extracted_status_


def extract_timeline(crawler_, user_id):
    statuses_ = crawler_.user_timeline(id=user_id)
    res_ = []

    for status_ in statuses_:
        if status_.lang == 'en':
            res_.append(extract_status(status_))
        else:
            continue
    return res_


if __name__ == '__main__':
    # crawler = Crawler()
    #
    # extract_timeline(crawler, 47856457)
    bds_melbourne = Boundaries("data/MelbourneGeojson.json")
    #
    bboxes = bds_to_bboxes(bds_melbourne)
    crawler = Crawler()

    crawler.stream_filter(1, languages=['en'], locations=bboxes[0], is_async=True)
