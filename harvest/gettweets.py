from TweetStore import TweetStore
from TwitterAPI.TwitterAPI import TwitterAPI

#your keys

API_KEY = XXX
API_SECRET = XXX
ACCESS_TOKEN = XXX
ACCESS_TOKEN_SECRET = XXX

storage = TweetStore('test_db')
api = TwitterAPI(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

for item in api.request('statuses/filter', {'track', 'pizza'}):
    if 'text' in item:
        print('%s -- %s\n' % (item['user']['screen_name'], item['text']))
        storage.save_tweet(item)
    elif 'message' in item:
        print('ERROR %s: %s\n' % (item['code'], item['message']))
