"""
@author Team 42, Melbourne, Mandeep Singh, 991857
"""
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

def clean_tweet(tweet):
    tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)", " ", tweet).split())
    tweet = tweet.lower()
    tokens = word_tokenize(tweet)
    stop_words = set(stopwords.words('english'))
    filtered_words = [word for word in tokens if word not in stop_words]
    return filtered_words


def tag_exercise(twitter_data):
    tweet_words = clean_tweet(twitter_data['doc']['text'])
    exercise_bow = ["exercise","workout","gym","yoga","jogging","aerobics","cardio"]
    check = any(item in tweet_words for item in excercise_bow)
    twitter_data['exercise'] = check
    return twitter_data
