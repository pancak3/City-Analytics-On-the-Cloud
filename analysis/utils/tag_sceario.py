import nltk
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


def tag_scenario(twitter_data):
    tweet_words = clean_tweet(twitter_data['doc']['text'])
    exercise_bow = ["exercise","workout","gym","yoga","jogging","aerobics","cardio"]
    climate_bow = ["climate", "globalwarming", "climatechange", "sealevel","greenhouse"]
    check_excercise = any(item in tweet_words for item in excercise_bow)
    twitter_data['exercise'] = check_excercise
    check_climate = any(item in tweet_words for item in climate_bow)
    twitter_data['climate_change'] = check_climate
    return twitter_data
