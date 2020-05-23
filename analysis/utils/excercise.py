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


def tag_excercise(twitter_data):
    tweet_words = clean_tweet(twitter_data['doc']['text'])
    excercise_bow = ["excercise","workout","gym","yoga","jogging","aerobics","cardio"]
    for word in excercise_bow:
        for token in tweet_words:
            if word == token:
                twitter_data['excercise'] = True
            else:
                twitter_data['excercise'] = False


