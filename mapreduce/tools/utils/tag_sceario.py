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
    cricket_bow = ["cricket","batsman","bowler","t20","odi","mcg","scg","bbl","bigbash"]
    tennis_bow = ["tennis","australianopen","nadal","djokovic","federer","kyrgios","barty","atp","williams"]
    footy_bow = ["afl", "footy","aussierules","collingwoodfc","richmond_fc","sydneyswans"]
    motorsport_bow = ["ferrari","vettel","Lewis","prix","ricciardo","formula1","f1","ausgp","motogp","motorsport"]
    soccer_bow =  ["fifa","aleague","soccer","liverpool","messi","ronaldo","epl","mufc","ffa","melcity","melbournevictory"]
    sports = ["cricket","tennis","footy","motorsport","soccer"]
    for sport in sport:
        bow = sport+"_bow"
        check = any(item in tweet_words for item in bow)
        twitter_data[sport] = check
    return twitter_data
    