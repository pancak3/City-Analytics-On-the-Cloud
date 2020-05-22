import nltk
import re
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def clean_tweet(tweet):
    # remove urls and other non english characters
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)", " ", tweet).split()) 

def generate_sentiment(twitter_data):    
    tweet_text = clean_tweet(twitter_data['doc']['text'])
    sia = SentimentIntensityAnalyzer()
    sent_scores = sia.polarity_scores(tweet_text)
    compound_sent = sent_scores['compound']

    if compound_sent > 0:
        twitter_data['sentiment'] = "positive"
    
    elif compound_sent < 0:
        twitter_data['sentiment'] = "negative"

    else:
        twitter_data['sentiment'] = "neutral"
