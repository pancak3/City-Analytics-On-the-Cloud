import nltk
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer

try:
    nltk.download('vader_lexicon')
except:
    pass


def clean_tweet(tweet):
    # remove urls and other non english characters
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)", " ", tweet).split())


def generate_sentiment(text):
    tweet_text = clean_tweet(text)
    sia = SentimentIntensityAnalyzer()
    sent_scores = sia.polarity_scores(tweet_text)
    if sent_scores['compound'] > 0:
        sent_scores['sentiment'] = "positive"
    elif sent_scores['compound'] < 0:
        sent_scores['sentiment'] = "negative"
    else:
        sent_scores['sentiment'] = "neutral"
    return sent_scores


if __name__ == '__main__':
    t = {'doc': {
        'full_text': "Mixed Lot of 15 #ultimate Comics Listed facebookmarketplace\n@bulldogcomics62\n\nCheers\n\nCharlie\n\n#facebookmarketplace #ultimatecomics #avengers #batman #Captainamerica #spiderman #sony #fox #disney #netflix #daredevil… https://t.co/Vz0ID9xG5D"}}
    generate_sentiment(t)
