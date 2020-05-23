import nltk
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer


# nltk.download('vader_lexicon')


def clean_tweet(tweet):
    # remove urls and other non english characters
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)", " ", tweet).split())


def generate_sentiment(twitter_data):
    tweet_text = clean_tweet(twitter_data['doc']['full_text'])
    sia = SentimentIntensityAnalyzer()
    sent_scores = sia.polarity_scores(tweet_text)
    compound_sent = sent_scores['compound']

    if compound_sent > 0:
        twitter_data['sentiment'] = "positive"

    elif compound_sent < 0:
        twitter_data['sentiment'] = "negative"

    else:
        twitter_data['sentiment'] = "neutral"
    print(twitter_data)


if __name__ == '__main__':
    t = {'doc': {
        'full_text': "Mixed Lot of 15 #ultimate Comics Listed facebookmarketplace\n@bulldogcomics62\n\nCheers\n\nCharlie\n\n#facebookmarketplace #ultimatecomics #avengers #batman #Captainamerica #spiderman #sony #fox #disney #netflix #daredevil… https://t.co/Vz0ID9xG5D"}}
    generate_sentiment(t)
