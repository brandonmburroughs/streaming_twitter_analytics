#!/usr/bin/env python
"""
Twitter Stream Consumer
Author:  Brandon M. Burroughs
Description:  Creates a consumer to work with the Twitter stream
"""

# Imports
import yaml
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.streaming.kafka import KafkaUtils
from textblob import TextBlob
import json


# Load configs from yaml
config = yaml.load(open('config.yaml'))


# Functions
def get_tweet_text(tweet):
    """
    Try to get the text of a Tweet.  Return an empty string if there is an
    error.

    Parameters
    ----------
    tweet : JSON
        The JSON encoded Tweet from the twitter API

    Returns
    -------
    tweet_text : str
        The text of the tweet or an empty string
    """
    try:
        tweet_text = tweet['text']
    except KeyError:
        tweet_text = ""

    return tweet_text


# Setting up Spark contexts
sc = SparkContext(master="local[4]", appName="Twitter_Streaming_Analytics")
streamContext = StreamingContext(sc, 1)
sqlContext = SQLContext(sc)

# Setting up Kafka stream
zookeeper_location = '%s:%s' % (config['zookeeper.host'], config['zookeeper.port'])
topic = 'twitter_stream'
kafka_stream = KafkaUtils.createStream( streamContext, 
                                        zookeeper_location, 
                                        'raw-event-streaming-consumer', 
                                        {topic: 1})

# Extract JSON from message
tweet = kafka_stream.map(lambda (k,v): json.loads(v)) # Returns value

# Get text from Tweet
tweet_text = tweet.map(get_tweet_text)

# Get sentiment of Tweet text
tweet_text_with_sentiment = tweet_text.map(lambda text: (text, TextBlob(text).sentiment.polarity) )
tweet_sentiment = tweet_text_with_sentiment.map(lambda (k,v): (v, 1))

# Aggregate each Tweet's sentiment
tweet_sentiment_agg = tweet_sentiment.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# Use accumulators to keep a running total of the sum and average sentiment
tweet_sentiment_sum = streamContext.sparkContext.accumulator(0)
tweet_sentiment_count  = streamContext.sparkContext.accumulator(0)

def increment_accumulators(sent_sum, count):
    tweet_sentiment_sum.add(sent_sum)
    tweet_sentiment_count.add(count)
    return None

tweet_sentiment_agg.foreachRDD(lambda rdd: rdd.foreach(lambda (sent_sum, count): increment_accumulators(sent_sum, count) ) )
print(tweet_sentiment_sum.value, tweet_sentiment_count.value)#, float(tweet_sentiment_sum.value) / tweet_sentiment_count.value
tweet_sentiment_sum.value

#tweet_sentiment_mean = tweet_sentiment_agg.map(lambda (sent_sum, count): (float(sent_sum) / count, count))

# Print results
#tweet_sentiment_mean.pprint()

# Start Spark stream
streamContext.start()
streamContext.awaitTermination()