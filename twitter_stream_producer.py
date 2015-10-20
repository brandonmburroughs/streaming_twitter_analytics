#!/usr/bin/env python
"""
Twitter Stream Producer
Author:  Brandon M. Burroughs
Description:  Creates a twitter stream around topics and pushes those to Kafka
"""


# Imports
import yaml
from kafka import KafkaClient, SimpleProducer
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


# Load configs from yaml
config = yaml.load(open('config.yaml'))


class StdOutListener(StreamListener):
    """ 
    A listener handles tweets are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.

    Parameters
    ----------
    None

    Attributes
    ----------
    None

    Notes
    ----- 
    Taken from https://raw.githubusercontent.com/tweepy/tweepy/master/examples/streaming.py
    """
    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)


class KafkaOutListener(StreamListener):
    """
    This listener handles tweets received from the stream.
    This listener publishes those tweets to Kafka.

    Parameters
    ----------
    kafka_client : type KafkaClient from kafka-python package
        An intialized kafka client connection

    Attributes
    ----------
    simple_producer : type SimpleProducer from kafka-python package
        A simple producer created with the Kafka client

    Notes
    ----- 
    This is a simple version for now that just sends the entire Tweet.  This
    will be expanded later.
    """
    def __init__(self, kafka_client):
        #if isinstance(kafka_client, KafkaClient):
        self.client = kafka_client
        self.simple_producer = SimpleProducer(self.client)


    def on_data(self, data):
        """
        Defines what to do when data comes in
        """
        # Send the message with our simple producer
        self.simple_producer.send_messages('twitter_stream', data)
        return True

    def on_error(self, status):
        """
        Defines what to do when there is an error
        """
        print(status)


class TwitterStream():
    """
    This defines a Twitter stream with the given listener and topic

    Parameters
    ----------
    topic : str or list of str
        This is the topic (or list of topics) the Twitter stream will filter
        down to and return.

    listener : subclass of StreamListener from tweepy
        The listener tells the Twitter stream what to do with the data as it 
        streams in.

    Attributes
    ----------
    auth : type OAuthHandler from tweepy
        This creates the authorization for the Twitter app

    stream : type Stream from tweepy
        This is the initialized twitter stream.

    Notes
    ----- 
    I'm honestly not sure this is a very good class or if it would be better as 
    a function.  I'm not one to be object oriented just for the sake of it.  I 
    probably prefer functional.
    """
    def __init__(self, topic, listener = StdOutListener()):
        self.topic = list(topic)
        self.listener = listener
        
        # Complete authorization
        self.auth = OAuthHandler(config['consumer.key'], config['consumer.secret'])
        self.auth.set_access_token(config['access.token'], config['access.token.secret'])

        # Create stream
        self.stream = Stream(self.auth, self.listener)

    def start_twitter_stream(self):
        """
        Start Twitter pull on specified topic(s)

        Parameters
        ----------
        None

        Returns
        -------
        None, it only starts a process
        """
        self.stream.filter(track=self.topic)

        return None

if __name__ == '__main__':
    # Connect to Kafka cluster
    kafka = KafkaClient("%s:%s" % (config['kafka.host'], config['kafka.port']) )

    # Create a Kafka listener
    kafka_listener = KafkaOutListener(kafka)

    # Construct the list of twitter topics
    twitter_topic_list = config['twitter.topic']
    n_twitter_topic_list = len(twitter_topic_list)
    topic = str(twitter_topic_list[n_twitter_topic_list - 1]) if n_twitter_topic_list == 1 else ' OR '.join(twitter_topic_list)
    
    # Create a Twitter Stream with Kafka listener
    twitter_stream = TwitterStream(topic, kafka_listener)

    # Start the twitter stream
    twitter_stream.start_twitter_stream()