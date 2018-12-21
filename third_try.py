import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import multiprocessing
import threading, logging, time

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, SimpleClient
from kafka import KafkaConsumer

#import mysql.connector
#from cassandra.cluster import Cluster

# These access token and secret are obtained when applying for a twitter api:
access_token = "760357019001954304-LsDTXYyzAREMFSnLvcvlJYB6AClh9sj"
access_token_secret =  "zN57Xc9I0HkugQC113X43F81PxAY7Dyos6GXXGZcXiRyl"
consumer_key =  "zmXr9bDWoRFzQtTevEL9CUted"
consumer_secret =  "tBlBFQtoDV3MdWD1DtDI5A6DPjdcAyQA0mgI8JFoOlXXyyoQoS"


# This is a class to create a streamlistener, which we will use later to create a twitter stream:
class StdOutListener(StreamListener):
    # This methods sends data from the twitter stream into the kafka topic 'twitter'
    def on_data(self, data):
        kafka = SimpleClient("localhost:9092")
        producer = SimpleProducer(kafka)
        producer.send_messages("twitter", data.encode('utf-8'))
        #print(data)
        return True

    def on_error(self, status):

        print('ERROR!!!! --------------------------->')


# This function creates a stream listener and starts a stream which shows all messages that contain the word 'python'
def prod():
    # A stream listener object is created
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # A tweepy stream object is created by using the stream listener and the api credentials
    stream = Stream(auth, l)
    # Finally, a twitter stream which contains all tweets that have the word 'python' is created
    stream.filter(track="Donald Trump")


# This class consumes messages from kafka and inserts them into cassandra and mysql
# It inherits from the multiprocessing.Process class in order to be able to start several consumers
class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        os.environ[
            'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 pyspark-shell'
        sc = SparkContext()
        sc.setLogLevel('ERROR')
        #Streaming window 1s
        ssc = StreamingContext(sc, 1)
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092')

        # Subscribe to kafka topic named 'twitter'
        consumer.subscribe(['twitter'])

        kafkaParams = {"bootstrap.servers": "localhost:9092"}
        #print('antes')
        tweet_DStream = KafkaUtils.createDirectStream(ssc, ['twitter'], kafkaParams)
        #print('después')
        tweet_DStream.foreachRDD(lambda twitterRDD : twitterRDD.foreach(lambda line : print(line)))

        #print(tweet_DStream)

        ssc.start()
        ssc.awaitTermination()
        print(str(stream_of_tweets))
        consumer.close()


# This function is used to start a kafka consumer - which will, in turn, send the tweets to cassandra and mysql
def cons():
    tasks = [
        Consumer()
    ]

    for t in tasks:
        t.start()


# Finally, we use 2 threads to be able to simultaneously send and consume messages from kafka
t1 = threading.Thread(target=prod)
t1.start()

t2 = threading.Thread(target=cons)
t2.start()