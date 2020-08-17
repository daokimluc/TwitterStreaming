from tweepy import OAuthHandler, Stream, StreamListener
from kafka import KafkaProducer

# Replace the values below with yours Access Token
consumer_key='EL9bP53IKTBWsmNVkXeBaTLH0'
consumer_secret='URuSzNfTCFBq7nPyaSqAjqksECbD2vUFUMq2RfhYSjoloWPQbq'
access_token='1063944351724662784-MVTRTpJk2JdoFTOl3UeVt04orBjSUJ'
access_token_secret='GgwSxgkQVrXfnsQXxbuon3CDmV9GERKz0H8EYfv3Db76p'

class KafkaListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
	    # Send message to Kafka from the producer, "twitter-stream" is the topic
        producer.send("twitterStream", data.encode("utf-8"))  
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
	# Create a simple producer as Kafka client
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    l = KafkaListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

	# Create a stream
    stream = Stream(auth, l)
    stream.filter(track=['#'])  # parameters: e.g. language='en', track=[search keywords for tweets]
