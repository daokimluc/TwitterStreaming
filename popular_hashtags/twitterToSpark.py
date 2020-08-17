import socket
import sys
import requests
import requests_oauthlib
import json

ACCESS_TOKEN = '1063944351724662784-MVTRTpJk2JdoFTOl3UeVt04orBjSUJ'
ACCESS_SECRET = 'GgwSxgkQVrXfnsQXxbuon3CDmV9GERKz0H8EYfv3Db76p'
CONSUMER_KEY = 'EL9bP53IKTBWsmNVkXeBaTLH0'
CONSUMER_SECRET = 'URuSzNfTCFBq7nPyaSqAjqksECbD2vUFUMq2RfhYSjoloWPQbq'
twitter_auth = requests_oauthlib.OAuth1(
    CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)


def streamTweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    stream_param = [('language', 'en'), ('locations',
                                         '-140.99778, 41.6751050889, -52.6480987209, 83.23324'), ('track', '#')]
    stream_url = url + '?' + \
        '&'.join([str(t[0]) + '=' + str(t[1]) for t in stream_param])
    # resp is a data stream received from the Twitter APIs
    resp = requests.get(stream_url, auth=twitter_auth, stream=True)
    print(stream_url, resp)
    return resp


def twitter_to_spark(resp, tcp_conn):
    # Read the stream continuously
    for line in resp.iter_lines():
        try:
            all_tweet = json.loads(line)
            # pyspark can't accept stream, add '\n'
            tweet_pure_text = all_tweet['text'] + '\n'

            print("Tweet pure text is : " + tweet_pure_text)
            print("------------------------------------------")
            tcp_conn.send(tweet_pure_text.encode('utf-8', errors='ignore'))
        except:
            e = sys.exc_info()[0]
            print("Error is: %s" % e)


# Main logic: make TCP connection to Spark app, forward the data stream to Spark app
conn = None
TCP_IP = "localhost"
TCP_PORT = 9090
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((TCP_IP, TCP_PORT))
sock.listen(1)
print("Twitter end server is waiting for TCP connection...")
conn, addr = sock.accept()
print("Spark process connected. streaming tweets to Spark process.")
resp = streamTweets()
twitter_to_spark(resp, conn)
