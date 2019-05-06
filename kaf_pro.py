from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler

consumer_key =  'iMAuRZe9DnxdzA3vCFxB5Vlj1'
consumer_secret = '7xUszvPHTjnGrpbn8YFuTZ1abxJYgiZFDFGt4RWgFZ7ZoGxfIg'
access_token = '779683967469367296-VScuMcwWRy0n7xSxFgCPtwSCzVe4BI6'
access_token_secret =  'ZjRuAdmvqed8vmnMaxgEqwcEDbCkuK50tv71FOYOHqkWU'

class KafkaListener(StreamListener):
    def on_data(self, streamData):
        producer.send_messages("twitterStream", streamData.encode('utf-8'))
        print (streamData)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
listener = KafkaListener()
stream = Stream(auth, listener)
stream.filter(track="twitterStream")
