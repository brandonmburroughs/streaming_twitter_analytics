from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer

kafka = KafkaClient("192.168.33.10:9092")

print("After connecting to kafka")

consumer = SimpleConsumer(client=kafka, group="my-group", topic="twitter_stream")

for message in consumer:
    print(message)
