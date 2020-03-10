import json

from kafka import KafkaConsumer
from pymongo import MongoClient

MONGO_HOST = "127.0.0.1"
MONGO_PORT = 27017

consumer = KafkaConsumer("SmartBankSpark3Kafka",bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',
     enable_auto_commit=True)

client = MongoClient('localhost', 27017)
db = client["smartbank"]
col = db["smartbank_da"]

for val in consumer:
    message = val.value
    message = message.decode("utf-8")
    msg = json.loads(message)
    print(type(msg))
    col.insert_one(msg)


client.close()







