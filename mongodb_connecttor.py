import configparser
import json

from kafka import KafkaConsumer
from pymongo import MongoClient


config = configparser.ConfigParser()
config.read("Config/config.ini")
bootstrap_servers = config.get("Kafka", "Bootstrap_Servers")
mongo_port = config.get("Mongo", "Mongo_Port")
mongo_host = config.get("Mongo", "Mongo_Host")
mongo_db = config.get("Mongo", "Mongo_Db")


kafka_topic = "TopicTest"
consumer = KafkaConsumer("TopicTest", bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest',
                         enable_auto_commit=True)

client = MongoClient(mongo_host, mongo_port)
db = client[mongo_db]
col = db["smartbank_da"]

for val in consumer:
    message = val.value
    message = message.decode("utf-8")
    msg = json.loads(message)
    print(type(msg))
    col.insert_one(msg)

client.close()
