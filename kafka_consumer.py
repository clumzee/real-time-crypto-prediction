import json
from operator import index
import os
import logging
import pandas as pd
import environ
from kafka.consumer import KafkaConsumer
from kafka import TopicPartition, OffsetAndMetadata

logging.basicConfig(level=logging.INFO)


env = environ.Env(
    # set casting, default value
    DEBUG=(bool, False)
)

environ.Env.read_env()


def bitcoin_key_deserializer(key):
    return key.decode("utf-8")


def bitcoin_value_deserializer(value):
    return json.loads(value.decode("utf-8"))

bitcoin_data = pd.DataFrame.from_dict(
    {
        "time_exhange": [],
        "time_coinapi": [],
        "uuid": [],
        "price": [],
        "size": [],
        "taker_side": [],
        "symbol_id": [],
        "sequence": [],
        "type": [],
    }
)



consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    group_id=env("CONSUMER_GROUP"),
    key_deserializer=bitcoin_key_deserializer,
    value_deserializer=bitcoin_value_deserializer,
    enable_auto_commit=False
)

consumer.subscribe(env("KAFKA_TOPIC_NAME"))

for record in consumer:
    bitcoin_temp_df = pd.DataFrame(record.value, index=[0])
    bitcoin_data = bitcoin_data.append(bitcoin_temp_df)
    print(f"Length of Data: {len(bitcoin_data)}")
    # logging.info(f"Key:- {record.key}, \n Value: {record.value}, \n Offset: {record.offset}")
    topic_partition = TopicPartition(record.topic, record.partition)
    offset = OffsetAndMetadata(record.offset+1, record.timestamp)
    consumer.commit({
        topic_partition:offset
    })

    if len(bitcoin_data) > 100:
        bitcoin_data.to_csv("bitcoin.csv", index=False)
        break
