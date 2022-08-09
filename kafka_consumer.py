import json
import os
import logging
import pandas as pd
import environ
from kafka.consumer import KafkaConsumer

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
    bootstrap_servers=env("BOOTSTRAP_SERVERS"),
    group_id=env("CONSUMER_GROUP"),
    key_deserializer=bitcoin_key_deserializer,
    value_deserializer=bitcoin_value_deserializer,
)

consumer.subscribe(env("KAFKA_TOPIC_NAME"))

for record in consumer:
    bitcoin_temp_df = pd.DataFrame.from_dict(record.value)
    bitcoin_data.append(bitcoin_temp_df)
    # logging.info(f"Key:- {record.key}, \n Value: {record.value}, \n Offset: {record.offset}")

    if len(bitcoin_data) > 100:
        pass
