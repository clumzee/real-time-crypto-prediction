import asyncio
import logging
import websockets
import socket
import json
import environ
import pandas as pd
from distutils.debug import DEBUG
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder.appName("spark session").master("local[*]").getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")



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


env = environ.Env(
    # set casting, default value
    DEBUG=(bool, False)
)

environ.Env.read_env()

# producer = KafkaProducer(bootstrap_servers=env("BOOTSTRAP_SERVERS"))


with open("message.json", "r") as json_file:
    message_dict = json.load(json_file)

logging.basicConfig(level=logging.INFO)


def log_message(message: str) -> None:
    logging.info(f"Message:- {message}")


async def consumer_handler(websocket) -> None:
    async for message in websocket:
        # log_message(type(message))
        response_dict = json.loads(message)

        if response_dict["symbol_id"] == env("BTC_SYMBOL"):
            log_message(f"Bitcoin Symbol detected, Price:- {response_dict}")
            # bitcoin_data.append(response_dict, ignore_index=True)

            bitcoin_df = (
                spark.readStream.format("socket")
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                .option("subscribe", kafka_topic_name)
                .option("startingOffsets", "latest")
                .load()
            )


            # log_message(response_dict.keys())


async def consume(url: str) -> None:

    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps(message_dict))

        await consumer_handler(websocket)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(consume(url=env("WEBSOCKET_URL")))
    loop.run_forever()
