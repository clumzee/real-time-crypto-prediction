import asyncio
import logging
import websockets
import json
import environ
import pandas as pd
from kafka import KafkaAdminClient
from kafka.producer import KafkaProducer

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

producer = KafkaProducer(bootstrap_servers=env("BOOTSTRAP_SERVERS"))


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
            producer.send(
                topic=env("KAFKA_TOPIC_NAME"),
                key=response_dict["uuid"].encode("utf-8"),
                value=message.encode("utf-8"),
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
