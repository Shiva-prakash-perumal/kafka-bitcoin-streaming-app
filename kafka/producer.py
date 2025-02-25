from kafka import KafkaProducer
import json
import websockets
import asyncio

KAFKA_TOPIC = "bitcoin-transactions"
KAFKA_SERVER = "localhost:9092"

async def fetch_transactions():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    uri = "wss://ws.blockchain.info/inv"
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"op": "unconfirmed_sub"}))

        while True:
            message = await ws.recv()
            producer.send(KAFKA_TOPIC, json.loads(message))

asyncio.run(fetch_transactions())
