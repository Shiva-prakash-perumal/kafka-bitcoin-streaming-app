from kafka import KafkaConsumer
import json
import redis
from datetime import datetime

KAFKA_TOPIC = "bitcoin-transactions"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

for message in consumer:
    tx = message.value
    timestamp = datetime.now().strftime("%H:%M")

    # Store transaction count per minute
    r.zincrby("transactions_count", 1, timestamp)

    # Store high-value addresses
    for out in tx['x']['out']:
        addr = out.get('addr', 'unknown')
        value = out['value']
        r.zincrby("high_value_addresses", value, addr)

    # Keep last 100 transactions
    r.lpush("latest_transactions", json.dumps(tx))
    r.ltrim("latest_transactions", 0, 99)
