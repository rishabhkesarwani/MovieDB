from kafka import KafkaProducer
import json

bootstrap_servers = "localhost:9092"
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

# produces castId in respective kafka topics
def produceCastId(castId, media="movie"):
    producer.send(f"cast-{media}", {"cast": castId, "media": media})
