import requests
from kafka import KafkaConsumer
from cast_producer import produceCastId
import threading

api_key = "95398bbe14ab93e06460f08561621f1b"
base_url = "https://api.themoviedb.org/3"
discover_endpoint = "/discover/{media}"
credits_endpoint = "/{media}/{id}/credits"

consumer = None

# Fetches the credits of a particular multimedia
def getCredits(multimedia_id, media="movie"):
    return requests.get(
        f"{base_url}{credits_endpoint.format(media=media, id=multimedia_id)}",
        params={"api_key": api_key},
    ).json()

# produces the cast ids for particular multimedia in the respective kafka topic
def getCastIds(media):
    consumer = KafkaConsumer(
        f"media-{media}", value_deserializer=lambda m: m.decode("utf-8")
    )
    for message in consumer:
        try:
            media_credits = getCredits(message.value, media)
            for cast in media_credits["cast"]:
                produceCastId(cast["id"], media)
        except KeyError:
            print(f"credit key-error {message.value} - {media_credits}")

# two threads to produce parallely
for media in ["movie", "tv"]:
    threading.Thread(target=getCastIds, args=(media,)).start()
