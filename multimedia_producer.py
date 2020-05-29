import requests
from kafka import KafkaProducer
import threading

api_key = "95398bbe14ab93e06460f08561621f1b"
base_url = "https://api.themoviedb.org/3"
discover_endpoint = "/discover/{media}"
credits_endpoint = "/{media}/{id}/credits"

bootstrap_servers = "localhost:9092"
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: bytes(f"{v}", encoding="utf-8"),
)

# Fetches a page of multimedia (movies and tv shows) details released or aired in november 2019
def getMultimediaList(page, media="movie"):
    params = {
        "page": page,
        "api_key": api_key,
    }
    if media == "tv":
        params["air_date.gte"] = "2019-11-01"
        params["air_date.lte"] = "2019-11-30"
    else:
        params["release_date.gte"] = "2019-11-01"
        params["release_date.lte"] = "2019-11-30"
    return requests.get(
        f"{base_url}{discover_endpoint.format(media=media)}", params=params,
    ).json()


# Fetches all the ids from all the pages for particular multimedia and write in respective kafka topic.
def produceMultimediaIds(media="movie"):
    list_total_pages = -1
    count = 1
    while count <= list_total_pages or list_total_pages == -1:
        page_list = getMultimediaList(count, media)
        if list_total_pages == -1:
            list_total_pages = page_list["total_pages"]
        try:
            for result in page_list["results"]:
                producer.send(f"media-{media}", result["id"])
        except KeyError:
            print(page_list)
        count += 1


# two threads to produce parallely in media-movie, media-tv kafka topics
for media in ["movie", "tv"]:
    threading.Thread(target=produceMultimediaIds, args=(media,)).start()
