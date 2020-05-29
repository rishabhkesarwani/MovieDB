import requests

api_key = "95398bbe14ab93e06460f08561621f1b"
base_url = "https://api.themoviedb.org/3"
discover_endpoint = "/discover/{media}"
credits_endpoint = "/{media}/{id}/credits"

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


# Fetches all the ids from all the pages for particular multimedia and returns as set.
def getMultimediaIdList(media="movie"):
    list_total_pages = -1
    count = 1
    id_set = set()
    while count <= list_total_pages or list_total_pages == -1:
        page_list = getMultimediaList(count, media)
        if list_total_pages == -1:
            list_total_pages = page_list["total_pages"]
        try:
            for result in page_list["results"]:
                id_set.add(result["id"])
        except KeyError:
            print(page_list)
        count += 1
    return id_set


# Fetches the credits of a particular multimedia
def getCredits(multimedia_id, media="movie"):
    return requests.get(
        f"{base_url}{credits_endpoint.format(media=media, id=multimedia_id)}",
        params={"api_key": api_key},
    ).json()


# Returs the cast (actor and actress) ids in set.
def castList(media="movie"):
    id_set = getMultimediaIdList(media)
    cast_set = set()
    for id in id_set:
        try:
            credits = getCredits(id, media)
            for cast in credits["cast"]:
                cast_set.add(cast["id"])
        except KeyError:
            print(id, credits)
    return cast_set


movie_cast_set = castList("movie")
tv_cast_set = castList("tv")
# finds intersection of casts from movie and tv shows and counts the length.
print(len(movie_cast_set.intersection(tv_cast_set)))
