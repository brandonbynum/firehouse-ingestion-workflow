import asyncio
from typing import Dict
from bs4 import BeautifulSoup
import requests
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from models import Artists, ArtistGenre, Genres


def get_artist_dicts() -> Dict:
    print("\nQuerying for all artists...")
    artists_dicts = Artists.get().dicts()
    existing_artists = {}
    for dict in artists_dicts:
        existing_artists[dict["name"]] = dict["id"]
    print(f"\t{len(existing_artists.keys())} existing artists returned")
    return existing_artists


async def main():
    url = os.environ.get("ARTIST_IMPORT_URL")
    existing_artists = get_artist_dicts()

    print("\nQuerying for all genres...")
    genres = {}
    genre_dicts = Genres.get().dicts()
    for dict in genre_dicts:
        genres[dict["name"]] = dict["id"]
    print(f"\t{len(genres.keys())} genres returned")

    print("\nScraping web pages based on genre...")
    scraped_artist_genres = []
    artists_to_create = []
    for genre_name in genres.keys():
        print(f"\t{genre_name}")
        html = requests.get(url + genre_name).content

        try:
            soup = BeautifulSoup(html, features="html.parser")
            all_a_elements = soup.find_all("a", class_="artist-name")
            print(f"\t\tElements found: {len(all_a_elements)}")

        except:
            print(f"\tFailed to retrieve chosic contents for: {genre_name}")
            continue

        print("\t\tExtracting artists and pairing w/ genre")
        for element in all_a_elements:
            artist_name = element.string.extract().strip()
            pair = {artist_name: genre_name}

            if artist_name not in existing_artists.keys() and artist_name not in [x["name"] for x in artists_to_create]:
                artists_to_create.append({"name": artist_name})

            if pair not in scraped_artist_genres:
                scraped_artist_genres.append(pair)
    print(f"\t{len(scraped_artist_genres)} scraped artist genre pairs")
    print(f"\t{len(artists_to_create)} new artists to create")

    print(f"\Creating new artist names...")
    Artists.create_many(artists_to_create)

    existing_artists = get_artist_dicts()

    print("\nQuerying for all artist genre pairings...")
    artist_genres_dicts = ArtistGenre.get().dicts()
    existing_pairs = []
    for row in artist_genres_dicts:
        existing_pairs.append({row["artist_name"]: row["genre_name"]})
    print(f"\t{len(existing_pairs)} existing artist genre pairs")

    print(f"\nRemoving scraped artist genre pairs which already exist...")
    artist_genres_to_create = []
    old_pairs = []
    for pair in scraped_artist_genres:
        if pair not in existing_pairs:
            # convert pair names to id
            artist_name = list(pair.keys())[0]
            artist_id = existing_artists[artist_name]
            genre_name = list(pair.values())[0]
            genre_id = genres[genre_name]

            artist_genres_to_create.append({"artist_id": artist_id, "genre_id": genre_id})
        else:
            old_pairs.append(pair)
    print(f"\t{len(old_pairs)} scraped artist genre pairs pairs which already exist")
    print(f"\t{len(artist_genres_to_create)} scraped artist genre pairs to create")

    print(f"\tCreating new artist genre pairs")
    ArtistGenre.create_many(artist_genres_to_create)


if __name__ == "__main__":
    asyncio.run(main())
