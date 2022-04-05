# Package imports
import aiohttp
import asyncio
from datetime import datetime
from dotenv import load_dotenv
import json
import logging
import math
from os import environ, path
from peewee import *
import sys

# Local imports
from models import *
from services.firehouse_db_service import FirehouseDBService
from utilities.states import states

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, ".env"))


class EventIngestionService:
    def __init__(self):
        self.api_key = environ.get("API_KEY")
        self.base_url = environ.get("BASE_URL")
        self.db_service = FirehouseDBService()
        self.metro_id = None

        self.cities = []
        self.db_events = []
        self.saved_events = []
        self.sk_events_for_db = []
        self.venues = {}

    async def main(self):
        # TODO: Pull in all metro areas from db and loop
        metro_area_names = ["Chicago", "Los Angeles", "Miami", "New York", "Phoenix", "San Diego"]
        db_service = self.db_service

        print("Beginning event data ingestion for")
        for metro_area_name in metro_area_names:
            print("------------------------------------------------------------------------")
            print("Fetching metro id for %s...", metro_area_name)
            songkick_metroarea_id = await self.get_source_metro_id(metro_area_name)
            print("\tRetrieved ID: %s" % songkick_metroarea_id)

            print("\tRetrieving events...")
            songkick_metroarea_events = await self.get_sk_metro_events(songkick_metroarea_id)
            print("\t\t%s events retrieved" % len(songkick_metroarea_events))

            # TODO: Merge logic filtering for none and nonexistent artists
            print("\tFiltering out events containing no artist data...")
            events_with_artists = self.filter_events_with_artist(songkick_metroarea_events)
            num_events_without_artist = len(songkick_metroarea_events) - len(events_with_artists)
            print("\t\t%s event(s) filtered out" % num_events_without_artist)
            print("\t\t%s event(s) with artist(s)" % len(events_with_artists))

            print("\tFiltering out events without an artist that exists in the db...")
            events_with_existing_artist = await self.filter_events_by_existing_artist(events_with_artists)
            num_events_artist_doesnt_exist = len(events_with_artists) - len(events_with_existing_artist)
            print("\t\t%s event(s) without an existing artist were filtered out" % num_events_artist_doesnt_exist)
            print("\t\t%s events with existing artist(s)" % len(events_with_existing_artist))

            # # TODO: Remove events that contain 'CANCELLED' in title

            # # TODO: Refactor below method to loop within method and use sets where possible for optimiization
            print("\tFiltering out events that already exist")
            validated_events = await self.remove_existing_events_from_queue(
                metro_area_name, events_with_existing_artist
            )

            if not len(validated_events) > 0:
                print("\t\t%s new events to add for metroplitan area %s" % (len(validated_events), metro_area_name))
            else:
                num_of_filtered_out_events = len(events_with_existing_artist) - len(validated_events)
                print(
                    "\t\t%s new event(s) already exist in the database and were filtered out"
                    % num_of_filtered_out_events
                )
                print("\t\t%s new events validated for insert" % len(validated_events))

                print(f"\tCity Data Preparation: {metro_area_name}")
                self.identify_and_insert_cities(metro_area_name, validated_events)

                print("\tPreparing venue data...")
                venues_to_add = self.prepare_venues_to_add(metro_area_name, validated_events)
                num_of_venues_to_add = len(venues_to_add)
                print(
                    "\t\t%s venue models prepped: %s"
                    % (num_of_venues_to_add, json.dumps(venues_to_add, sort_keys=True, indent=4))
                )
                db_service.save_venues(venues_to_add)

                print("\tPreparing event data...")
                prepped_event_models = self.prepare_events_to_add(validated_events)
                num_of_prepped_event_models = len(prepped_event_models)
                print(
                    "\t\t%s event models prepped: %s"
                    % (num_of_prepped_event_models, json.dumps(prepped_event_models, sort_keys=True, indent=4))
                )

                if num_of_prepped_event_models < 1:
                    print("No new events to insert")
                else:
                    db_service.save_events(prepped_event_models)

                    print("Event Data Preparation")
                    prepped_event_artist_models = self.create_artist_event_relations(validated_events)
                    num_of_prepped_ea_models = len(prepped_event_artist_models)
                    print(
                        "\t\t%s event artist models prepped: %s"
                        % (num_of_prepped_ea_models, json.dumps(prepped_event_artist_models, sort_keys=True, indent=4))
                    )
                    db_service.save_event_artists(prepped_event_artist_models)

    async def build_http_tasks(self, urls: set):
        tasks = set()
        for url in urls:
            request = self.get_request(url)
            tasks.add(request)
        return await asyncio.gather(*tasks, return_exceptions=True)

    def create_artist_event_relations(self, events):
        models_to_save = []
        # Possible Improvements:
        #       - ISSUE: Single DB call for each event.
        #          SOLUTION: Collect details from each loop, and make a singular DB call.
        #
        #       - ISSUE: Identifying an error and properly logging it/making note of it.
        #          SOLUTION: - Try / Except and log.
        #                    - Note the error, and if event_artist model is not created, mark it
        #                      as needing a review manual update.
        #
        #       - ISSUE: Outgoing UI models are hard coded in the function.
        #          SOLUTION: Create directory with UI models / create mapper to match the DB model
        #                    to the respective fields of the outgoing UI model.
        for event in events:
            artists = event["performance"]
            for index, artist in enumerate(artists):
                event_artist_model = {
                    "artist_id": None,
                    "event_id": None,
                    "headliner": False,
                }
                artist_name = artists[index]["artist"]["displayName"]
                event_name = event["displayName"].split("(")[0]
                event_date = event["start"]["date"]
                venue_name = event["venue"]["displayName"]
                artist_model = self.db_service.get_artist(artist_name)
                event_model = self.db_service.get_event(event_date, event_name, venue_name).get()

                if artist_model != None and event_model != None:
                    event_artist_model["artist_id"] = artist_model.id
                    event_artist_model["event_id"] = event_model.id
                    event_artist_model["headliner"] = True if artists[index]["billing"] == "headline" else False
                    models_to_save.append(event_artist_model)
        return models_to_save

    async def filter_events_by_existing_artist(self, events):
        event_artists = set()
        for index, event in enumerate(events):
            for artist_index, artist_data in enumerate(event["performance"]):
                artist_name = artist_data["artist"]["displayName"]
                event_artists.add(artist_name)

        existing_artists_query = Artists.select().where(Artists.name.in_(event_artists))
        existing_artists_names = sorted({artist.name for artist in existing_artists_query})

        matching_events = list(
            filter(
                lambda event: bool(
                    set([obj["artist"]["displayName"] for obj in event["performance"]]).intersection(
                        existing_artists_names
                    )
                ),
                events
                # event['performance'][0]['artist']['displayName'] in existing_artists_names, events
            )
        )

        return matching_events

    def filter_events_with_artist(self, events):
        events_with_artist = list(filter(lambda event: len(event["performance"]) > 0, events))
        return events_with_artist

    async def get_request(self, url):
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.request("GET", url)
                # Note that this may raise an exception for non-2xx responses
                # You can either handle that here, or pass the exception through
                data = await resp.json()
            except Exception as err:
                print(f"Other error occurred: {err}")
                sys.exit()
            else:
                return data["resultsPage"]

    async def get_sk_metro_events(self, sk_metro_area_id: int):
        try:
            url = f"{self.base_url}/metro_areas/{sk_metro_area_id}/calendar.json?{self.api_key}"
            res = await self.get_request(url)
            sk_metro_events = res["results"]["event"]

            total_amount_pages = math.ceil(res["totalEntries"] / 50)
            if total_amount_pages > 1:
                page_url = f"{self.base_url}/events.json?{self.api_key}&location=sk:{sk_metro_area_id}&page="
                urls = {page_url + str(count) for count in range(2, total_amount_pages + 1)}
                additional_pages_data = await self.build_http_tasks(urls)

                for page_resp in additional_pages_data:
                    sk_metro_events += page_resp["results"]["event"]
        except:
            logging.error("Failed to retreive events from sk for sk metro id %s" % sk_metro_area_id)
        return sk_metro_events

    async def get_source_metro_id(self, metro_name):
        url = f"{self.base_url}/search/locations.json?query={metro_name}&{self.api_key}"
        res = await self.get_request(url)
        return res["results"]["location"][0]["metroArea"]["id"]

    async def create_metro_area(name):
        try:
            MetropolitanArea.create(name=name)
            print(f'Successfully created new Metropolitan Area "%s"' % name)
        except:
            print("Error creating Metropolitan Area record for %s" % name)

    def identify_and_insert_cities(self, metropolitan_name, sk_events):
        metro_query = self.db_service.get_metropolitan_id(metropolitan_name)

        if metro_query is None:
            print('\t\t"%s" not found, creating entry' % metropolitan_name)

            try:
                MetropolitanArea.create(name=metropolitan_name)
                print('\t\tSuccessfully created new Metropolitan Area "%s"' % metropolitan_name)
                metro_query = self.db_service.get_metropolitan_id(metropolitan_name)
            except:
                print("\t\tError creating Metropolitan Area record for %s" % metropolitan_name)
                exit

        db_city_names = self.db_service.get_metropolitan_city_names(metropolitan_name)
        cities_to_add = {}

        for sk_event in sk_events:
            city_name = sk_event["location"]["city"].split(",")[0]
            # city_query = self.db_service.get_city(city_name)

            # City does not exist in db and is not in queue
            if city_name not in db_city_names and city_name not in cities_to_add.keys():
                city_data = {
                    "name": city_name,
                    "state": sk_event["location"]["city"].split(",")[1].strip(),
                    "country": "United States",
                    "metropolitan_id": metro_query.id,
                }
                cities_to_add[city_name] = city_data

        cities_to_add = [cities_to_add[city_name] for city_name in cities_to_add.keys()]
        self.db_service.save_cities(cities_to_add)

    def prepare_events_to_add(self, events):
        event_rows = []
        for event in events:
            venue = self.db_service.get_venue(event["venue"]["displayName"]).get()
            event_name = event["displayName"].split("(")[0]
            event_type = event["type"]

            if "festival" in event_name.lower() and event_type.lower() != "festival":
                event_type = "Festival"

            model = {
                "venue_id": venue.id,
                "name": event_name,
                "start_at": event["start"]["time"],
                "tickets_link": event["uri"],
                "date": event["start"]["date"],
                "type": event_type,
                "is_active": not event["status"] == "cancelled",
            }
            event_rows.append(model)
        return event_rows

    def prepare_venues_to_add(self, metropolitan_name, sk_events):
        db_venue_names = self.db_service.get_metropolitan_venue_names(metropolitan_name)
        venues_to_add = {}

        for sk_event in sk_events:
            event_name = sk_event["displayName"]
            city_name = sk_event["location"]["city"].split(",")[0]
            venue_data = {
                "city_id": self.db_service.get_city(city_name).get().id,
                "name": sk_event["venue"]["displayName"],
                "address": "N/A",
            }
            # Check if venue exists, if not create model and add to save queue
            venue_name = venue_data["name"]
            venue_id = self.db_service.get_venue(venue_name)

            if not venue_id and venue_name not in venues_to_add.keys():
                venues_to_add[venue_name] = venue_data
        return [venues_to_add[venue_name] for venue_name in venues_to_add.keys()]

    async def remove_existing_events_from_queue(self, metropolitan_name, songkick_events):
        db_metro_events = self.db_service.get_metropolitan_events(metropolitan_name)
        sk_events_for_db = list()

        logging.debug(f"\tloopinig through sk events: {len(songkick_events)}")
        for sk_index, sk_event in enumerate(songkick_events):
            sk_event = songkick_events[sk_index]
            sk_event_date = sk_event["start"]["date"]
            sk_event_name = sk_event["displayName"].split("(")[0]
            sk_event_venue_name = sk_event["venue"]["displayName"]
            should_add_event = True

            for db_index, db_event in enumerate(db_metro_events):
                equal_event_name = sk_event_name == str(db_event["name"])
                equal_date = sk_event_date == str(db_event["date"])[:10]
                equal_venue = sk_event_venue_name == self.db_service.get_venue_name(db_event["venue_id"])

                if equal_date and equal_venue:
                    should_add_event = False
                    break

            if should_add_event:
                sk_events_for_db.append(sk_event)
            continue

        # Used to remove duplicate dictitonaries.
        set_of_sk_events_for_db = {json.dumps(dictionary, sort_keys=True) for dictionary in sk_events_for_db}
        filtered_sk_events_for_db = [json.loads(dictionary) for dictionary in set_of_sk_events_for_db]
        return filtered_sk_events_for_db

    if __name__ == "__main__":
        asyncio.run(main())
