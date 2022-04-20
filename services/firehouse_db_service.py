import peewee
from peewee import *
import json
import logging
from os import getenv
from models import *


class FirehouseDBService:
    def __init__(self):
        self.db_connection = PostgresqlDatabase(
            getenv("DB_NAME"),
            user=getenv("DB_USER"),
            password=getenv("DB_PW"),
            host=getenv("DB_HOST"),
            port=getenv("DB_PORT"),
        )

    def close_connection(self):
        self.db_connection.close()
        print("Connection closed.")

    def get_artists(self, artist_name=None):
        try:
            if artist_name:
                return Artists.select().where(Artists.name == artist_name).get()
            return Artists.select().get()
        except Artists.DoesNotExist:
            return None

    def get_all_artist_genres(self):
        try:
            return (
                ArtistGenre.select(ArtistGenre, Artists.name.alias("artist_name"), Genres.name.alias("genre_name"))
                .join(Genres, on=(ArtistGenre.genre_id == Genres.id))
                .switch(ArtistGenre)
                .join(Artists, on=(ArtistGenre.artist_id == Artists.id))
            )
        except DoesNotExist:
            return None

    def get_city(self, city_name):
        try:
            return Cities.select().where(Cities.name == city_name)
        except DoesNotExist:
            return None

    def get_event(self, event_date, event_name, venue_name):
        try:
            return (
                Events.select()
                .where(
                    Events.name == event_name,
                    Events.date == event_date,
                    Events.venue_id == self.get_venue(venue_name),
                )
                .get()
            )
        except Events.DoesNotExist:
            return None

    def get_genres(self, genre_name=None):
        try:
            if genre_name:
                return Genres.select().where(fn.Lower(Genres.name) == genre_name)
            return Genres.select()
        except:
            print("\t\tError querying Genres table for genre name %s" % genre_name)

    def get_metropolitan_id(self, metro_name):
        try:
            return MetropolitanArea.select().where(MetropolitanArea.name == metro_name).get()
        except DoesNotExist:
            return None

    def get_metropolitan_cities(self, metro_name):
        metro_id = self.get_metropolitan_id(metro_name)
        return Cities.select().where(Cities.metropolitan_id == metro_id)

    def get_metropolitan_events(self, metro_name):
        metropolitan_venues = self.get_metropolitan_venues(metro_name)
        venue_ids = [venue.id for venue in metropolitan_venues.iterator()]
        query = Events.select().where(Events.venue_id.in_(venue_ids))
        return [event for event in query.dicts().iterator()]

    def get_venue(self, venue_name):
        try:
            return Venues.select().where(Venues.name == venue_name)
        except DoesNotExist:
            return None

    def get_venue_name(self, venue_id):
        query = Venues.select().where(Venues.id == venue_id)
        return query.get().name

    def get_metropolitan_venues(self, metro_name):
        metropolitan_cities = self.get_metropolitan_cities(metro_name)
        city_ids = [city.id for city in metropolitan_cities.iterator()]
        return Venues.select().where(Venues.city_id.in_(city_ids))

    def get_metropolitan_venue_names(self, metro_name):
        query = self.get_metropolitan_venues(metro_name)
        return [venue.name for venue in query.iterator()]

    def save_artist_genres(self, artist_genre_models):
        try:
            with self.db_connection.atomic():
                ArtistGenre.insert_many(artist_genre_models).execute()
            num_of_models = len(artist_genre_models)
            print("\t%s artist genre models saved" % num_of_models)
        except Exception as err:
            print(f"Error saving artist genres: {err}")

    def save_artists(self, artist_names):
        try:
            with self.db_connection.atomic():
                Artists.insert_many(artist_names).execute()
            print("\t%s artist models saved" % len(artist_names))
        except:
            print(f"Error saving artists")

    def save_cities(self, city_rows):
        num_of_cities = len(city_rows)
        if num_of_cities < 1:
            print("\t\tNo new cities to add.")
        else:
            city_model_display = len(city_rows), json.dumps(city_rows, sort_keys=True, indent=4)
            try:
                with self.db_connection.atomic():
                    Cities.insert_many(city_rows).execute()
                print("\t\t%s cities successfully inserted: %s" % city_model_display)
            except:
                print("\t\tError saving cities: %s" % city_model_display)

    def save_events(self, event_rows):
        try:
            with self.db_connection.atomic():
                Events.insert_many(event_rows).execute()
            print("\t\t%s events successfully inserted" % len(event_rows))
        except:
            print("\t\tError saving events")

    def save_venues(self, venue_rows):
        num_of_venues = len(venue_rows)

        if num_of_venues < 1:
            print("\t\tNo new venues to insert.")
        else:
            try:
                with self.db_connection.atomic():
                    Venues.insert_many(venue_rows).execute()
                print("\t\t%s venues successfully inserted" % num_of_venues)
            except:
                print("\t\tError saving %s venues" % num_of_venues)

    def save_event_artists(self, models_to_insert):
        try:
            event_ids = [model["event_id"] for model in models_to_insert]
            existing_models = Event_Artist.select().where(Event_Artist.event_id.in_(event_ids))
            new_models_to_insert = list(filter(lambda x: x not in existing_models.dicts(), models_to_insert))

            with self.db_connection.atomic():
                Event_Artist.insert_many(
                    new_models_to_insert,
                    fields=[
                        "event_id",
                        "artist_id",
                        "headliner",
                    ],
                ).execute()
            print("\t\t%s event_artist models successfully saved" % len(new_models_to_insert))

        except:
            logging.error("\t\t%s event_artist models failed to save" % len(new_models_to_insert))

        return new_models_to_insert
