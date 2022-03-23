from peewee import *
import json
import logging

from utilities.pretty_print import pretty_print
import models


class ShowfeurDB:
    def __init__(self):
        # self.db_connection = PostgresqlDatabase(
        #     'den4ncj6b3nja4',
        #     user='pxyerzwuholdqp',
        #     password='e45b07a7306a8868f1ebc5bf3a63d46c8a8416e602f1749c4af90f44ee96140e',
        #     host='ec2-34-204-22-76.compute-1.amazonaws.com', port=5432
        # )

        self.db_connection = PostgresqlDatabase(
            "showfeur_development", user="brandon", password="121314121314", host="localhost", port=5432
        )

    def close_connection(self):
        self.db_connection.close()
        logging.info("Connection closed.")

    def get_artist(self, artist_name):
        Artists = models.Artists
        try:
            return Artists.select().where(Artists.name == artist_name).get()
        except DoesNotExist:
            return None

    def get_matching_artists(self, artist_list):
        Artists = models.Artists
        return Artists.select().where(Artists.name.in_(artist_list))

    def get_city(self, city_name):
        try:
            return models.Cities.select().where(models.Cities.name == city_name)
        except DoesNotExist:
            return None

    def get_event(self, event_date, event_name, venue_name):
        try:
            return models.Events.select().where(
                models.Events.name == event_name,
                models.Events.date == event_date,
                models.Events.venue_id == self.get_venue(venue_name),
            )
        except DoesNotExist:
            return None

    def get_genre(self, genre_name):
        try:
            return models.Genres.select().where(fn.Lower(models.Genres.name) == genre_name)
        except:
            print("Error querying Genres table.")

    def get_genres(self):
        try:
            return models.Genres.select()
        except:
            print("Error querying Genres table.")

    def get_metropolitan_id(self, metro_name):
        MetropolitanArea = models.MetropolitanArea
        try:
            return MetropolitanArea.select().where(MetropolitanArea.name == metro_name).get()
        except DoesNotExist:
            return None

    def get_metropolitan_cities(self, metro_name):
        Cities = models.Cities
        metro_id = self.get_metropolitan_id(metro_name)
        return Cities.select().where(Cities.metropolitan_id == metro_id)

    def get_metropolitan_city_names(self, metro_name):
        cities = self.get_metropolitan_cities(metro_name)
        return [city.name for city in cities.iterator()]

    def get_metropolitan_events(self, metro_name):
        Events = models.Events
        metropolitan_venues = self.get_metropolitan_venues(metro_name)
        venue_ids = [venue.id for venue in metropolitan_venues.iterator()]
        query = Events.select().where(Events.venue_id.in_(venue_ids))
        return [event for event in query.dicts().iterator()]

    def get_venue(self, venue_name):
        try:
            return models.Venues.select().where(models.Venues.name == venue_name)
        except DoesNotExist:
            return None

    def get_venue_name(self, venue_id):
        Venues = models.Venues
        query = Venues.select().where(Venues.id == venue_id)
        return query.get().name

    def get_metropolitan_venues(self, metro_name):
        metropolitan_cities = self.get_metropolitan_cities(metro_name)
        city_ids = [city.id for city in metropolitan_cities.iterator()]
        Venues = models.Venues
        return Venues.select().where(Venues.city_id.in_(city_ids))

    def get_metropolitan_venue_names(self, metro_name):
        query = self.get_metropolitan_venues(metro_name)
        return [venue.name for venue in query.iterator()]

    def save_artist_genres(self, artist_genre_models):
        try:
            with self.db_connection.atomic():
                models.ArtistGenre.insert_many(artist_genre_models).execute()
            print("\tArtist genres saved")
        except Exception as err:
            print(f"Error saving artist genres: {err}")

    def save_artists(self, artist_names):
        pretty_print(artist_names, True)
        try:
            with self.db_connection.atomic():
                models.Artists.insert_many(artist_names).execute()
            print("\tArtists saved")
        except:
            print(f"Error saving artists")

    def save_cities(self, city_rows):
        try:
            with self.db_connection.atomic():
                models.Cities.insert_many(city_rows).execute()
            print("\t\tCities saved:")
        except:
            print("Error saving cities.")
        for city in city_rows:
            pretty_print(city, True)

    def save_events(self, event_rows):
        try:
            with self.db_connection.atomic():
                models.Events.insert_many(event_rows).execute()
            print("\tEvents saved")
        except:
            print("Error saving events.")

    def save_venues(self, venue_rows):
        try:
            with self.db_connection.atomic():
                models.Venues.insert_many(venue_rows).execute()
            print("\t\tVenues saved")
        except:
            print("Error saving venues.")

        for venue in venue_rows:
            pretty_print(venue, True)

    def save_event_artists(self, models_to_insert):
        print(models_to_insert)
        event_ids = [model["event_id"] for model in models_to_insert]
        existing_models = models.Event_Artist.select().where(models.Event_Artist.event_id.in_(event_ids))

        # Remove already existng models
        new_models_to_insert = models_to_insert + [model for model in existing_models]
        print(f"New models to insert: {new_models_to_insert}")
        try:
            with self.db_connection.atomic():
                models.Event_Artist.insert_many(
                    new_models_to_insert,
                    fields=[
                        "event_id",
                        "artist_id",
                        "headliner",
                    ],
                ).execute()
            print(f"{len(new_models_to_insert)} event artist models successfully saved!")
            pretty_print(new_models_to_insert, True)
            return new_models_to_insert
        except Exception:
            print(f"\t{Exception} --- Error occurred while inserting event artist models")
