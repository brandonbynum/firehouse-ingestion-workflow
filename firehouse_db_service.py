from peewee import *
import json
import logging
import models
from dotenv import load_dotenv
from os import getenv, path

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, ".env"))


class FirehouseDBService:
    def __init__(self):
        # self.db_connection = PostgresqlDatabase(
        #     'den4ncj6b3nja4',
        #     user='pxyerzwuholdqp',
        #     password='e45b07a7306a8868f1ebc5bf3a63d46c8a8416e602f1749c4af90f44ee96140e',
        #     host='ec2-34-204-22-76.compute-1.amazonaws.com', port=5432
        # )

        self.db_connection = PostgresqlDatabase(
            getenv("DB_NAME"),
            user=getenv("DB_USER"),
            password=getenv("DB_PW"),
            host=getenv("DB_HOST"),
            port=getenv("DB_PORT"),
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
            logging.error("\t\tError querying Genres table for genre name %s" % genre_name)

    def get_genres(self):
        try:
            return models.Genres.select()
        except:
            logging.error("Error querying Genres table")

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
        num_of_models = len(artist_genre_models)
        try:
            with self.db_connection.atomic():
                models.ArtistGenre.insert_many(artist_genre_models).execute()
            logging.info("\t%s artist genre models saved" % num_of_models)
        except Exception as err:
            logging.error(f"Error saving artist genres: {err}")

    def save_artists(self, artist_names):
        try:
            with self.db_connection.atomic():
                models.Artists.insert_many(artist_names).execute()
            logging.info("\t%s artist models saved" % len(artist_names))
        except:
            logging.error(f"Error saving artists")

    def save_cities(self, city_rows):
        num_of_cities = len(city_rows)
        if num_of_cities < 1:
            logging.info("\t\tNo new cities to add.")
        else:
            city_model_display = len(city_rows), json.dumps(city_rows, sort_keys=True, indent=4)
            try:
                with self.db_connection.atomic():
                    models.Cities.insert_many(city_rows).execute()
                logging.info("\t\t%s cities successfully inserted: %s" % city_model_display)
            except:
                logging.error("\t\tError saving cities: %s" % city_model_display)

    def save_events(self, event_rows):
        try:
            with self.db_connection.atomic():
                models.Events.insert_many(event_rows).execute()
            logging.info("\t\t%s events successfully inserted" % len(event_rows))
        except:
            logging.error("\t\tError saving events")

    def save_venues(self, venue_rows):
        num_of_venues = len(venue_rows)

        if num_of_venues < 1:
            logging.info("\t\tNo new venues to insert.")
        else:
            try:
                with self.db_connection.atomic():
                    models.Venues.insert_many(venue_rows).execute()
                logging.info("\t\t%s venues successfully inserted" % num_of_venues)
            except:
                logging.error("\t\tError saving %s venues" % num_of_venues)

    def save_event_artists(self, models_to_insert):
        event_ids = [model["event_id"] for model in models_to_insert]
        existing_models = models.Event_Artist.select().where(models.Event_Artist.event_id.in_(event_ids))

        # Remove already existng models
        new_models_to_insert = models_to_insert + [model for model in existing_models]

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

            logging.info("\t\t%s event_artist models successfully saved" % len(new_models_to_insert))

            return new_models_to_insert
        except Exception:
            logging.error(f"\t\t{Exception} --- Error occurred while inserting event artist models")
