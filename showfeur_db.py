from peewee import *
import json
import logging

from utilities.pretty_print import pretty_print
import models

class ShowfeurDB():
    def __init__(self):
        self.db_connection = PostgresqlDatabase(
            'den4ncj6b3nja4',
            user='pxyerzwuholdqp',
            password='e45b07a7306a8868f1ebc5bf3a63d46c8a8416e602f1749c4af90f44ee96140e',
            host='ec2-34-204-22-76.compute-1.amazonaws.com', port=5432
        )

    def close_connection(self):
        self.db_connection.close()
        logging.info('Connection closed.')

    def get_artist(self, artist_name):
        Artists = models.Artists
        try:
            return Artists.select().where(
                Artists.artist_name == artist_name
            ).get()
        except DoesNotExist:
            return None
    
    def get_matching_artists(self, artist_list):
        Artists = models.Artists
        return Artists.select().where(Artists.artist_name.in_(artist_list))

    def get_city(self, city_name):
        try:
            return (
                models.Cities
                .select()
                .where(models.Cities.city_name == city_name)
            )
        except DoesNotExist:
            return None

    def get_event(self, event_date, event_name, venue_name):
        try:
            return (
                models.Events
                .select()
                .where(
                    models.Events.event_name == event_name,
                    models.Events.event_date == event_date,
                    models.Events.venue_id == self.get_venue(venue_name)
                )
            )
        except DoesNotExist:
            return None

    def get_metropolitan_id(self, metro_name):
        MetropolitanArea = models.MetropolitanArea
        try:
            return (
                MetropolitanArea
                .select()
                .where(MetropolitanArea.metropolitan_name == metro_name)
                .get()
            ) 
        except DoesNotExist:
            return None

    def get_metropolitan_cities(self, metro_name):
        Cities = models.Cities
        metro_id = self.get_metropolitan_id(metro_name)
        #query = 
        return Cities.select().where(Cities.metropolitan == metro_id)
        #return [city.city_id for city in query]

    def get_metropolitan_city_names(self, metro_name):
        cities = self.get_metropolitan_cities(metro_name)
        
        return [city.city_name for city in cities.iterator()]

    def get_metropolitan_events(self, metro_name):
        Events = models.Events
        metropolitan_venues = self.get_metropolitan_venues(metro_name)
        venue_ids = [venue.venue_id for venue in metropolitan_venues.iterator()]

        query = Events.select().where(Events.venue_id.in_(venue_ids))
        return [event for event in query.dicts().iterator()]
        
    def get_venue(self, venue_name):
        try:
            return (
                models.Venues
                .select()
                .where(
                    models.Venues.venue_name == venue_name
                )
            )
        except DoesNotExist:
            return None

    def get_venue_name(self, venue_id):
        Venues = models.Venues
        query = (
            Venues
            .select()
            .where(Venues.venue_id == venue_id)
        )
        return query.get().venue_name

    def get_metropolitan_venues(self, metro_name):
        metropolitan_cities = self.get_metropolitan_cities(metro_name)
        city_ids = [city.city_id for city in metropolitan_cities.iterator()]
        Venues = models.Venues
        return (
            Venues
            .select()
            .where(Venues.city_id.in_(city_ids))
        )

    def get_metropolitan_venue_names(self, metro_name):
        query = self.get_metropolitan_venues(metro_name)
        return [venue.venue_name for venue in query.iterator()]

    def save_cities(self, city_rows):
        try:
            with self.db_connection.atomic():
                models.Cities.insert_many(city_rows).execute()
            print('\t\tCities saved:')
        except:
            print('Error saving cities.')
        for city in city_rows:
            pretty_print(city, True)
        
    def save_events(self, event_rows):
        try:
            with self.db_connection.atomic():
                models.Events.insert_many(event_rows).execute()
            print('\t\Events saved')
        except:
            print('Error saving events.')
    
    def save_venues(self, venue_rows):
        try:
            with self.db_connection.atomic():
                models.Venues.insert_many(venue_rows).execute()
            print('\t\tVenues saved')
        except:
            print('Error saving venues.')
    
        for venue in venue_rows:
            pretty_print(venue, True)

    def save_event_artists(self, data_row):
        try:
            models.Event_Artist.insert_many(
                data_row,
                fields=[
                    'artist_id',
                    'event_id',
                    'is_headliner',
                ]
            ).execute()
            print(f'{len(data_row)} event artist models successfully saved!')
            pretty_print(data_row, True)
        except:
            print('\tError occurred while inserting event artist models')
