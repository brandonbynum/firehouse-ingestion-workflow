from peewee import *
import json

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
            print('No results')

    def get_metropolitan_cities(self, metro_name):
        Cities = models.Cities
        metro_id = self.get_metropolitan_id(metro_name)
        cities_query = Cities.select().where(Cities.metropolitan == metro_id)
        return [city.city_id for city in cities_query]

    def get_metropolitan_events(self, metro_name):
        Events = models.Events
        metropolitan_venues = self.get_metropolitan_venues(metro_name)
        venue_ids = list(metropolitan_venues.keys())

        events = Events.select().where(Events.venue_id.in_(venue_ids))
        for event in events:
            print(f'\n\tTEST : : : {event}')

        return events
        
    def get_venue_name(self,venue_db_index):
        Venues = models.Venues
        print(Venues.select().where(Venues.venue_id == venue_db_index))

    def get_metropolitan_venues(self, metro_name):
        city_ids = self.get_metropolitan_cities(metro_name)
        Venues = models.Venues
        existing_venues_in_city_query = (
            Venues
            .select()
            .where(Venues.city_id.in_(city_ids))
        )
                
        venue_dict = {}
        for venue in existing_venues_in_city_query:
            venue_dict[venue.venue_id] = venue.venue_name

        #print(json.dumps(venue_dict, indent=4))
        return venue_dict
