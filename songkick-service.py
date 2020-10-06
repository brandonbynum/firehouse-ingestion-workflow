import aiohttp
import asyncio
import json
import math
from peewee import *

from models import *
from states import states
from timer import Timer
        
class SongKickService():
    def __init__(self, metro_name):
        self.api_key = 'fUiSaa7nFB1tDdh7'

        self.metro_id = None
        self.sk_id = None
        self.cities = []
        self.db_events = []
        self.sk_events =[]
        self.metro_name = metro_name
        self.venues = {}

        metro_id_query = MetropolitanArea.select().where(MetropolitanArea.metropolitan_name == metro_name)
        self.metro_id = metro_id_query.get()

        cities_query = Cities.select().where(Cities.metropolitan == self.metro_id)
        cities = [city for city in cities_query]
        self.metro_cities = cities

        existing_venues_in_city_query = Venues.select().where(Venues.city.in_(cities))
        for venue in existing_venues_in_city_query:
            self.venues[venue.venue_id] = venue.venue_name

    async def sk_get_request(self, url: str):
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.request('GET', url)
                # Note that this may raise an exception for non-2xx responses
                # You can either handle that here, or pass the exception through
                data = await resp.json()
            except Exception as err:
                print(f'Other error occurred: {err}')
                return err
            else:
                return data['resultsPage']

    # def prepare_data(self):
    #     for metro_name in self.metro_area_names:
    #         print(f'PREPARING DATA: {metro_name}')
    #         metro_data = self.metro_area_data[metro_name]

    #         events_to_save = []
    #         saved_cities = set()
    #         saved_venues = set()
    #         for event in metro_data['events']:
    #             #print(json.dumps(event, sort_keys=True, indent=4))
    #             city_name = event['location']['city'].split(',')[0]
    #             state_abbreveation = event['location']['city'].split(',')[1].strip()
    #             venue_name = event['venue']['displayName']

    #             # Check if city name exists
    #             existing_city_names = {city.city_name for city in metro_data['cities']}
    #             if city_name not in existing_city_names and city_name not in saved_cities:
    #                 Cities(
    #                     city_name = city_name,
    #                     city_state = states[state_abbreveation],
    #                     city_country = 'United States',
    #                     metropolitan = self.metro_area_data[metro_name]['id']
    #                 ).save()
    #                 saved_cities.add(city_name)
    #                 print(f'{city_name} saved to the database.')
    #             elif city_name in saved_cities:
    #                 print(f'{city_name} saved in this sessioin')
    #             elif city_name in existing_city_names:
    #                 print(f'{city_name} already exists in the database prior to this session.')

                
    #             # Check if venue exists, if not create the venue and save
    #             existing_venue_names = [metro_data['venues'][venue] for venue in set(metro_data['venues'])]
    #             if venue_name not in existing_venue_names and venue_name not in saved_venues:
    #                 city_id = [city for city in metro_data['cities'] if city.city_name == city_name][0]
    #                 Venues(
    #                     city = city_id,
    #                     venue_name = venue_name,
    #                     venue_address = 'N/A',
    #                 ).save()
    #                 saved_venues.add(venue_name)
    #                 print(f'{venue_name} saved to the database.')
    #                 # TODO: Log the venue creation and that an address is needed
    #             elif venue_name in saved_venues:
    #                 print(f'{venue_name} saved in this sessioin')
    #             elif venue_name in existing_venue_names:
    #                 print(f'{venue_name} already exists in the database prior to this session.')
                    
    #             # TODO: Log the event and that ticket link is needed
    #             venue_model = Venues.select().where(Venues.venue_name == venue_name).get()
                
    #             event_model = {
    #                 'venue': venue_model.venue_id,
    #                 'event_date': event['start']['date'],
    #                 'event_name': event['displayName'],
    #                 'event_start_at': event['start']['time'],
    #                 'event_type': 'Concert',
    #                 'tickets_link': event['uri'],
    #             }

    #             if event_model not in events_to_save:
    #                 print('event queued to save')
    #                 events_to_save.append(event_model)

            # Events.insert_many(
            #     events_to_save, 
            #     fields=[
            #         Events.venue, 
            #         Events.event_date,
            #         Events.event_name,
            #         Events.event_start_at,
            #         Events.event_type,
            #         Events.tickets_link,
            #     ]).execute()

    ## TODO: API SOLUTION
    #   goal: remove fetched songkick events that already exist
    #   1. run this solution, set flag to decide whether or not to run sql solution if api fails
    #   2. call events list api end point
    #   3. loop through each retrieved songkick event (find fastest algorithm to sort through )
    #       a. loop through all existing events
    #           -  check if following event fields match: venue name, date, event start time, artist name
    #               1.  if so, delete event from lisit of fetched events, since it already exists
    async def remove_existing_events_using_api(self, metro_area_id):
        return None

    async def filter_events_by_existing_artist(self):
        timer = Timer('REMOVE NON ELECTRONIC EVENTS')
        timer.begin()

        existing_artists_query = Artists.select()
        existing_artists_names = {artist.artist_name for artist in existing_artists_query}
        print(f'existing artists: {existing_artists_names}')

        matching_events = list(filter(
            lambda event: 
            event['performance'][0]['artist']['displayName'] in existing_artists_names, self.sk_events
        ))
        #matching_events = [event for event in self.sk_events if event['performance'][0]['artist']['displayName'] in existing_artists_names]
        self.sk_events = matching_events
        print(f'# of SK events after filtering by artist: {len(self.sk_events)}, \ntype:{type(self.sk_events)}')
        timer.stop()
        timer.results()
        timer.reset()


    ## DIRECT DATABASE SOLUUTION
    #   goal: remove fetched songkick events that already exist
    #   1. connect to db
    #   2. query for metropolitan area id
    #   3. query for list of cities w/ matching metropolitan area id
    #   4. query for list of venues w/ matchinig city_id
    #   5. query for existing events w/ matching venue_id
    #
    #   6. loop through each songkick fetched event
    #       a. loop through each existing event
    #           - loop through existinig venue ids
    #               1. grab existing event's venue's name
    #           - check if following event fields match: venue name, date, event start time, artist name
    #               1.if so, delete event from list of fetched events, since it already exists
    async def remove_events_existing_in_db(self):
        timer = Timer('\nREMOVE EXISTING EVENTS USING DB')
        timer.begin()
        #connection = pg_db.connect()
        existing_venue_ids = list(self.venues.keys())
        print(f'existing venue ids: {existing_venue_ids}')
        events_query = Events.select().where(Events.venue.in_(existing_venue_ids))
        self.db_events = [event for event in events_query]
        print(f'LENGTTH OF DB EVENTTS: {len(events_query)}')

        sk_events_for_db = list()
        # loop through all songkick fetched events
        print(f'loopinig through sk events: {len(self.sk_events)}')
        for index, sk_event in enumerate(self.sk_events):
            sk_event = self.sk_events[index]
            print(f'\nretrieved event being checked: {sk_event["displayName"]} / INDEX - {index}')
            sk_event_venue_name = sk_event['venue']['displayName']
            sk_event_date = sk_event['start']['date']
            sk_event_start_time = sk_event['start']['time']

            if len(self.db_events) == 0:
                sk_events_for_db.append(sk_event)
                print('sk_event added')

            # loop through all db events
            print(f'looping through all db events: {len(self.db_events)}')
            for position, db_event in enumerate(self.db_events):
                print(f'existing event: {db_event.event_name} / POSITION: {position}')
                db_event_date = str(db_event.event_date)[:10]
                db_event_start_time = str(db_event.event_start_at)
                db_event_venue_name = [self.venues[venue_id] for venue_id in self.venues if venue_id == db_event.venue.venue_id][0]
                
                print(f'FILTERED EVENTS: {len(sk_events_for_db)}')
                # Compare venue name, date, start time of the two events, artist name
                # print(f'sk/db venue names: {sk_event_venue_name} / {db_event_venue_name}')
                # print(f'sk/db event date: {sk_event_date} / {db_event_date}')
                # print(f'sk/db start time: {sk_event_start_time} / {db_event_start_time}')
                print(f'sk/db venue names: {sk_event_venue_name == db_event_venue_name}')
                print(f'sk/db event date: {sk_event_date == db_event_date}')
                print(f'sk/db start time: {sk_event_start_time == db_event_start_time}')

                if sk_event_venue_name == db_event_venue_name and sk_event_date == db_event_date and sk_event_start_time == db_event_start_time:
                    print(f'event exists: {db_event.event_name} will not be added.')
                    break
                
                if position == len(self.db_events) - 1:
                    print(f'event does not exist: {sk_event["displayName"]}, preparing to add')
                    sk_events_for_db.append(sk_event)               
            continue

        print(f'\nnumber of {self.metro_name} events start: {len(self.sk_events)}')
        set_of_sk_events_for_db = {json.dumps(d, sort_keys=True) for d in sk_events_for_db}
        filtered_sk_events_for_db = [json.loads(t) for t in set_of_sk_events_for_db]            
        self.sk_events_for_db = filtered_sk_events_for_db
        print(f'number of {self.metro_name} events end: {len(self.sk_events_for_db)}')

        pg_db.close()
        timer.stop()
        timer.results()
        timer.reset()
    
    async def get_sk_metroarea_id(self):
        timer = Timer('RETRIEVE METROAREA ID')
        timer.begin()

        location_url = f'https://api.songkick.com/api/3.0/search/locations.json?query={self.metro_name}&apikey={self.api_key}'
        res = await self.sk_get_request(location_url)
        self.sk_id = res['results']['location'][0]['metroArea']['id']
        print(f'metro area: {self.metro_name}')
        print(f'songkick id: {self.sk_id}')
        
        timer.stop()
        timer.results()
        timer.reset()

    async def get_metro_sk_events(self):
        timer = Timer('RETRIEVE METRO SONGKICK EVENTS')
        timer.begin()            

        url = f'https://api.songkick.com/api/3.0/metro_areas/{self.sk_id}/calendar.json?apikey={self.api_key}'
        res = await self.sk_get_request(url)
        sk_events = res['results']['event']
        pages = math.ceil(res['totalEntries'] / 50)

        if pages > 1:
            tasks = set()
            # Build Tasks // MAKE REUSABLE FUNCTION
            for currentPage in range(pages):
                currentPage += 1
                url = f'https://api.songkick.com/api/3.0/events.json?apikey={self.api_key}&location=sk:{self.sk_id}&page={currentPage}'
                tasks.add(self.sk_get_request(url))
            paged_data = await asyncio.gather(*tasks, return_exceptions=True)

            remaining_events = []
            for page_resp in paged_data:
                remaining_events += page_resp['results']['event']
            sk_events += remaining_events
        events_with_artist = list(filter(lambda event: len(event['performance']) > 0, sk_events))
        self.sk_events = events_with_artist
        print(f'# of SK events retrieved: {len(self.sk_events)}, type:{type(self.sk_events)}')
        timer.stop()
        timer.results()
        timer.reset()

# TODO: Optimize code by using sets instead of lists
async def main():
    timer = Timer('ENTIRE SERVICE')
    timer.begin()

    metro_area_name = 'Phoenix'
    instance = SongKickService(metro_area_name)
    await instance.get_sk_metroarea_id()
    await instance.get_metro_sk_events()
    await instance.filter_events_by_existing_artist()

    # TODO: Refactor below method to loop within method and use sets where possible for optimiization
    await instance.remove_events_existing_in_db()

    print('\n')
    print(instance.metro_name, 'RESULTS:')
    for event in instance.sk_events_for_db:
        print(event['displayName'])
    print('\n')

    #instance.prepare_data()

        
    # TODO: Map to event to model

    timer.stop()
    timer.results()
    timer.reset()


if __name__ == '__main__':
    asyncio.run(main())
