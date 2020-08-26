import aiohttp
import asyncio
import json
import math
from peewee import *

from models import *
from states import states
from timer import Timer
        
class SongKickService():
    def __init__(self, metro_area_names):
        self.api_key = 'fUiSaa7nFB1tDdh7'
        self.metro_area_data = {}
        self.metro_area_names = metro_area_names
        
        for name in self.metro_area_names:
            self.metro_area_data[name] = {
                'id': None,
                'songkick_id': None,
                'events': [],
                'cities': [],
                'venues': [],
            }

        for metro_name in metro_area_names:
            metro_id_query = MetropolitanArea.select().where(MetropolitanArea.metropolitan_name == metro_name)
            self.metro_area_data[metro_name]['id'] = metro_id_query.get()

            cities_query = Cities.select().where(Cities.metropolitan == self.metro_area_data[metro_name]['id'])
            cities = [city for city in cities_query]
            self.metro_area_data[metro_name]['cities'] = cities

            existing_venues_in_city_query = Venues.select().where(Venues.city.in_(cities))
            self.metro_area_data[metro_name]['venues'] = {}
            for venue in existing_venues_in_city_query:
                self.metro_area_data[metro_name]['venues'][venue.venue_id] = venue.venue_name

    async def songkick_get_request(self, url: str):
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

    def has_artist(self, event):
        if len(event['performance']) > 0:
            return event

    def prepare_data(self):
        events_to_save = set()
        cities_to_save = {}
        venues_to_save = {}
        for metro_name in self.metro_area_names:
            metro_data = self.metro_area_data[metro_name]
            for event in metro_data['events']:
                #print(json.dumps(event, sort_keys=True, indent=4))
                city_name = event['location']['city'].split(',')[0]
                state_abbreveation = event['location']['city'].split(',')[1].strip()
                venue_name = event['venue']['displayName']

                existing_city_names = {city.city_name for city in metro_data['cities']}
                if city_name not in existing_city_names and city_name not in set(cities_to_save.keys()):
                    cities_to_save[city_name] = (
                        Cities(
                            city_name = city_name,
                            city_state = states[state_abbreveation],
                            city_country = 'United States',
                            metropolitan = self.metro_area_data[metro_name]['id']
                        )
                    )
                
                existing_venue_names = [metro_data['venues'][venue] for venue in set(metro_data['venues'])]
                if venue_name not in existing_venue_names and venue_name not in set(venues_to_save.keys()):
                    city_id = [city for city in metro_data['cities'] if city.city_name == city_name][0]
                    # venues_to_save[venue_name] = (
                    #     Venues(
                    #         city = city_id,
                    #         venue_name = venue_name
                    #     )
                    # )

                    Venues(
                        venue_id = Venues.select(fn.Max(Venues.venue_id))[0].venue_id + 1,
                        city = city_id,
                        venue_name = venue_name,
                        venue_address = '',
                    ).save()
                    # TODO: Log the venue creation and that an address is needed

                # TODO: Log the event and that ticket link is needed
                venue_model = Venues.select().where(Venues.venue_name == venue_name).get()

                print('venue_model', venue_model)
                break
                events_to_save.add(
                    Events(
                        venue = venue_model,
                        event_date = event['start']['date'],
                        event_name = event['displayName'],
                        event_start_at = event['start']['time'],
                        event_type = 'Concert',
                        tickets_link = event['uri'],
                    )
                )

                print(len(events_to_save))
                print(len(cities_to_save))
                print(len(venues_to_save))

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

    async def remove_non_electronic_events(self):
        timer = Timer('REMOVE NON ELECTRONIC EVENTS')
        timer.begin()
        existing_artists_query = Artists.select()
        existing_artists_names = {artist.artist_name.strip() for artist in existing_artists_query}
        metro_area_names = set(self.metro_area_data.keys())

        for metro_area_index, metro_area_name in enumerate(self.metro_area_names):
            retrieved_events = self.metro_area_data[metro_area_name]['events']
            matching_events = [event for event in retrieved_events if event['performance'][0]['artist']['displayName'] in existing_artists_names]
            self.metro_area_data[metro_area_name]['events'] = matching_events
            
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
    async def remove_existing_events_using_db(self):
        timer = Timer('REMOVE EXISTING EVENTS USING DB')
        timer.begin()
        #connection = pg_db.connect()
        for metro_area_name in self.metro_area_data:
            existing_venues_in_metro = self.metro_area_data[metro_area_name]['venues']
            venue_ids = list(existing_venues_in_metro.keys())
            events_query = Events.select().where(Events.venue.in_(venue_ids))
            existing_events_in_metro_area = [event for event in events_query]

            filitered_events = []
            # loop through all songkick fetched events
            for index, retrieved_event in enumerate(self.metro_area_data[metro_area_name]['events']):
                retrieved_event_venue_name = retrieved_event['venue']['displayName']
                retrieved_event_date = retrieved_event['start']['date']
                retrieved_event_start_time = retrieved_event['start']['time']
                # loop through all existing events
                for existing_event in existing_events_in_metro_area:
                    existing_event_date = str(existing_event.event_date)[:10]
                    existing_event_start_at = str(existing_event.event_start_at)
                    existing_event_venue_name = [existing_venues_in_metro[venue] for venue in existing_venues_in_metro if venue == existing_event.venue.venue_id][0]

                    # Compare venue name, date, start time of the two events, artist name
                    if retrieved_event_venue_name == existing_event_venue_name and retrieved_event_date == existing_event_date and retrieved_event_start_time == existing_event_start_at:
                        self.metro_area_data[metro_area_name]['events'][index]
                        del self.metro_area_data[metro_area_name]['events'][index]
        pg_db.close()
        timer.stop()
        timer.results()
        timer.reset()
    
    async def retrieve_metroarea_ids(self):
        timer = Timer('RETRIEVE METROAREA IDs')
        timer.begin()

        if len(self.metro_area_names) > 1:
            tasks = []
            for name in self.metro_area_names:
                location_url = f'https://api.songkick.com/api/3.0/search/locations.json?query={name}&apikey={self.api_key}'
                tasks.append(self.songkick_get_request(location_url))
            all_location_data = await asyncio.gather(*tasks, return_exceptions=True)

            metro_area_ids = {}
            for location in all_location_data:
                metro_area_data = location['results']['location'][0]['metroArea']
                metro_area_id = metro_area_data['id']
                metro_area_name = metro_area_data['displayName']

                self.metro_area_data[metro_area_name]['songkick_id'] = metro_area_id

        else:
            data = await self.songkick_get_request(url=location_url)
            metro_area_id = data['results']['location'][0]['metroArea']['id']
            metro_area_name = metro_area_names[0]
            self.metro_area_data[metro_area_name]['id'] = metro_area_id
        
        timer.stop()
        timer.results()
        timer.reset()

    async def retrieve_metroarea_events(self):
        timer = Timer('RETRIEVE METROAREA EVENTS')
        timer.begin()

        async def call_remaining_pages(num_of_pages, metro_id):
            tasks = set()
            # Build Tasks // MAKE REUSABLE FUNCTION
            for currentPage in range(num_of_pages):
                currentPage += 1
                url = f'https://api.songkick.com/api/3.0/events.json?apikey={self.api_key}&location=sk:{metro_id}&page={currentPage}'
                tasks.add(self.songkick_get_request(url))
            paged_data = await asyncio.gather(*tasks, return_exceptions=True)

            remaining_events = []
            for page_resp in paged_data:
                remaining_events += page_resp['results']['event']
                
            return remaining_events

        for area_name in self.metro_area_data.keys():
            events_url = f'https://api.songkick.com/api/3.0/metro_areas/{self.metro_area_data[area_name]["songkick_id"]}/calendar.json?apikey={self.api_key}'
            metro_event_data = await self.songkick_get_request(events_url)
            events_results = metro_event_data['results']['event']
            pages = math.ceil(metro_event_data['totalEntries'] / 50)

            if pages > 1:
                remaining_page_events = await call_remaining_pages(num_of_pages=pages, metro_id=self.metro_area_data[area_name]['songkick_id'])
                events_results += remaining_page_events
            self.metro_area_data[area_name]['events'] = list(filter(self.has_artist, events_results))

        timer.stop()
        timer.results()
        timer.reset()

# TODO: Optimize code by using sets instead of lists
async def main():
    timer = Timer('ENTIRE SERVICE')
    timer.begin()

    metro_area_names = set(['Phoenix', 'Tucson'])
    instance = SongKickService(metro_area_names)

    await instance.retrieve_metroarea_ids()
    await instance.retrieve_metroarea_events()
    await instance.remove_non_electronic_events()

    # TODO: Refactor below method to loop within method and use sets where possible for optimiization
    await instance.remove_existing_events_using_db()

    print('\n')
    for metro_name in instance.metro_area_data.keys():
        #print(instance.metro_area_data[metro_name])
        for event in instance.metro_area_data[metro_name]['events']:
            print(event['displayName'])
    print('\n')

    instance.prepare_data()

        
    # TODO: Map to event to model

    timer.stop()
    timer.results()
    timer.reset()


if __name__ == '__main__':
    asyncio.run(main())
