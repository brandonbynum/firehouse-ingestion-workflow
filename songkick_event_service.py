import aiohttp
import asyncio
from dotenv import load_dotenv
import json
import math
import os
from peewee import *
import pprint

from models import *
from showfeur_db import ShowfeurDB
from states import states
from timer import Timer


def dictionary_list_to_set(self, list):
    new_set = {json.dumps(dictionary, sort_keys=True) for dictionary in list}
    return new_set

def dictionary_set_to_list(self, set):
    new_list = [json.loads(dictionary) for dictionary in set]
    return new_list

class SongkickEventService():
    load_dotenv()

    def __init__(self):
        self.api_key = 'apikey=fUiSaa7nFB1tDdh7' #os.getenv('SONGKICK_API_KEY')
        self.base_url = 'https://api.songkick.com/api/3.0' #os.getenv('SONGKICK_BASE_URL')
        self.metro_id = None

        self.cities = []
        self.db_events = []
        self.saved_events = []
        self.sk_events_for_db = []
        self.venues = {}

    async def build_http_tasks(self, urls: set):
        tasks = set()
        for url in urls:
            request = self.sk_get_request(url)
            tasks.add(request)
        return await asyncio.gather(*tasks, return_exceptions=True)

    def create_artist_event_relations(self, event_list):
        print(f'\nCreating event artist relations: {self.metro_name}')
        timer = Timer(f'\nCreating event artist relations: {self.metro_name}')
        timer.begin()

        event_artist_models_to_save = []

        for event_model in event_list:
            for index, artist in enumerate(event_model['artists']):
                event_artist_model = {
                    'artist_id': None,
                    'event_id': None,
                    'is_headliner': False,
                }

                try:
                    event_artist_model['artist_id'] = Artists.select().where(
                        Artists.artist_name == event_model['artists'][index]['displayName']
                    ).get().artist_id
                except:
                    print(f'\t{event_model["artist"][index]["displayName"]} does not exist in db.')
                    #TODO: If artist doesn't exist create artist
                    continue

                event_artist_model['event_id'] = Events.select().where(
                    Events.event_name == event_model['event_name'],
                    Events.event_date == event_model['event_date'],
                    Events.event_start_at == event_model['event_start_at']
                ).get().event_id

                if index == 0:
                    event_artist_model['is_headliner'] = True
                print('{',event_artist_model,'}')

                event_artist_models_to_save.append(event_artist_model)
        try:
            Event_Artist.insert_many(
                event_artist_models_to_save,
                fields=[
                    'artist_id',
                    'event_id',
                    'is_headliner',
                ]
            ).execute()
            print(f'{len(event_artist_models_to_save)} event artist models successfully saved!')
        except:
            print('\tError occurred while inserting event artist models')

        timer.stop()
        timer.results()
        timer.reset()

    async def sk_get_request(self, url):
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

    def prepare_and_save_db(self):
        print(f'\nPREPARING DATA: {self.metro_name}')
        timer = Timer(f'\nPREPARING DATA: {self.metro_name}')
        timer.begin()
        
        db_city_names = {city.city_name for city in self.cities}
        db_venue_names = {self.venues[venue] for venue in self.venues}

        events_to_save = []
        cities_to_save = []
        cities_to_save_names = []
        venues_to_save = []
        venues_to_save_names = []

        for sk_event in self.sk_events_for_db:
            #print(json.dumps(event, sort_keys=True, indent=4))
            sk_event_city_name = sk_event['location']['city'].split(',')[0]
            state_abbreveation = sk_event['location']['city'].split(',')[1].strip()
            sk_event_venue_name = sk_event['venue']['displayName']
            

            # Check if city name exists in db, if not creatte model and add to save queue
            if sk_event_city_name not in db_city_names and sk_event_city_name not in cities_to_save_names:
                cities_to_save.append({
                    'city_name': sk_event_city_name,
                    'city_state': states[state_abbreveation],
                    'city_country': 'United States',
                    'metropolitan': self.metro_id
                })
                cities_to_save_names.append(sk_event_city_name)
                print(f'\n\t{sk_event_city_name} queued to save.')
            elif sk_event_city_name in cities_to_save_names:
                print(f'\n\t{sk_event_city_name} already queued to be saved.')
            elif sk_event_city_name in db_city_names:
                print(f'\n\t{sk_event_city_name} already exists in the db. {[city for city in self.cities if sk_event_city_name == city.city_name]}')
            

            # Check if venue exists, if not create model and add to save queue
            if sk_event_venue_name not in db_venue_names and sk_event_venue_name not in venues_to_save_names:
                venue_model_to_save = {
                    'venue_name': sk_event_venue_name,
                    'venue_address': 'N/A',
                }

                if sk_event_city_name in cities_to_save_names: #city id does not exist yet
                    venue_model_to_save['city_name'] = sk_event_city_name
                elif sk_event_city_name in db_city_names:
                    venue_model_to_save['city_id'] = [city.city_id for city in self.cities if sk_event_city_name == city.city_name][0]

                venues_to_save.append(venue_model_to_save)
                venues_to_save_names.append(sk_event_venue_name)
                print(f'\t{sk_event_venue_name} queued to save.')
            elif sk_event_venue_name in venues_to_save:
                print(f'\t{sk_event_venue_name} already queued to be saved.')
            elif sk_event_venue_name in db_venue_names:
                print(f'\t{sk_event_venue_name} already exists in the db ({[venue for venue in self.venues if sk_event_venue_name == self.venues[venue]][0]})')   
            # TODO: Log the event and that ticket link is needed

            # Creatte model obj for event
            sk_event_model_to_save = {
                'artists': sk_event['performance'],
                'event_date': sk_event['start']['date'],
                'event_name': sk_event['displayName'],
                'event_start_at': sk_event['start']['time'],
                'event_type': 'Concert',
                'tickets_link': sk_event['uri'],
            }
            if sk_event_venue_name in venues_to_save_names:
                sk_event_model_to_save['venue_name'] = sk_event_venue_name
            elif sk_event_venue_name in db_venue_names:
                sk_event_model_to_save['venue_id'] = [
                    venue for venue in self.venues if sk_event_venue_name == self.venues[venue]][0]
            events_to_save.append(sk_event_model_to_save)

        #  BEGIN SAVING ITEMS ===================================================================================
        print(f'\n\tCities to save ({len(cities_to_save)}): {cities_to_save}')
        if len(cities_to_save) > 0:
            try:
                Cities.insert_many(
                    cities_to_save,
                    fields=[
                        'city_name',
                        'city_state',
                        'city_country',
                        'metropolitan'
                    ]
                ).execute()
                print(f'{len(cities_to_save)} city models successfully saved!')
            except:
                print('Error occurred while inserting city models')

        print(f'\nVenues to save ({len(venues_to_save)}): {json.dumps(venues_to_save, indent=4, sort_keys=True)}')
        final_venue_models_to_save = []
        if len(venues_to_save) > 0:
            for venue_model in venues_to_save:
                if 'city_name' in venue_model.keys():
                    try:
                        city_id = Cities.select().where(Cities.city_name == venue_model['city_name'])
                        venue_model['city_id'] = city_id
                        del venue_model['city_name']
                    except:
                        print(f'Error finding city_id for venue: {venue_model.venue_name}')
                final_venue_models_to_save.append(venue_model)
            
            try:
                Venues.insert_many(
                    final_venue_models_to_save,
                    fields=[
                        'city_id',
                        'venue_name',
                        'venue_address',
                    ]
                ).execute()
                print(f'{len(final_venue_models_to_save)} venue models successfully saved!')
            except:
                print('Error occurred while inserting venue models')

        print(f'\nEvents to save ({len(events_to_save)}): {json.dumps(events_to_save, indent=4, sort_keys=True)}')
        final_event_models_to_save = []
        if len(events_to_save) > 0:
            for event_model in events_to_save:
                if 'venue_name' in event_model.keys():
                    try:
                        venue_id = Venues.select().where(Venues.venue_name == event_model['venue_name'])
                        event_model['venue_id'] = venue_id
                        del venue_model['venue_name']
                    except:
                        print(f'Error finding venue_id for event: {event_model.event_name}')
                final_event_models_to_save.append(event_model)
            
            try:
                Events.insert_many(
                    final_event_models_to_save,
                    fields=[
                        'event_date',
                        'event_name',
                        'event_start_at',
                        'event_type',
                        'tickets_link',
                        'venue_id',
                    ]
                ).execute()
                print(f'{len(final_event_models_to_save)} event models successfully saved!')    
            except:
                print('Error occurred while inserting event models')

        self.saved_events = final_event_models_to_save   
        
        timer.stop()
        timer.results()
        timer.reset()

    async def filter_events_by_existing_artist(self, events):
        timer = Timer('\n\tREMOVE NON ELECTRONIC EVENTS')
        timer.begin()

        print(f'\tinput: \n\t\ttype: {type(events)} \n\t\tlen: {len(events)}')
        event_artists = sorted(
            set(
                event['performance'][0]['artist']['displayName'] for event in events
            )
        )

        print(f'\n\tevent artists:')
        for artist in event_artists:
            print(f'\n\t\t{artist}')
        
        existing_artists_query = Artists.select().where(
            Artists.artist_name.in_(event_artists)
        )
        existing_artists_names = sorted({
            artist.artist_name for artist in existing_artists_query
        })

        print(f'\n\tmatching artist names:')
        for artist in existing_artists_names:
            print(f'\n\t\t{artist}')

        matching_events = list(
            filter(
                lambda event: 
                event['performance'][0]['artist']['displayName'] in existing_artists_names, events
            )
        )
        
        #print(json.dumps(matching_events, sort_keys=True, indent=4))
        print(f'\n\toutput: \n\t\tlen:{len(matching_events)} \n\t\ttype:{type(matching_events)}')
        return matching_events
        timer.stop()
        timer.results()
        timer.reset()


    ## DIRECT DATABASE SOLUUTION
    #   goal: remove fetched songkick events that already exist
    #   TODO 1. query for existing events in metropolitan area
    #
    #   6. loop through each songkick fetched event
    #       a. loop through each existing event
    #           - loop through existinig venue ids
    #               1. grab existing event's venue's name
    #           - check if following event fields match: venue name, date, event start time, artist name
    #               1.if so, delete event from list of fetched events, since it already exists
    async def remove_events_existing_in_db(self, metropolitan_name, songkick_events):
        timer = Timer('\nREMOVE EXISTING EVENTS USING DB')
        timer.begin()

        db_metro_events = ShowfeurDB().get_metropolitan_events(metropolitan_name)
        sk_events_for_db = list()

        # loop through all songkick fetched events
        print(f'\tloopinig through sk events: {len(songkick_events)}')
        for index, sk_event in enumerate(songkick_events):
            sk_event = songkick_events[index]
            print(f'\n\t\tSongkick Event Being : {sk_event["displayName"]} / INDEX - {index}')
            sk_event_venue_name = sk_event['venue']['displayName']
            sk_event_date = sk_event['start']['date']
            sk_event_start_time = sk_event['start']['time']

        #     # loop through all db events
            print(f'looping through all db events: {len(db_metro_events)}')
            for position, db_event in enumerate(db_metro_events):
                #print(f'existing event: {db_event.event_name} / POSITION: {position}')
                db_event_date = str(db_event.event_date)[:10]
                #print(f'\n\t\t{db_event_date}')
                db_event_start_time = str(db_event.event_start_at)
                #db_event_venue_name = ShowfeurDB.get_venue_name(db_event.venue_id)
                break
                # print(f'FILTERED EVENTS: {len(sk_events_for_db)}')
                # # Compare venue name, date, start time of the two events, artist name
                # if sk_event_venue_name == db_event_venue_name and sk_event_date == db_event_date and sk_event_start_time == db_event_start_time:
                #     print(f'event exists: {db_event.event_name} will not be added.')
                #     break
                
        #         if position == len(self.db_events) - 1:
        #             print(f'event does not exist: {sk_event["displayName"]}, preparing to add')
        #             sk_events_for_db.append(sk_event)               
        #     continue

        # print(f'\nnumber of {self.metro_name} events start: {len(songkick_events)}')
        # # Used to remove duplicate dictitonaries.
        # set_of_sk_events_for_db = {json.dumps(dictionary, sort_keys=True) for dictionary in sk_events_for_db}
        # filtered_sk_events_for_db = [json.loads(dictionary) for dictionary in set_of_sk_events_for_db]   
        # self.sk_events_for_db = filtered_sk_events_for_db
        # print(f'number of {self.metro_name} events end: {len(self.sk_events_for_db)}')

        pg_db.close()
        timer.stop()
        timer.results()
        timer.reset()
    
    async def get_sk_metroarea_id(self, metro_name):
        url = f'{self.base_url}/search/locations.json?query={metro_name}&{self.api_key}'
        res = await self.sk_get_request(url)
        return res['results']['location'][0]['metroArea']['id']

    async def get_sk_metro_events(self, sk_metro_area_id: int):
        url = f'{self.base_url}/metro_areas/{sk_metro_area_id}/calendar.json?{self.api_key}'
        res = await self.sk_get_request(url)
        sk_metro_events = res['results']['event']

        total_amount_pages = math.ceil(res['totalEntries'] / 50)
        if total_amount_pages > 1:
            page_url = f'{self.base_url}/events.json?{self.api_key}&location=sk:{sk_metro_area_id}&page='
            urls = {page_url + str(count) for count in range(2, total_amount_pages + 1)}
            additional_pages_data = await self.build_http_tasks(urls)

            for page_resp in additional_pages_data:
                sk_metro_events += page_resp['results']['event']
        return sk_metro_events

    def filter_events_with_artist(self, events):
        events_with_artist = list(filter(lambda event: len(event['performance']) > 0, events))
        return events_with_artist
