import aiohttp
import asyncio
from dotenv import load_dotenv
import json
import logging
import math
import os
from peewee import *
import pprint

from models import *
from showfeur_db import ShowfeurDB
from utilities.states import states
from utilities.timer import Timer
from utilities.pretty_print import pretty_print


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
        self.db_service = ShowfeurDB()
        self.metro_id = None

        self.cities = []
        self.db_events = []
        self.saved_events = []
        self.sk_events_for_db = []
        self.venues = {}

    async def build_http_tasks(self, urls: set):
        tasks = set()
        for url in urls:
            request = self.get_request(url)
            tasks.add(request)
        return await asyncio.gather(*tasks, return_exceptions=True)

    def create_artist_event_relations(self, events):
        print('\nCreating event artist relations:')
        models_to_save = []

        for event in events:
            print('\n\tEvent being evaluated: %s' % event['displayName'])
            artists = event['performance']
            for index, artist in enumerate(artists):
                artist_name = artists[index]['artist']['displayName']
                event_artist_model = {
                    'artist_id': None,
                    'event_id': None,
                    'is_headliner': False,
                }

                event_name = event['displayName'].split('(')[0]
                event_date = event['start']['date']
                venue_name = event['venue']['displayName']

                artist_id = self.db_service.get_artist(artist_name).artist_id
                event_id = self.db_service.get_event(event_date, event_name, venue_name).get().event_id
                
                if not artist_id:
                    print('\t\tArtist "%s" does not exist in db.' % artist_name)
                elif not event_id:
                    print('\t\tEvent "%s" does not exist in db.' % event_name)
                else:
                    print('\t\tArtist Id (%s):' % artist_id)
                    print('\t\tEvent Id (%s):' % event_id)
                    event_artist_model['artist_id'] = artist_id
                    event_artist_model['event_id'] = event_id

                    if index == 0:
                        event_artist_model['is_headliner'] = True
                    models_to_save.append(event_artist_model)
        
        return models_to_save

    async def filter_events_by_existing_artist(self, events):
        print(f'\tinput: \n\t\ttype: {type(events)} \n\t\tlen: {len(events)}')
        print(f'\n\t\tevent artists:')
        event_artists = set()
        for index, event in enumerate(events):
            for artist_index, artist_data in enumerate(event['performance']):
                artist_name = artist_data['artist']['displayName']
                event_artists.add(artist_name)
                print(f'\t\t\t{artist_name}')
            
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

        print(f'\n\toutput: \n\t\tlen:{len(matching_events)} \n\t\ttype:{type(matching_events)}')
        return matching_events

    def filter_events_with_artist(self, events):
        events_with_artist = list(filter(lambda event: len(event['performance']) > 0, events))
        return events_with_artist

    async def get_request(self, url):
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

    async def get_sk_metro_events(self, sk_metro_area_id: int):
        url = f'{self.base_url}/metro_areas/{sk_metro_area_id}/calendar.json?{self.api_key}'
        res = await self.get_request(url)
        sk_metro_events = res['results']['event']

        total_amount_pages = math.ceil(res['totalEntries'] / 50)
        if total_amount_pages > 1:
            page_url = f'{self.base_url}/events.json?{self.api_key}&location=sk:{sk_metro_area_id}&page='
            urls = {page_url + str(count)
                    for count in range(2, total_amount_pages + 1)}
            additional_pages_data = await self.build_http_tasks(urls)

            for page_resp in additional_pages_data:
                sk_metro_events += page_resp['results']['event']
        return sk_metro_events
    
    async def get_sk_metroarea_id(self, metro_name):
        url = f'{self.base_url}/search/locations.json?query={metro_name}&{self.api_key}'
        res = await self.get_request(url)
        return res['results']['location'][0]['metroArea']['id']

    def prepare_cities_to_add(self, metropolitan_name, sk_events):        
        db_city_names = self.db_service.get_metropolitan_city_names(metropolitan_name)
        print('\tDatabase City Names for Metro "%s": %s' % (metropolitan_name, db_city_names))
        
        cities_to_add = {}
        for sk_event in sk_events:
            event_name = sk_event['displayName']
            print('\n\tEvaluating event: %s' % event_name)
            # print(json.dumps(sk_event, sort_keys=True, indent=4))
            state_abbr = sk_event['location']['city'].split(',')[1].strip()
            city_data = {
                'city_name': sk_event['location']['city'].split(',')[0],
                'city_state': states[state_abbr],
                'city_country': 'United States',
                'metropolitan_id': self.db_service.get_metropolitan_id(metropolitan_name).metropolitan_id
            }

            # Check if city name exists in db, if not creatte model and add to save queue
            city_name = city_data['city_name']
            city_id = self.db_service.get_city(city_name)
            print('\t\tChecking if "%s" exists in cities table...' % city_name)
            if city_id:
                print(f'\t\t\t{city_name} already exists in the db. {db_city_names}')
            elif city_name in cities_to_add.keys():
                print(f'\t\t\t{city_name} already queued to be saved.')
            else:
                cities_to_add[city_name] = city_data
                print(f'\t\t\t{city_name} queued to save.')
        return [cities_to_add[city_name] for city_name in cities_to_add.keys()]
    
    def prepare_events_to_add(self, events):
        event_rows = []
        for event in events:
            #pretty_print(event, True)
            venue = self.db_service.get_venue(event['venue']['displayName']).get()
            model = {
                'venue_id': venue.venue_id,
                'event_name': event['displayName'].split('(')[0],
                'event_start_at': event['start']['time'],
                # 'event_end_at': ,
                # 'tickets_link':,
                'event_date': event['start']['date'],
                'event_type': 'Concert',
                'is_active': not event['status'] == 'cancelled',
            }
            event_rows.append(model)
        print(event_rows)
        return event_rows
    
    def prepare_venues_to_add(self, metropolitan_name, sk_events):
        db_venue_names = self.db_service.get_metropolitan_venue_names(metropolitan_name)
        print('\t Database Venue Names in %s : %s' % (metropolitan_name, db_venue_names))
        
        venues_to_add = {}
        for sk_event in sk_events:
            event_name = sk_event['displayName']
            print('\n\tEvaluating event: %s' % event_name)
            city_name = sk_event['location']['city'].split(',')[0]
            venue_data = {
                'city_id': self.db_service.get_city(city_name).get().city_id,
                'venue_name': sk_event['venue']['displayName'],
                'venue_address': 'N/A',
            }

            # Check if venue exists, if not create model and add to save queue
            venue_name = venue_data['venue_name']
            print('\t\tChecking if "%s" exists in venues table...' % venue_name)
            venue_id = self.db_service.get_venue(venue_name)
            if venue_id:
                print('\t\t\t %s already exists in the db (%s)' % (venue_name, db_venue_names))
            elif venue_name in venues_to_add.keys():
                print(f'\t\t\t{venue_name} already queued to be saved.')
            else:
                venues_to_add[venue_name] = venue_data
                print('\t\t\t %s queued to save.' % venue_name)
        return [venues_to_add[venue_name] for venue_name in venues_to_add.keys()]

        # print(json.dumps(venues_to_add, sort_keys=True, indent=4))
                        
        #     # TODO: Log the event and that ticket link is needed

        #     # Create model obj for event
        #     sk_event_model_to_save = {
        #         'artists': sk_event['performance'],
        #         'event_date': sk_event['start']['date'],
        #         'event_name': sk_event['displayName'],
        #         'event_start_at': sk_event['start']['time'],
        #         'event_type': 'Concert',
        #         'tickets_link': sk_event['uri'],
        #     }
        #     if sk_event_venue_name in venues_to_save_names:
        #         sk_event_model_to_save['venue_name'] = sk_event_venue_name
        #     elif sk_event_venue_name in db_venue_names:
        #         sk_event_model_to_save['venue_id'] = [
        #             venue for venue in self.venues if sk_event_venue_name == self.venues[venue]][0]
        #     events_to_save.append(sk_event_model_to_save)

        # print(f'\nEvents to save ({len(events_to_save)}): {json.dumps(events_to_save, indent=4, sort_keys=True)}')
        # final_event_models_to_save = []
        # if len(events_to_save) > 0:
        #     for event_model in events_to_save:
        #         if 'venue_name' in event_model.keys():
        #             try:
        #                 venue_id = Venues.select().where(Venues.venue_name == event_model['venue_name'])
        #                 event_model['venue_id'] = venue_id
        #                 del venue_model['venue_name']
        #             except:
        #                 print(f'Error finding venue_id for event: {event_model.event_name}')
        #         final_event_models_to_save.append(event_model)
            
        #     try:
        #         Events.insert_many(
        #             final_event_models_to_save,
        #             fields=[
        #                 'event_date',
        #                 'event_name',
        #                 'event_start_at',
        #                 'event_type',
        #                 'tickets_link',
        #                 'venue_id',
        #             ]
        #         ).execute()
        #         print(f'{len(final_event_models_to_save)} event models successfully saved!')    
        #     except:
        #         print('Error occurred while inserting event models')

        # self.saved_events = final_event_models_to_save   

    async def remove_events_existing_in_db(self, metropolitan_name, songkick_events):
        db_metro_events = self.db_service.get_metropolitan_events(metropolitan_name)
        sk_events_for_db = list()

        # loop through all songkick fetched events
        logging.debug(f'\tloopinig through sk events: {len(songkick_events)}')
        for sk_index, sk_event in enumerate(songkick_events):
            sk_event = songkick_events[sk_index]
            sk_event_date = sk_event['start']['date']
            sk_event_name = sk_event['displayName'].split('(')[0]
            sk_event_venue_name = sk_event['venue']['displayName']
            should_add_event = True
            logging.debug(f'\n\t\tSongkick Event Being Checked: {sk_event_name} / sk_index - {sk_index}')

            # loop through all db events
            logging.debug(f'\t\t\tlooping through all db events: {len(db_metro_events)}')
            for db_index, db_event in enumerate(db_metro_events):
                db_event_date = str(db_event['event_date'])[:10]
                db_event_name = str(db_event['event_name'])
                db_event_venue_name = self.db_service.get_venue_name(db_event['venue_id'])
                logging.debug(f'\t\t\t\texisting event: {db_event_name} / db_index: {db_index}')

        
                logging.debug(f'\t\t\t\t\t{db_event_date}')
                logging.debug(f'\t\t\t\t\t{db_event_name}')
                logging.debug(f'\t\t\t\t\t{db_event_venue_name}')

                # print(f'FILTERED EVENTS: {len(sk_events_for_db)}')

                # Compare venue name, date, artist name
                equal_event_name = sk_event_name == db_event_name
                equal_date = sk_event_date == db_event_date
                equal_venue = sk_event_venue_name == db_event_venue_name
                if  equal_date and equal_venue:
                    print(f'\t\t\tevent exists: {db_event["event_name"]} will not be added.')
                    should_add_event = False
                    break

            if should_add_event: 
                print(f'\t\t\tevent does not exist: {sk_event["displayName"]}, preparing to add')
                sk_events_for_db.append(sk_event)
            continue 

        print(f'\nnumber of {metropolitan_name} events start: {len(songkick_events)}')
        # Used to remove duplicate dictitonaries.
        set_of_sk_events_for_db = {json.dumps(dictionary, sort_keys=True) for dictionary in sk_events_for_db}
        filtered_sk_events_for_db = [json.loads(dictionary) for dictionary in set_of_sk_events_for_db]   
        return filtered_sk_events_for_db
        print(f'number of {metropolitan_name} events end: {len(filtered_sk_events_for_db)}')

        pg_db.close()