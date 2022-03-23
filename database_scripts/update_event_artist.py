import aiohttp
import asyncio
from datetime import datetime, date
import json
from models import *
from peewee import *
from playhouse.shortcuts import model_to_dict, dict_to_model
from utilities.timer import Timer
from showfeur_db import ShowfeurDB

async def main():
    timer = Timer('')
    timer.begin()
    
    current_date = date.today()
    db_service = ShowfeurDB()
    events_query = Events.select().where((Events.event_date <= current_date))
    event_ids = [event.event_id for event in events_query]
    event_artist_query = (Event_Artist
                          .select(Event_Artist.event_id)
                          .where((Event_Artist.event_id
                                  .in_(event_ids))))
    active_events = [event for event in events_query]
    existing_event_artist_records = [event_artist.event_id for event_artist in event_artist_query]
    existing_event_artist_event_ids = [event.event_id for event in existing_event_artist_records]
    event_artist_pairs_to_create = []
    
    for event in active_events:
        if event.event_id not in existing_event_artist_event_ids:   
            event_name = event.event_name
            artist_name = None

            if 'with' in event_name:
                artist_name = event_name.split(' with')[0]
            elif 'and' in event_name:
                artist_name = event_name.split(' and')[0]
            else:
                artist_name = event_name.split(' at')[0]
                
            artist_query = Artists.select().where(Artists.artist_name == artist_name)
            artists = [model for model in artist_query]
            
            if len(artists) > 0:
                artist_id = [artist.artist_id for artist in artists][0]
                event_artist_pairs_to_create.append({ 
                    'event_id': event.event_id,
                    'artist_id': artist_id,
                    'is_headliner': True,
                })
                        
    if len(event_artist_pairs_to_create) > 1:
        db_service.save_event_artists(event_artist_pairs_to_create)
        
    timer.stop()
    print(timer.results())
    timer.reset()

if __name__ == '__main__':
    asyncio.run(main())
