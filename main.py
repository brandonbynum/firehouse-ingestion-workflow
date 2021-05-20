import aiohttp
import asyncio
from datetime import datetime
import json
from timer import Timer
import logging

from utilities.pretty_print import pretty_print
from songkick_event_service import SongkickEventService
from showfeur_db import ShowfeurDB

async def main():
    timer = Timer('ENTIRE SERVICE')
    timer.begin()
    logging.basicConfig(filename='Run.log', level=logging.INFO, format='%(asctime)s:: %(message)s')

    metro_area_name = 'Phoenix'
    sk_service = SongkickEventService()
    db_service = ShowfeurDB()

    logging.info('Step 1: Retrieving metro id for % s', metro_area_name)
    songkick_metroarea_id = await sk_service.get_sk_metroarea_id(metro_area_name)
    logging.info(f'\t{songkick_metroarea_id}')
    logging.info('')

    logging.info('Step 2: Retrieving events')
    songkick_metroarea_events = await sk_service.get_sk_metro_events(songkick_metroarea_id)
    #logging.info(json.dumps(songkick_metroarea_events[-1:], sort_keys=True, indent=2))
    logging.info(f'\t{len(songkick_metroarea_events)} events retrieved.')

    logging.info('Removing events with no artist data')
    events_with_artists = sk_service.filter_events_with_artist(songkick_metroarea_events)
    logging.info(f'\t# of events removed: {len(songkick_metroarea_events) - len(events_with_artists)}')

    logging.info("Removing events w/ artists that don't exist in our system...")
    electronic_events = await sk_service.filter_events_by_existing_artist(events_with_artists)

    #TODO: Remove events that contain 'CANCELLED' in title

    # TODO: Refactor below method to loop within method and use sets where possible for optimiization
    logging.info('Checking for events which already exist...')
    event_data_to_add = await sk_service.remove_events_existing_in_db(metro_area_name, electronic_events)

    if not len(event_data_to_add) > 0:
        logging.info('No events to add for metroplitan area %s' % metro_area_name)
        exit()
    
    logging.info(f'City Data Preparation: {metro_area_name}')
    cites_to_add = sk_service.prepare_cities_to_add(metro_area_name, event_data_to_add)
    if len(cites_to_add) > 0:
        await db_service.save_cities(cites_to_add)
    else:
        logging.info('\tNo new cities to add.')

    logging.info(f'Venue Data Preparation: {metro_area_name}')
    venues_to_add = sk_service.prepare_venues_to_add(metro_area_name, event_data_to_add)
    if len(venues_to_add) > 0:
        db_service.save_venues(venues_to_add)
    else:
        logging.info('\tNo new venues to add.')

    logging.info('Event Data Preparation')
    prepped_event_models = sk_service.prepare_events_to_add(event_data_to_add)
    logging.info(prepped_event_models)
    if len(prepped_event_models) > 0:
        db_service.save_events(prepped_event_models)
        prepped_event_artist_models = sk_service.create_artist_event_relations(event_data_to_add)
        db_service.save_event_artists(prepped_event_artist_models)
    else:
        logging.info('No new events to saves')


    timer.stop()
    logging.info(timer.results())
    timer.reset()

if __name__ == '__main__':
    asyncio.run(main())
