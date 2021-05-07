import aiohttp
import asyncio
import json
from timer import Timer

from songkick_event_service import SongkickEventService

async def main():
    timer = Timer('ENTIRE SERVICE')
    timer.begin()

    metro_area_name = 'Phoenix'
    service = SongkickEventService()

    print('\nRetrieving metro id.')
    songkick_metroarea_id = await service.get_sk_metroarea_id(metro_area_name)
    print(f'\t{songkick_metroarea_id}')

    print('\nRetrieving events...')
    songkick_metroarea_events = await service.get_sk_metro_events(songkick_metroarea_id)
    #print(json.dumps(songkick_metroarea_events[-1:], sort_keys=True, indent=2))
    print(f'\t{len(songkick_metroarea_events)} events retrieved.')

    print('\nRemoving events with no artist data')
    events_with_artists = service.filter_events_with_artist(songkick_metroarea_events)
    print(f'\t# of events removed: {len(songkick_metroarea_events) - len(events_with_artists)}')

    print("\nRemoving events w/ artists that don't exist in our system...")
    electronic_events = await service.filter_events_by_existing_artist(events_with_artists)

    # TODO: Refactor below method to loop within method and use sets where possible for optimiization
    print('\nChecking for events which already exist...')
    events_to_add = await service.remove_events_existing_in_db(metro_area_name, electronic_events)

    # print('\n')
    # print(service.metro_name, 'RESULTS:')
    # for event in service.sk_events_for_db:
    #     print(event['displayName'])
    # print('\n')

    service.prepare_and_save_db()
    # service.create_artist_event_relations(service.saved_events)

    # timer.stop()
    # timer.results()
    # timer.reset()

if __name__ == '__main__':
    asyncio.run(main())