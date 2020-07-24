import asyncio
import aiohttp
import math

class SongKickFilter():
    def __init__(self, metro_area_names):
        self.metro_area_ids = []
        self.metro_area_names = metro_area_names
        self.api_key = 'fUiSaa7nFB1tDdh7'

    async def get_request(self, url: str):
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.request('GET', url)
                # Note that this may raise an exception for non-2xx responses
                # You can either handle that here, or pass the exception through
                data = await resp.json()
                print(f"Received data for {url}")
            except Exception as err:
                print(f'Other error occurred: {err}')
                return err
            else:
                print('Success!')
                return data['resultsPage']

    async def get_metroarea_ids(self):
        if len(self.metro_area_names) > 1:
            tasks = []
            for name in self.metro_area_names:
                location_url = f'https://api.songkick.com/api/3.0/search/locations.json?query={name}&apikey={self.api_key}'
                tasks.append(self.get_request(location_url))

            all_location_data = await asyncio.gather(*tasks, return_exceptions=True)

            metro_area_ids = {}
            for location in all_location_data:
                metro_area_data = location['results']['location'][0]['metroArea']
                metro_area_id = metro_area_data['id']
                metro_area_name = metro_area_data['displayName']

                metro_area_ids[metro_area_name] = metro_area_id
            self.metro_area_ids = metro_area_ids
        else:
            data = await self.get_request(url=location_url)
            metro_area_id = data['results']['location'][0]['metroArea']['id']
            metro_area_name = metro_area_names[0]
            self.metro_area_ids[metro_area_name] = metro_area_id

    async def get_metro_area_events(self):
        # =============== METHODS ===============
        def has_artists(event):
            if len(event['performance']) > 0:
                return event

        def filter_events_without_artist(events):
            results = list(filter(has_artists, events))
            return results

        async def call_remaining_pages(num_of_pages, metro_id):
            tasks = []
            print('building urls\n')
            # Build Tasks // MAKE REUSABLE FUNCTION
            for currentPage in range(num_of_pages):
                currentPage += 1
                url = f'https://api.songkick.com/api/3.0/events.json?apikey={self.api_key}&location=sk:{metro_id}&page={currentPage}'
                print(f'page: {currentPage} data prepared\n')
                tasks.append(self.get_request(url))

            paged_data = await asyncio.gather(*tasks, return_exceptions=True)
            remaining_events = []
            for index, page_resp in enumerate(paged_data):
                page_resp_event_list = page_resp['results']['event']
                filtered_events = filter_events_without_artist(page_resp_event_list)
                remaining_events += filtered_events
            return remaining_events
        # =============== LOGIC ===============
        if len(self.metro_area_ids) > 1:
            tasks = []
            filtered_metro_events = {}
            # call event process for each city
            for metro_name in self.metro_area_ids:
                events_url = f'https://api.songkick.com/api/3.0/metro_areas/{self.metro_area_ids[metro_name]}/calendar.json?apikey={self.api_key}'
                metro_event_data = await self.get_request(events_url)
                page_one_events = metro_event_data['results']['event']
                filtered_metro_events[metro_name] = filter_events_without_artist(page_one_events)

                pages = math.ceil(metro_event_data['totalEntries'] / 50)
                if pages > 1:
                    remaining_page_events = await call_remaining_pages(num_of_pages=pages, metro_id=self.metro_area_ids[metro_name])
                    filtered_metro_events[metro_name] += remaining_page_events
        elif len(self.metro_area_ids) == 1:
            data = await self.get_request(url=events_url)
            filtered_metro_events[self.metro_area_ids.keys()[0]] = filter_events_without_artist(data)
        else:
            print('No metro ids provided!')

        return filtered_metro_events

async def main():
    metro_area_names = ['Phoenix', 'Tucson']
    instance = SongKickFilter(metro_area_names)

    await instance.get_metroarea_ids()
    metro_area_events = await instance.get_metro_area_events() 

    total_events = 0
    for metro_area_name in metro_area_events:
        total_events += len(metro_area_events[metro_area_name])
        print(f'{metro_area_name}: {len(metro_area_events[metro_area_name])}')
    print(f'total events: {total_events}')

main = main()
asyncio.run(main)

    
