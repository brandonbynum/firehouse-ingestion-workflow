import aiohttp
import asyncio
import requests
from requests.auth import HTTPBasicAuth

from utilities.build_http_tasks import build_http_tasks
from utilities.pretty_print import pretty_print as pp

class SpotifyService():
    def __init__(self, auth_token):
        self.auth_token = auth_token
        self.base_url = 'https://api.spotify.com/v1/'
        self.headers = {'Authorization': 'Bearer {}'.format(auth_token)}

    async def get_req(self, session, url):
            try:
                async with session.get(url) as resp:
                    # Note that this may raise an exception for non-2xx responses
                    # You can either handle that here, or pass the exception through
                    # if resp.status_code == 401:
                    #     print('{}\n'.format({r.json()['error']['message']}))
                    return await resp.json()
            except Exception as err:
                print(f'Other error occurred: {err}')
                return err
            
    # def get_req(self, url):
    #     try:
    #         r = requests.get(url, headers=self.headers)
    #         if r.status_code == 401:
    #             print(f"{r.json()['error']['message']}\n")
    #             exit()
    #         return r.json()
    #     except requests.exceptions.RequestException as e:
    #         print("error ocurred!")

    async def get_my_playlists(self):
        endpoint = "me/playlists"
        url = self.base_url + endpoint
        response = await self.get_req(url)
        playlists = {}

        for playlist in response['items']:
            id = playlist["id"]
            name = playlist["name"]
            playlists[id] = name
            
        return playlists
    
    async def get_playlist(self, id):
        print('Retrieving playlist data...')
        endpoint = f"playlists/{id}"
        url = self.base_url + endpoint
        return await self.get_req(url)
    
    async def get_artists_genres(self, artist_list):
        endpoint = "search?type=artist&q="
        batch_size = 50
        batched_artists = [artist_list[index:index + batch_size] for index in range(0, len(artist_list), batch_size)]
        urls = {}
        
        print(len(batched_artists))
        for x, batch in enumerate(batched_artists):
            print(len(batch))
            urls[x] = []
            for y, artist in enumerate(batch):
                url = self.base_url + endpoint + artist.replace(' ', '+')
                urls[x].append(url)
                
        artist_genres = {}
        for x, batch in enumerate(urls):
            res = await build_http_tasks(self.headers, self.get_req, urls[0])
            #pp(res, False)
        
            for response in res:
                data = response['artists']['items'][0]
                artist_genres[data['name']] = data['genres']
            pp(artist_genres, False)
        # print(res)
        # print('Endpoint: {}'.format(urls[0]))
        #print('Response: {}'.format(pp(res, False)))
        
        #artist_list = [artist['name'] for artist in res['artists']['items']]
        #print(artist_list)

