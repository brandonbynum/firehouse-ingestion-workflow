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

    def get_req(self, url):
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 401:
                print(f"{r.json()['error']['message']}\n")
                exit()
            return r.json()
        except requests.exceptions.RequestException as e:
            print("error ocurred!")

    async def get_my_playlists(self):
        url = self.base_url + "me/playlists"
        response = self.get_req(url)
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
            http_tasks = await build_http_tasks(self.headers, self.get_req, urls[x])
            #pp(res, False)
            for response in http_tasks:
                data = []
                if response:
                    data = response['artists']['items'][0]
                
                if data:
                    name = data['name']
                    artist_genres[name] = data['genres']
                else:
                    artist_genres[name] = data    
        return artist_genres
