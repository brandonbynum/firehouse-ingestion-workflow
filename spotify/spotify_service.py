import requests
from requests.auth import HTTPBasicAuth
class SpotifyService():
    def __init__(self, auth_token):
        self.auth_token = auth_token
        self.base_url = 'https://api.spotify.com/v1/'
        self.headers = {'Authorization': 'Bearer ' + auth_token}

    
    def get_req(self, url):
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 401:
                print(f"{r.json()['error']['message']}\n")
                exit()
            return r.json()
        except requests.exceptions.RequestException as e:
            print("error ocurred!")

    def get_my_playlists(self):
        endpoint = "me/playlists"
        url = self.base_url + endpoint
        response = self.get_req(url)
        playlists = {}

        for playlist in response['items']:
            id = playlist["id"]
            name = playlist["name"]
            playlists[id] = name
            
        return playlists
    
    def get_playlist(self, id):
        print('Retrieving playlist data...')
        endpoint = f"playlists/{id}"
        url = self.base_url + endpoint
        return self.get_req(url)
               

