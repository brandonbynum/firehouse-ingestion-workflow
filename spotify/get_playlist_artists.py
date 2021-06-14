from datetime import date
import json
import pandas as pd

path = 'responses/'
input_filename = 'spotify_playlist_resp.json'
data = None



with open(f'{path}{input_filename}') as f:
    data = json.load(f)
    f.close()

playlist_name = data['name'].replace(' ', '-')
output_filename = f'{date.today()}_{playlist_name}_artists.xlsx'

key = 'Artist Name'
artists = {key: set()}
for track in data['tracks']['items']:
    artists[key].add(track['track']['artists'][0]['name'])

sorted_artists = sorted(list(artists[key]))
artists = {key: sorted_artists}
[[key, value]] = artists.items()
print(artists)
# pd.DataFrame(artists).to_excel(output_filename, header=False, index=False)