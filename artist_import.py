import aiohttp
import asyncio
from datetime import datetime
import json
from utilities.timer import Timer
import logging
import os
import pandas as pd

from utilities.pretty_print import pretty_print
from spotify.spotify_service import SpotifyService
from songkick_event_service import SongkickEventService
from showfeur_db import ShowfeurDB


async def main():
    logging.basicConfig(filename='Run.log', level=logging.INFO,
                        format='%(asctime)s:: %(message)s')

    auth_token = 'BQAGw6Ev2z-W9VnhM3fM5qQ7kLBA1OO9n81Fy09SaRKvey-g2gbX_fMTZzbPX5HnNK4rL_UTMj5aOT_b_vsmD1ktaQCugUXkfmGMcNJSI0CTHmtrwSgVrT8rcALXoTHduQ-g_xI2kpaQivUkMF_WQLvqTQqr-2f4MRoes0yF8Py2xuyaxX2h6A'
    db_service = ShowfeurDB()
    spotify_service = SpotifyService(auth_token)
    root = 'spotify/artist_exports'
    files = os.listdir(root)
    
    for index, file in enumerate(files):
        print(f'{index}: {file}')
    choice = input('Enter number of file to import: ')
    
    path = root + '/' + files[int(choice)]
    df = pd.read_csv(path)
    artist_imports = set([artist for artist in df['artists']])
    existing_artist_query= db_service.get_matching_artists(artist_imports)
    existing_artists = set([artist.artist_name for artist in existing_artist_query])
    #TODO: Remove existing artists
    artists_to_add = list(artist_imports - existing_artists)
    print('Artist Import Length: {}'.format(len(artist_imports)))
    print('Difference in Lengths: {}'.format(len(existing_artists)))
    print('Final Import Length: {}'.format(len(artists_to_add)))
    
    res = await spotify_service.get_artists_genres(artists_to_add)
    

if __name__ == '__main__':
    asyncio.run(main())
