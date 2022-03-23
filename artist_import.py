import aiohttp
import asyncio
from datetime import datetime
import json
from utilities.timer import Timer
from utilities.pretty_print import pretty_print as pp
import logging
import os
import pandas as pd

from utilities.pretty_print import pretty_print
from spotify.spotify_service import SpotifyService
from songkick_event_service import SongkickEventService
from showfeur_db import ShowfeurDB


def create_artist_model(artist_name):
    return {
        'artist_name': artist_name
    }

async def main():
    logging.basicConfig(filename='Run.log', level=logging.INFO,
                        format='%(asctime)s:: %(message)s')

    auth_token = 'BQBe3kO4Skydld76SUYt9BKVNO7sjOpYypqOsez7oJUUOh4BMVfmj_tWLfMf_KwPFdlZEMiRY8gbNz4RCKI1RdfdWOVuPxWGbgqltUr1HMmJGzQ9CUVqOLWwO6TNOi4AXbZtHC-YcuvYu4ExuvNIM6F_02fo4b-2-N7gzRX_yX287KmjZ42czw'
    db_service = ShowfeurDB()
    spotify_service = SpotifyService(auth_token)
    root = 'spotify/artist_exports'
    files = os.listdir(root)
    
    
    
    choice = None
    while True:
        for index, file in enumerate(files):
            print(f'\n{index}: {file}')
        
        try:
            choice = int(input('\nEnter number of file to import: '))
            possible_choices = [index for index in range(len(files))]
            
            if choice not in possible_choices:
                print('Invalid input, please try agian...')
            else:
                break
        except ValueError:
            print('Invalid input, please try agian...')
    
    path = root + '/' + files[choice]
    df = pd.read_csv(path)
    playlist_artist_names = set([artist for artist in df['artists']])
    existing_artist_query= db_service.get_matching_artists(playlist_artist_names)
    existing_artists_names = set([artist.artist_name for artist in existing_artist_query])
    print(f'EXISTING ARTIST NAMES: {sorted(existing_artists_names)}')
    artist_names_to_add = list(playlist_artist_names - existing_artists_names)
    print('Artist Import Length: {}'.format(len(playlist_artist_names)))
    print('Difference in Lengths: {}'.format(len(existing_artists_names)))
    print('Final Import Length: {}'.format(len(artist_names_to_add)))
    
    artist_genre_data = await spotify_service.get_artists_genres(artist_names_to_add)  
    
    valid_genres = {}
    genres_query = db_service.get_genres() 
    for genre in genres_query:
        valid_genres[genre.genre_name.lower()] = genre.genre_id
    
    artist_models = []
    artist_genre_model_dict = {}
    pp(artist_genre_data, True)
    
    for artist_name in artist_genre_data.keys():
        print(artist_name)
        should_add = False
        artist_genres = artist_genre_data[artist_name]
        artist_genre_models = []
        
        for genre in artist_genres:
            if genre and genre in valid_genres.keys():
                print(f'{genre} is valid')
                should_add = True
                artist_genre_models.append({
                    'artist_id': None,
                    'genre_id': valid_genres[genre]
                })
            else:
                print(f'{genre} not valid')
        
        if should_add:
            print(f'To add {artist_name}\n')
            artist_model = create_artist_model(artist_name)
            artist_models.append(artist_model)
            artist_genre_model_dict[artist_name] = artist_genre_models
        else:
            print(f'To NOT add {artist_name}\n')
    
    pp(artist_models, True)
    pp(artist_genre_model_dict, True)
    
    db_service.save_artists(artist_models)
    
    # # TODO: Pull artist just saved and extract ID
    saved_artists_names = [artist['artist_name'] for artist in artist_models]
    saved_artists_query = db_service.get_matching_artists(saved_artists_names)
    print(f'\n{saved_artists_names}\n')
    
    for artist_model in saved_artists_query:
        artist_name = artist_model.artist_name
        for artist_genre_model in artist_genre_model_dict[artist_name]:
            artist_genre_model['artist_id'] = artist_model.artist_id
    pp(artist_genre_model_dict, True)
    
    artist_genre_model_list = []
    for artist in artist_genre_model_dict.keys():
        artist_genre_model_list += artist_genre_model_dict[artist]
    for model in artist_genre_model_list:
        print(model)
    print(type(artist_genre_model_list))

    db_service.save_artist_genres(artist_genre_model_list)
if __name__ == '__main__':
    asyncio.run(main())
