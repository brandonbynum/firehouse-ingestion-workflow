import aiohttp
import asyncio
import csv
from datetime import datetime
import json
import logging
import pandas as pd
import datetime
from utilities.pretty_print import pretty_print as pp
from showfeur_db import ShowfeurDB
from spotify.spotify_service import SpotifyService
import songkick_event_import

# TODO: Write script to pull down playlists
# tthen extractt artists from playlists
# filter for artists which do not already exist
# get genres for each artistt
# create models for artists to add
# create artist genres 
# save all models

def save_artists_to_csv(data):
    artist_names = set()
    for item in data["tracks"]["items"]:
        artists = [artist["name"] for artist in item["track"]["artists"]]
        artist_names.update(artists)
    artist_list = list(artist_names)
    # pp(list(artist_names), True)
    
    filename = f'artist_output_{datetime.date.today()}.csv'
    headers = ['artists']
    df = pd.DataFrame(artist_list, columns=headers)
    df.to_csv(filename, index=False)
    df_saved_file = pd.read_csv(filename)
    print(f'artists saved to {filename}')
    
def spotify_menu():
    auth_token = input("Enter auth token:\n")
    service = SpotifyService(auth_token)
    
    # TODO: Make a dummy call to check if auth token is valid, if not ask user to input again.
    while True:
        print("\nChoose one of the following actions by entering it's respective number.")
        print("\t1 - Get my playlists")
        print("\t0 - Exit to main menu")
        choice = int(input("\tInput: "))
        
        if choice == 1:
            while True:
                print("\nFetching playlists...")
                playlists = service.get_my_playlists()
                
                if not len(playlists) > 0:
                    print('No playlists found!')
                else:
                    pl_keys = playlists.keys()
                    pl_direct = {}
                    for index, key in enumerate(pl_keys):
                        pl_direct[f"{index}"] = key
                        print(f'\t{index}: {playlists[key]}')
                        
                    while True:
                        sel_pl = input("Select one of the above playlists to output artists: ") 
                        if (int(sel_pl) >= len(pl_direct)):
                            print('invalid input...')
                        else: 
                            response = service.get_playlist(pl_direct[sel_pl])
                            #TODO: Filter remixed songs and find way to select correct artist to
                            #      prevent non-house artists from being added
                            playlist_artists = save_artists_to_csv(response)
                            return False
        elif choice == 0:
            return False
    

async def main():
    logging.basicConfig(
        filename='Run.log', 
        level=logging.INFO, 
        format='%(asctime)s:: %(message)s'
    )
    print("Welcome to Showfeur's backend CLI.")
    
    while True:
        service_choice = int(input("\nPlease choose a service.\n\t1 - Songkick\n\t2 - Spotify\n\t0 - Exit\nInput: "))
        print('')
        if service_choice == 1:
            songkick_event_import.main()
            #print("Sorry, not developed yet!")
            
        elif service_choice == 2:
            spotify_menu()
        elif service_choice == 0:
            exit()
        else:
            print('choice not recognized!')
    
    


if __name__ == '__main__':
    asyncio.run(main())
