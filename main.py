import aiohttp
import asyncio
from datetime import datetime
import json
import logging

from utilities.pretty_print import pretty_print
from showfeur_db import ShowfeurDB



# TODO: Write script to pull down playlists
# tthen extractt artists from playlists
# filter for artists which do not already exist
# get genres for each artistt
# create models for artists to add
# create artist genres 
# save all models


async def main():
    logging.basicConfig(
        filename='Run.log', 
        level=logging.INFO, 
        format='%(asctime)s:: %(message)s'
    )

    



 

if __name__ == '__main__':
    asyncio.run(main())
