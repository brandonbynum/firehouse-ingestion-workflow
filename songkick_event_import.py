from sqlite3 import Date
import aiohttp
import asyncio
from datetime import datetime
import json
from utilities.timer import Timer
import logging

from utilities.pretty_print import pretty_print
from songkick_event_service import SongkickEventService
from showfeur_db import ShowfeurDB


async def main():
    timer = Timer('ENTIRE SERVICE')
    timer.begin()

    sk_service = SongkickEventService()
    await sk_service.main()

    timer.stop()
    logging.info(timer.results())
    timer.reset()

if __name__ == '__main__':
    asyncio.run(main())
