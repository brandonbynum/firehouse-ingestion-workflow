import aiohttp
import asyncio
from datetime import datetime, date
import json
from models import *
from peewee import *
from playhouse.shortcuts import model_to_dict, dict_to_model
from utilities.timer import Timer

async def main():
    timer = Timer('')
    timer.begin()
    
    current_date = date.today()

    # TODO : Call db service with get events query which takes where clause as parameter
    events_query = Events.select().where((Events.is_active == True) & (Events.event_date < current_date))

    events_to_update = [event.event_id for event in events_query]

    # TODO: Call db service update events
    Events.update(is_active = False).where(Events.event_id.in_(events_to_update)).execute()

    
    print(events_to_update)

    timer.stop()
    print(timer.results())
    timer.reset()

if __name__ == '__main__':
    asyncio.run(main())
