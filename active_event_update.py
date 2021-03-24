import aiohttp
import asyncio
from datetime import datetime, date
import json
from models import *
from peewee import *
from playhouse.shortcuts import model_to_dict, dict_to_model
from timer import Timer

def date_str_to_date(date_str):
    date_str_split = [num_string for num_string in date_str.split('-')]
    day = int(date_str_split[0])
    month = int(date_str_split[1])
    year = int(date_str_split[2])
    return date(year, month, day)

def print_unserializable_dict(dict):
    json_data = json.dumps(
        dict,
        default=lambda o: '<not serializable>',
        sort_keys=False,
        indent=4,
    )
    print(json_data, '\n')

def print_model_set(model_query):    
    for event in model_query:
        event_as_dict = model_to_dict(event)
        print_unserializable_dict(event_as_dict)


def is_date_after_today(date_str='08-09-2021'):
    current_date = date.today()
    date_input = date_str_to_date(date_str)

    print(f'Current Date: {current_date}')
    print(f'Entered Date: {date_input}')
    print(date_input > current_date)

def update_is_active_field(model_query):
    for event in model_query:
        current_date = date.today()
        event_date = event.event_date
        if event_date > current_date:
            event.is_active = True
        else:
            event.is_active = False
        event.save()

async def main():
    timer = Timer('ENTIRE SERVICE')
    timer.begin()
    
    events_query = Events.select()
    update_is_active_field(events_query)

    timer.stop()
    timer.results()
    timer.reset()

if __name__ == '__main__':
    asyncio.run(main())
