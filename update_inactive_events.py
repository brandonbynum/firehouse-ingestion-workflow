import asyncio
from datetime import date
from models import *
from peewee import *
from utilities.timer import Timer

# TODO: Create log outputs of this process and email them
async def main():
    timer = Timer("")
    timer.begin()

    current_date = date.today()

    # TODO : Call db service with get events query which takes where clause as parameter
    events_query = Events.select().where((Events.is_active == True) & (Events.date < current_date))

    events_to_update = [event.id for event in events_query]

    # TODO: Call db service update events
    Events.update(is_active=False).where(Events.id.in_(events_to_update)).execute()

    print(events_to_update)

    timer.stop()
    print(timer.results())
    timer.reset()


if __name__ == "__main__":
    asyncio.run(main())
