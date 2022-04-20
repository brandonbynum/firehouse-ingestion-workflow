import asyncio
from datetime import date
from utilities.timer import Timer
import logging

from scripts.event_ingestion_1 import EventIngestionService1


async def main():
    # logging.basicConfig(
    #     filename=f"event_import_{date.isoformat(date.today())}.log",
    #     level=logging.INFO,
    #     format="%(asctime)s:: %(message)s",
    # )
    timer = Timer("Event Data Ingestion")
    timer.begin()

    event_service = EventIngestionService1()
    await event_service.main()

    timer.stop()
    print(timer.results())
    timer.reset()


if __name__ == "__main__":
    asyncio.run(main())
