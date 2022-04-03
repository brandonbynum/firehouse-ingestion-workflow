import asyncio
from datetime import date
from utilities.timer import Timer
import logging

from scripts.songkick.event_ingestion import EventIngestionService


async def main():
    logging.basicConfig(
        filename=f"../../logs/event_import/event_import_{date.isoformat(date.today())}.log",
        level=logging.INFO,
        format="%(asctime)s:: %(message)s",
    )
    timer = Timer("Songkick Data Ingestion")
    timer.begin()

    event_service = EventIngestionService()
    await event_service.main()

    timer.stop()
    logging.info(timer.results())
    timer.reset()


if __name__ == "__main__":
    asyncio.run(main())
