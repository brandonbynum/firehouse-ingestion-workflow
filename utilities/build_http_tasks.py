import asyncio
import aiohttp

async def build_http_tasks(urls: set, headers = None, req_func = None):
        tasks = set()
        async with aiohttp.ClientSession(headers=headers) as session:
            for url in urls:
                tasks.add(req_func(session, url))
            return await asyncio.gather(*tasks, return_exceptions=True)
