import aiohttp
import sys


async def get_request(url):
    async with aiohttp.ClientSession() as session:
        try:
            resp = await session.request("GET", url)
            # Note that this may raise an exception for non-2xx responses
            # You can either handle that here, or pass the exception through
            return await resp.json()
        except Exception as err:
            print(f"Other error occurred: {err}")
            sys.exit()
