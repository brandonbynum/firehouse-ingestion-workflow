from aiohttp import ClientSession
import sys


async def get_request(url, proxy=None):
    async with ClientSession(trust_env=True) as session:
        try:
            resp = await session.get(url, proxy=proxy)
            # Note that this may raise an exception for non-2xx responses
            # You can either handle that here, or pass the exception through
            return await resp.json()
        except Exception as err:
            print(f"Other error occurred: {err}")
            sys.exit()
