import asyncio
from main import _fetch_and_cache_stream

async def test():
    data = await _fetch_and_cache_stream("173", "http://localhost:8000")
    print("DATA:", data)

asyncio.run(test())
