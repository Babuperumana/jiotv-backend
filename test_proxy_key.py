import asyncio
from main import _stream_cache, _get_cached_stream, _fetch_and_cache_stream

async def test():
    # first fetch to ensure it's in cache
    await _fetch_and_cache_stream("443", "http://localhost:8000")
    print("CACHE:", _stream_cache.get("443"))
    cached = _get_cached_stream("443")
    print("CACHED GET:", cached)

asyncio.run(test())
