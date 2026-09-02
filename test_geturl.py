import asyncio
import httpx
from main import _sony_headers
async def test():
    headers = _sony_headers()
    async with httpx.AsyncClient() as client:
        resp = await client.post("https://jiotvapi.media.jio.com/playback/apis/v1/geturl?langId=6", data={"stream_type": "Live", "channel_id": "173"}, headers=headers)
        print(resp.json()["mpd"])
asyncio.run(test())
