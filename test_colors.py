import asyncio
import httpx
from main import _sony_headers, PLAYBACK_UA

async def test():
    headers = _sony_headers()
    async with httpx.AsyncClient() as client:
        r = await client.post("https://jiotvapi.media.jio.com/playback/apis/v1/geturl?langId=6", data={"stream_type": "Live", "channel_id": "144"}, headers=headers)
        u = r.json().get("result")
        if u:
            h = await client.head(u, headers={"user-agent": PLAYBACK_UA})
            print("Colors HD HLS:", h.status_code)
        else:
            print("No HLS URL")

asyncio.run(test())
