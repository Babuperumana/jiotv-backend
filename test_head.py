import asyncio
import httpx
from main import _sony_headers, PLAYBACK_UA

async def test():
    headers = _sony_headers()
    async with httpx.AsyncClient() as client:
        # Get Aaj Tak
        r1 = await client.post("https://jiotvapi.media.jio.com/playback/apis/v1/geturl?langId=6", data={"stream_type": "Live", "channel_id": "173"}, headers=headers)
        u1 = r1.json().get("result")
        h1 = await client.head(u1, headers={"user-agent": PLAYBACK_UA})
        print("Aaj Tak HLS:", h1.status_code)
        
        # Get Asianet HD
        r2 = await client.post("https://jiotvapi.media.jio.com/playback/apis/v1/geturl?langId=6", data={"stream_type": "Live", "channel_id": "443"}, headers=headers)
        u2 = r2.json().get("result")
        h2 = await client.head(u2, headers={"user-agent": PLAYBACK_UA})
        print("Asianet HD HLS:", h2.status_code)

asyncio.run(test())
