import asyncio
import httpx
import json
import base64

with open("session.json", "r") as f:
    SESSION = json.load(f)

async def test_geturl(os_name, platform_name):
    headers = {
        "appName": "RJIL_JioTV",
        "deviceId": SESSION.get("deviceid", ""),
        "devicetype": "phone",
        "os": os_name,
        "osversion": "9",
        "partner": "jiotvvod",
        "user-agent": "plaYtv/7.1.5 (Linux;Android 9) ExoPlayerLib/2.11.7",
        "usergroup": "tvYR7NSNn7rymo3F",
        "versioncode": "396",
        "platform": platform_name,
        "dm": "ZUK ZUK Z1",
        "authtoken": SESSION.get("authtoken", ""),
        "ssotoken": SESSION.get("ssotoken", ""),
        "userid": SESSION.get("userid", ""),
        "uniqueid": SESSION.get("uniqueid", ""),
        "crmid": SESSION.get("crmid", ""),
        "subscriberid": SESSION.get("subscriberid", ""),
        "Host": "jiotvapi.media.jio.com",
        "Appkey": "NzNiMDhlYzQyNjJm",
        "Languageid": "6",
        "Sid": "892898ba-f9de-4572-b6c2-e717b0ad",
        "Isott": "false",
        "Lbcookie": "1",
        "Accesstoken": SESSION.get("authtoken", ""),
        "analyticsId": SESSION.get("deviceid", ""),
    }
    
    async with httpx.AsyncClient(verify=False) as client:
        resp = await client.post(
            "https://jiotvapi.media.jio.com/playback/apis/v1/geturl?langId=6",
            data={"stream_type": "Live", "channel_id": "443"},
            headers=headers
        )
        print(f"[{os_name}/{platform_name}] Response:", resp.json())

async def main():
    await test_geturl("android", "ANDROID_PHONE")
    await test_geturl("ios", "APPLE_PHONE")
    await test_geturl("android", "ANDROID_TV")
    await test_geturl("web", "WEB")

asyncio.run(main())
