import aiohttp
import math
import asyncio
import time
from typing import Any, Dict, List, Union, Callable
import dotenv
import os

dotenv.load_dotenv()

SLEEP = 3
TRY_SLEEP = SLEEP
TRIES = 3

class Async:
    session: aiohttp.ClientSession
    apiKeys: List[str] = []
    keyCursor = 0
    keyCooldowns: Dict[str, float] = {}
    keyLock: asyncio.Lock = asyncio.Lock()

    def __init__(self):
        async def init():
            Async.session = aiohttp.ClientSession()
            keysRaw = os.environ.get("WYNNKEY", "")
            parsedKeys = [k.strip() for k in keysRaw.replace("\n", ",").split(",") if k.strip()]
            Async.apiKeys = parsedKeys
            Async.keyCooldowns = {k: 0.0 for k in parsedKeys}

        asyncio.get_event_loop().run_until_complete(init())

    def __del__(self):
        # need to instantiate this class just so I can use destructor on exit
        asyncio.get_event_loop().run_until_complete(Async.session.close())


    @staticmethod
    async def batched_get(uris: List[str], batch_size=30, search: Callable = None) -> List[aiohttp.ClientResponse]:
        results = []
        for i in range(len(uris)//batch_size):
            batch = uris[i:(i+1)*batch_size]
            batch_req = [Async.get(uri, search) for uri in batch]
            results += await asyncio.gather(*batch_req)

            print("sleeping")
            await asyncio.sleep(SLEEP)

        return results
    
    @staticmethod
    async def nextApiKey() -> str:
        if not Async.apiKeys:
            return ""

        while True:
            waitSeconds = 0.0
            now = time.monotonic()
            async with Async.keyLock:
                keyCount = len(Async.apiKeys)
                for _ in range(keyCount):
                    key = Async.apiKeys[Async.keyCursor]
                    Async.keyCursor = (Async.keyCursor + 1) % keyCount
                    if Async.keyCooldowns.get(key, 0.0) <= now:
                        return key

                earliestReady = min(Async.keyCooldowns.get(k, 0.0) for k in Async.apiKeys)
                waitSeconds = max(0.0, earliestReady - now)

            await asyncio.sleep(waitSeconds if waitSeconds > 0 else TRY_SLEEP)

    @staticmethod
    def readWaitSeconds(response: aiohttp.ClientResponse) -> float:
        resetHeader = response.headers.get("RateLimit-Reset")
        if resetHeader:
            try:
                return max(float(resetHeader), TRY_SLEEP)
            except Exception:
                pass

        retryHeader = response.headers.get("Retry-After")
        if retryHeader:
            try:
                return max(float(retryHeader), TRY_SLEEP)
            except Exception:
                pass

        return float(TRY_SLEEP)

    @staticmethod
    async def get(uri: str) -> Union[aiohttp.ClientResponse, None]:
        t = TRIES
        res = None
        while t:
            key = await Async.nextApiKey()
            bearerHeader = {"Authorization": f"Bearer {key}"} if key else {}
            try: 
                res = await Async.session.get(uri, headers=bearerHeader)

                if res.status == 429:
                    waitSeconds = Async.readWaitSeconds(res)
                    if key:
                        async with Async.keyLock:
                            Async.keyCooldowns[key] = time.monotonic() + waitSeconds
                    await asyncio.sleep(waitSeconds)
                    t -= 1
                    continue

                return await res.json()
            except Exception as e:
                if "v3/player" in uri: raise e # TODO: wait until wynnapi fixes unhelpful error 500 messages
                print(uri, "borked")
                await asyncio.sleep(TRY_SLEEP)
            t -= 1

    @staticmethod
    async def post(uri: str, param: Dict[Any, Any]) -> Union[aiohttp.ClientResponse, None]:
        t = TRIES
        res = None
        try: 
            res = await Async.session.post(uri, json=param)
            if not await res.text():  return
            return await res.json()
        except Exception as e:
            print(uri, "borked")
            print(e.with_traceback())
            await asyncio.sleep(TRY_SLEEP)
            
_ = Async()
