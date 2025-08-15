import aiohttp
import math
import asyncio
from typing import Any, Dict, List, Union, Callable
import dotenv
import os

dotenv.load_dotenv()

SLEEP = 3
TRY_SLEEP = SLEEP
TRIES = 3

class Async:
    session: aiohttp.ClientSession

    def __init__(self):
        async def init():
            Async.session = aiohttp.ClientSession()

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
    async def get(uri: str) -> Union[aiohttp.ClientResponse, None]:
        t = TRIES
        res = None
        bearer_header = { "Authorization": f"Bearer {os.environ.get('WYNNKEY')}" }
        while t:
            try: 
                res = await Async.session.get(uri, headers=bearer_header)
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
