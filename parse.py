import asyncio, logging, json, random, sys, os
import twscrape
from tqdm.asyncio import tqdm
from .accounts import accs
from .config import account_ids, keywords
from datetime import datetime, timezone

sys.setrecursionlimit(10000) 

logging.getLogger("httpx").setLevel(logging.WARNING)

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

async def init_api():
    api = twscrape.API()

    for acc, creds in accs.items():
        await api.pool.add_account(acc, *creds)

    await api.pool.login_all()
    return api

async def worker(semaphore: asyncio.Semaphore, api: twscrape.API, q: str, keywords: bool = False):
    tweets = []
    async with semaphore:  
        try:
            if keywords:
                async for doc in api.search_tweets(q): # for search by keyword
                    tweets.append(doc.dict())
            else:
                async for doc in api.user_tweets(q): # for listing tweets by account id
                    tweets.append(doc.dict())

        except Exception as e:
            logging.error(f'Error in worker for query "{q}": {e}')

        finally:
            with open(f'data/twitter/raw-unfiltered/{q}.json', 'w') as f:
                json.dump(tweets, f, indent=4, ensure_ascii=False, default=json_serial)
    
    return tweets

async def tw_main():
    api = await init_api()
    queries = keywords
    semaphore = asyncio.Semaphore(2)  

    tasks = [worker(semaphore, api, q) for q in queries]

    for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        result = await future