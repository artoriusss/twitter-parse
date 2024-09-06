#from .TwitterWrapper import TwitterWrapper
from TwitterWrapper import TwitterWrapper
from logger import get_logger
from parse import tw_main
import asyncio

logger = get_logger(__name__)

async def twitter_main():
    await tw_main()
    tw = TwitterWrapper()
    tw.process_raw()

if __name__ == '__main__':
    asyncio.run(twitter_main())