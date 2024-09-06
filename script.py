#from .TwitterWrapper import TwitterWrapper
from src.twitter.TwitterWrapper import TwitterWrapper
from config.logger import get_logger
from .parse import tw_main

logger = get_logger(__name__)

async def twitter_main():
    await tw_main()
    tw = TwitterWrapper()
    tw.process_raw()

if __name__ == '__main__':
    asyncio.run(twitter_main())