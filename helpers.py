from logger import get_logger
import requests, os
from tqdm import tqdm
import re
from html import unescape
import aiohttp
import asyncio
import os
from tqdm.asyncio import tqdm
from logging import getLogger

logger = get_logger(__name__)
CONCURRENT_DOWNLOADS = 5

async def extract_image(session, dir_, id_, img_url, semaphore):
    async with semaphore:  
        try:
            async with session.get(img_url) as response:
                response.raise_for_status()
                content = await response.read()
                path = os.path.join(dir_, f'{id_}.jpg')
                with open(path, 'wb') as file:
                    file.write(content)
                return True
        except Exception as e:
            logger.error(f'Error downloading image: {e}')
            return None

async def extract_images(dir_, tweets):
    image_count = 0
    extracted_count = 0
    tasks = []

    semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)

    async with aiohttp.ClientSession() as session:
        for tweet in tweets:
            tweet_photos = tweet.get('media', {}).get('photos', [])
            if not tweet_photos:
                continue
            image_count += len(tweet_photos)
            for i, photo in enumerate(tweet_photos):
                tweet_id = tweet['id'] if len(tweet_photos) == 1 else f"{tweet['id']}_{i}"
                img_url = photo['url']
                task = extract_image(session, dir_, tweet_id, img_url, semaphore)
                tasks.append(task)

        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc='Downloading images'):
            extracted = await task
            extracted_count += 1 if extracted else 0

    logger.info(f"Extraction finished.")
    logger.info(f"Extracted {extracted_count} images out of {image_count} total images.")

def get_attached_links(tweet):
    links = tweet.get("links")
    if not links:
        return []
    return [link.get("url") for link in links]

def clean_text(text):
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = unescape(text)
    words = text.strip().split()
    return ' '.join(words) if len(words) > 25 else None

def process_user_info(user):
    processed_user = {
        "id": user['id'],
        "url": user['url'],
        "username": user['username'],
        "display_name": user['displayname'],
        "created_date": user['created'],
        "description": user['rawDescription'],
        "location": user['location'],
        "followers_count": user['followersCount'],
        "friends_count": user['friendsCount'],
        "statuses_count": user['statusesCount'],
        "favourites_count": user['favouritesCount'],
        "listed_count": user['listedCount'],
        "media_count": user['mediaCount'],
        "is_verified": user['verified'],
        "profile_image_url": user['profileImageUrl'],
        "profile_banner_url": user['profileBannerUrl'],
        "is_protected": user['protected'],
        "blue": user['blue'],
        "blue_type": user['blueType'],
        "description_links": user['descriptionLinks'],
    }
    return processed_user

def process_tweet(tweet):
    if tweet is not None and isinstance(tweet, dict):
        place = tweet.get("place", {})
        place_id = place.get("id") if isinstance(place, dict) else None
    else:
        place_id = None
    quoted_tweet = tweet.get("quotedTweet")
    in_reply_to_user = tweet.get("inReplyToUser")

    quoted_tweet_id = quoted_tweet.get("id") if quoted_tweet else None
    quoted_user_id = quoted_tweet.get("user", {}).get("id") if quoted_tweet else None
    in_reply_to_user_id = in_reply_to_user.get("id") if in_reply_to_user else None
    attached_links = get_attached_links(tweet)
    text_clean = clean_text(tweet.get("rawContent")) 

    processed_tweet = {
        "id": tweet['id'],
        "url": tweet['url'],
        "created_date": tweet['date'],
        "author_id": tweet.get("user", {}).get("id"),
        "author_username": tweet.get("user", {}).get("username"),
        "author_created_date": tweet.get("user", {}).get("created"),
        "is_author_verified": tweet.get("user", {}).get("verified"), #
        "text_raw": tweet.get("rawContent"),
        "text_clean": text_clean,
        "attached_links": attached_links,
        "like_count": tweet.get("likeCount"),
        "quote_count": tweet.get("quoteCount"),
        "reply_count": tweet.get("replyCount"),
        "retweet_count": tweet.get("retweetCount"),
        "view_count": tweet.get("viewCount"),
        "quoted_tweet_id": quoted_tweet_id,
        "quoted_user_id": quoted_user_id,
        "in_reply_to_tweet_id": tweet.get("inReplyToTweetId"),
        "in_reply_to_user_id": in_reply_to_user_id,
        "source_label": tweet.get("sourceLabel"),
        "lang": tweet.get("lang"),
        "hashtags": tweet.get("hashtags"),
        "place_id": place_id,
    }
    return processed_tweet