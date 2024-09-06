import json, asyncio, os
from tqdm import tqdm
#from .helpers import *
from helpers import *

class TwitterWrapper():
    def __init__(self):
        self.img_dir = 'data/twitter/images'
        self.raw_path = 'data/twitter/raw-unfiltered'
        self.prc_dir = 'data/twitter/processed'
        self.accs_dir = 'data/twitter/accounts'
        self._create_dirs()

    def _create_dirs(self):
        os.makedirs(self.img_dir, exist_ok=True)
        os.makedirs(self.raw_path, exist_ok=True)
        os.makedirs(self.prc_dir, exist_ok=True)
        os.makedirs(self.accs_dir, exist_ok=True)

    def _read_data(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data

    def _save_data(self, ad_details, data_dir):
        with open(data_dir, 'w', encoding='utf-8') as f:
            json.dump(ad_details, f, ensure_ascii=False, indent=4)

    def get_images(self):
        tweets = self._read_data(self.raw_path)
        asyncio.run(extract_images(self.img_dir, tweets))

    def process_raw(self):
        files = os.listdir(self.raw_path)

        for f in tqdm(files, desc='Processing files'):
            if not f.endswith('.json'):
                continue
            fname = f.split('.')[0]
            path = os.path.join(self.raw_path, f)
            tweets = self._read_data(path)

            if len(tweets) == 0:
                continue
            
            user_data = process_user_info(tweets[0]['user'])
            path = os.path.join(self.accs_dir, f'{fname}.json')
            self._save_data(user_data, path)

            processed_tweets = []

            for tweet in tqdm(tweets, desc='Processing tweets'):
                processed_tweet = process_tweet(tweet)
                processed_tweets.append(processed_tweet)

            path = os.path.join(self.prc_dir, f'{fname}.json')
            self._save_data(processed_tweets, path)