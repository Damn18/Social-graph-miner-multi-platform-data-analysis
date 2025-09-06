import json
import os
from mastodon import Mastodon
from datetime import datetime, timedelta, timezone
import time
import random
from bs4 import BeautifulSoup
from langdetect import detect
from dotenv import load_dotenv

### CONFIG 
out_dir  = '/home/damn/Documents/PROJECTS/THESIS/Social-graph-miner-multi-platform-data-analysis/mastodon/dataset/random'
load_dotenv()
instance = 'https://mastodon.social'
start = datetime(2024, 2, 6, tzinfo=timezone.utc)
end = datetime(2025, 7, 6, tzinfo=timezone.utc)
access_token = os.getenv('MASTODON_TOKEN')
 
### DIRECTORY 
os.makedirs(out_dir, exist_ok=True)
mastodon = Mastodon(access_token="access_token", api_base_url=instance)
seen_ids = set()

all_posts = []
### EXTRACTION  
current_day = start
while current_day < end:
    posts_saved = 0
    attempts = 0
    MAX_ATTEMPTS = 10   # Only 10 attempts per day
    month_file_path = os.path.join(out_dir, f"{current_day.year}-{current_day.month:02d}.json")

    with open(month_file_path, 'a', encoding='utf-8') as fout:
        while posts_saved < 5 and attempts < MAX_ATTEMPTS:
            rand_hour = random.randint(0, 23)
            rand_minute = random.randint(0, 59)
            rand_second = random.randint(0, 59)
            random_dt = current_day.replace(hour=rand_hour, minute=rand_minute, second=rand_second)
            
            posts = mastodon.timeline_public(
                max_id=random_dt,
                limit=3
            )
            #time.sleep(1)  
            attempts += 1
            if posts:
                s = posts[0]
                api_lang = getattr(s, "language", None)
                save_post = (api_lang == "en")
                if save_post and current_day <= s.created_at < (current_day + timedelta(days=1)) and s.id not in seen_ids:
                    all_posts.append(s)
                    seen_ids.add(s.id)
                    posts_saved += 1

    
    current_day += timedelta(days=1)

print(f"Salvato tutto")
