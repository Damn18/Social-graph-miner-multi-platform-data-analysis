import json
import os
from mastodon import Mastodon
from datetime import datetime, timedelta, timezone
import time
import random
from dotenv import load_dotenv

### CONFIG 
load_dotenv()
out_dir  = '/home/damn/Documents/PROJECTS/THESIS/Social-graph-miner-multi-platform-data-analysis/mastodon/dataset/100_posts'

instance= 'https://mastodon.social'
hashtag = 'climatechange'
start= datetime(2024, 2, 6, tzinfo=timezone.utc)
end= datetime(2025, 6, 30, tzinfo=timezone.utc)
access_token = os.getenv('MASTODON_TOKEN')


### DIRECTORY 
os.makedirs(out_dir, exist_ok=True)
mastodon = Mastodon(access_token=access_token, api_base_url=instance)
seen_ids = set()

### EXTRACTION  
current_day = start
while current_day < end:
    posts_saved = 0
    month_file_path = os.path.join(out_dir, f"{current_day.year}-{current_day.month:02d}.json")

    with open(month_file_path, 'a', encoding='utf-8') as fout:
        while posts_saved < 5:
            rand_hour = random.randint(0, 23)
            rand_minute = random.randint(0, 59)
            rand_second = random.randint(0, 59)
            random_dt = current_day.replace(hour=rand_hour, minute=rand_minute, second=rand_second)
            
            posts = mastodon.timeline_hashtag(
                hashtag=hashtag,
                max_id=random_dt,
                limit=1
            )
            time.sleep(2)  
            if posts:
                s = posts[0]
                if current_day <= s.created_at < (current_day + timedelta(days=1)) and s.id not in seen_ids:
                    
                    fout.write(json.dumps(posts, ensure_ascii=False, default=str) + "\n")
                    seen_ids.add(s.id)
                    posts_saved += 1
    current_day += timedelta(days=1)

print(f"Salvato tutto")
