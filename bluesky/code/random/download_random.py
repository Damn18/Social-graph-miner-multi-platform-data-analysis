#!/usr/bin/env python3


from __future__ import annotations

import base64
import datetime as dt
import json
import os
import random
import time
from pathlib import Path
from typing import List
import requests
import nltk
from nltk.corpus import stopwords


start = "2025-03-09"   # inclusive
end = "2025-03-11"   # same
out_dir = Path("./bluesky/dataset/random")
filtering_word = "climatechange"
filtering_lang = "en"
num_hours      = 10  
limit_per_call = 100
max_retries    = 3
token_margin   = 300 # n. seconds before trying refresh/create token             

# stopwords for random search
nltk.download("stopwords", quiet=True)
STOPWORDS = stopwords.words("english")

# load environment
def _load_dotenv(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip() and not line.start):
                    k, v = line.strip().split("=" os.environ.setdefault(k, v)
    except FileNotFoundError:
        pass

import datetime as dt

# create months list between two dates
def _months_between(startd, endd):

    cur = dt.datetime.strptime(startd, "%Y-%m-%d").replace(day=1)
    last = dt.datetime.strptime(endd, "%Y-%m-%d").replace(day=1)
    months = []
    
    while cur <= last:
        months.append(cur.strftime("%Y-%m"))
        # move to next month
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)
    
    return months

# format random datetime for bluesky
def _iso(dt: dt.datetime):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# token expiry management
def _jwt_exp(jwt):
    """ Decompose the JWT string to extract the specific expiration datetime """
    try:
        header, payload_b64, _sig = jwt.split(".")
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return float(payload["exp"])
    except Exception:
        return time.time() + 7200

# token manager class
class TokenManager:
    def __init__(self, user,pw):
        """" normal init + call to create to obtain token"""
        self.user, self.pw = user, pw
        self.access = None
        self.refresh = None
        self.exp: float = 0.0
        self._create()

    @property
    def headers(self) :
        """ if fewer than TOKEN MARGIN seconds to expiry, then refresh """
        if time.time() > self.exp - token_margin:
            self._refresh()
        return {"Authorization": f"Bearer {self.access}"}

    def _create(self):
        """ normal call and compute expiry time """
        r = requests.post(
            "https://bsky.social/xrpc/com.atproto.server.createSession",
            json={"identifier": self.user, "password": self.pw},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        self.access, self.refresh = data["accessJwt"], data["refreshJwt"]
        self.exp = _jwt_exp(self.access)

    def _refresh(self):
        """ refresh call + compute expiry time """
        if not self.refresh:
            self._create(); return
        r = requests.post(
            "https://bsky.social/xrpc/com.atproto.server.refreshSession",
            headers={"Authorization": f"Bearer {self.refresh}"},
            timeout=15,
        )
        if r.status_code == 401:
            self._create(); return
        r.raise_for_status()
        data = r.json()
        self.access = data["accessJwt"]
        self.refresh = data.get("refreshJwt", self.refresh)
        self.exp = _jwt_exp(self.access)

def _rate_limit_sleep(resp):
    """ if 429 or 403 errors due to exceeding limits, compute wait time
    before making new calls from header Retry-After or ratelimit-reset"""

    # from header take Retry-After - value in seconds
    retry = resp.headers.get("Retry-After")
    if retry and retry.isdigit():
        # convert retry to integer
        wait = int(retry)
    else:
        # save reset unix value indicating when the rate-limit bucket refills
        reset = resp.headers.get("ratelimit-reset")
        if reset and reset.isdigit():
            # if more than zero seconds remain to reset, take that value, otherwise force zero
            wait = max(0, int(reset) - int(time.time()))
        else:
            # edge case if headers fail/invalid values – force 5 seconds
            wait = 5  
    print(f"rate-limit: sleep {wait}s")
    time.sleep(wait)



def main():
    _load_dotenv()
    user, pw = os.getenv("BLUESKY_USER"), os.getenv("BLUESKY_PASS")
    if not user or not pw:
        print("no BLUESKY_USER and BLUESKY_PASS in env.")

    real_starte.fromisoformat(startl_end   = dt.date.fromisoformat(end)TokenManager(user, pw)
    out_dir.mkdir(parents=True, exist_ok=True)
    seen_uris = set()

    for ym in _months_between(start       # handle incomplete month coverage
        month_first = dt.date.from"{ym}-01")
        month_next  = month_first.replace(year=month_first.year + (month_first.month == 12),
                                         month=(month_first.month % 12) + 1)
        # real first day (not necessarily the 1st)
        first_day = real_starth_first.year == real_start month_first.month == real_startlse month_first
        last_day  = (real_endelta(days=1)) ixt.year == real_end.yth_next.month == real_end.month) else month_next

        out_file = out_dir / f"bluesky_{ym}.json"
        print(f"Sampling posts for {ym}")

        with out_file.open("w", encoding="utf-8") as fh:
            fh.write("[\n"); first_elem = True
            day = first_day
            while day < last_day:
                saved_today = 0
                for _ in range(num_hours): # number of hours
                    randdt = dt.datetime.combine(day, dt.time()) + dt.timedelta(
                        hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
                    hour_end = randdt.replace(minute=59, second=59)
                    query_word = random.choice(STOPWORDS)

                    for attempt in range(max_retries):
                        try:
                            r = requests.get(
                                "https://bsky.social/xrpc/app.bsky.feed.searchPosts",
                                headers=tm.headers,
                                params={
                                    "q": query_word,
                                    "lang": filtering_lang,
                                    "since": _iso(randdt),
                                    "until": _iso(hour_end),
                                    "limit": limit_per-call,
                                },
                                timeout=5,
                            )
                            r.raise_for_status(); break
                        
                        # BACK OFF FOR API LIMIT ERRORS or SERVER FAILURES
                        # automatic retry + exponential wait with fixed attempts

                        except requests.HTTPError as exc:
                            code = exc.response.status_code
                            if code in (429, 403):
                                _rate_limit_sleep(exc.response)
                                if attempt < max_retries - 1: 
                                    continue
                            elif 500 <= code < 600 and attempt < max_retries - 1:
                                time.sleep(2 ** attempt); 
                                continue
                            print(f"{code} {exc.response.reason}; skipping"); r = None; 
                            break
                        except requests.RequestException as exc:
                            print(f"{exc}; skipping"); r = None; 
                            break

                    if r is None:
                        continue

                    # post 
                    post = (r.json().get("posts") or [None])[0]
                    if not post:
                        continue

                    # check duplicates
                    uri = post.get("uri")
                    if not uri or uri in seen_uris:
                        continue
                    if not first_elem:
                        fh.write(",\n")
                    json.dump(post, fh, ensure_ascii=False)
                    first_elem, saved_today = False, saved_today + 1
                    seen_uris.add(uri)

                print(f"{saved_today} unique posts for day {day}")
                day += dt.timedelta(days=1)
            fh.write("\n]\n")
        print(f"Saved {out_file}\n")


if __name__ == "__main__":
    main()
