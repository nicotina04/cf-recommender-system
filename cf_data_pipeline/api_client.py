import requests
import os
import json
import time
from typing import Optional
from config import *


def get_json(url: str, api_timeout: float = 10) -> Optional[dict]:
    try:
        res = requests.get(url, timeout=api_timeout)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.Timeout:
        print(f'[TIMEOUT] {url}')
        return None
    except Exception as e:
        print(f'[API ERROR] {url} -> {e}')
        return None
    
def safe_get_json(url: str, api_timeout, max_retries=5):
    for i in range(max_retries):
        res = get_json(url, api_timeout=api_timeout)
        if res is None:
            continue
        if res.get('status') == 'OK':
            return res
        elif "Call limit exceeded" in res.get("comment", ""):
            time.sleep(SLEEP_TIME)
        else:
            return None
    return None

def get_contest_list(cache_path = RES_CACHE_DATA_DIR / f'contest_list.json') -> Optional[dict]:
    if os.path.isfile(cache_path) is True:
        with open(cache_path, encoding='utf-8') as f:
            return json.load(f)
    
    try:
        data = get_json('https://codeforces.com/api/contest.list?gym=false', 10)

        if data['status'] != 'OK':
            raise ValueError('API status is not OK')
        
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return data
    except requests.RequestException as e:
        print(f'Failed to call API {e}')
        return None
    except Exception as e:
        print(f'[ERROR]: {e}')
        return None
    
def get_cf_rated_list_json(cache_path=f'./{RES_CACHE_BASENAME}/user.ratedList.json') -> Optional[dict]:
    if os.path.isfile(cache_path):
        with open(cache_path, encoding='utf-8') as f:
            return json.load(f)
    
    req_url = 'https://codeforces.com/api/user.ratedList?activeOnly=false&includeRetired=true'
    try:
        data = get_json(req_url, 50)
        if data['status'] != 'OK':
            raise ValueError('API status is not OK')
        
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return data
    except Exception as e:
        print(f'[EXCEPTION] {e}')
        return None
    
def get_contest_standings(
    contest_id: int,
    from_index: Optional[int] = None,
    count: Optional[int] = None,
    handles: Optional[str] = None,
    room: Optional[int] = None,
    show_unofficial: bool = False,
    participant_types: Optional[str] = None,
    as_manager: bool = False,
    only_problems: bool = False,
) -> Optional[dict]:
    base_url = f'https://codeforces.com/api/contest.standings?contestId={contest_id}&asManager={"true" if as_manager else "false"}'
    
    if from_index is not None:
        base_url += f"&from={from_index}"
    if count is not None:
        base_url += f"&count={count}"
    if handles:
        base_url += f"&handles={handles}"
    if room is not None:
        base_url += f"&room={room}"
    if show_unofficial is not None:
        base_url += f"&showUnofficial={'true' if show_unofficial else 'false'}"
    if participant_types:
        base_url += f"&participantTypes={participant_types}"

    wait_time = 10
    if only_problems:
        if from_index is None:
            base_url += "&from=1"
        if count is None:
            base_url += "&count=1"
    else:
        wait_time += 10

    return safe_get_json(base_url, api_timeout=wait_time)

def get_rated_users_by_contest(contest_id: int) -> Optional[dict]:
    url = f'https://codeforces.com/api/user.ratedList?activeOnly=false&includeRetired=true&contestId={contest_id}'
    return safe_get_json(url, api_timeout=90)

def get_user_rating_changes(handle: str) -> Optional[dict]:
    url = f'https://codeforces.com/api/user.rating?handle={handle}'
    return safe_get_json(url, api_timeout=10)

def get_contest_rating_changes(contest_id: int) -> Optional[dict]:
    url = f'https://codeforces.com/api/contest.ratingChanges?contestId={contest_id}'
    return safe_get_json(url, api_timeout=30)

def get_user_status(handle: str) -> Optional[dict]:
    url = f'https://codeforces.com/api/user.status?handle={handle}'
    return safe_get_json(url, api_timeout=17)