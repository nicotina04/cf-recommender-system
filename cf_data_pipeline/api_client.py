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
    
def get_contest_standings(contest_id: int, only_problems: bool) -> Optional[dict]:
    url = f'https://codeforces.com/api/contest.standings?contestId={contest_id}&asManager=false'
    if only_problems:
        url += '&from=1&count=1'

    wait_time = 10
    if not only_problems:
        wait_time += 10
    return safe_get_json(url, api_timeout=wait_time)

def get_rated_users_by_contest(contest_id: int) -> Optional[dict]:
    url = f'https://codeforces.com/api/user.ratedList?activeOnly=false&includeRetired=true&contestId={contest_id}'
    return get_json(url, api_timeout=90)
