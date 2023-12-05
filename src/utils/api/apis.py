import requests
import aiohttp
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
utils_dir = os.path.abspath(os.path.join(current_dir, '..', '..','..'))
sys.path.append(utils_dir)

from src.config.config import baseUrl

def make_post_request(url, params, payload=[]):
    response = requests.post(url, json=payload,params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error:", response.text,params)
        return None

async def make_async_request(url, data):
    async with aiohttp.ClientSession() as session:
        async with session.put(url, json=data) as response:
            if response.status == 200:
                return await response.json()
            else:
                return False

def import_rows_api(payload, tab):
    url = baseUrl + f"queue/import"
    params = {
        "tab": tab
    }
    return make_post_request(url, params,payload)


def export_to_gcs_link_api(file_id,tab):
    print(f"Start export file: {file_id}, tab: {tab}")
    url = baseUrl + "queue/export"
    params = {
        "tab": tab,
        "file_id": file_id
    }
    return make_post_request(url, params)

async def update_progress_input_file_api(body):
    url = baseUrl + f"/input-file/{body['id']}/update-progress"
    return make_async_request(url + get_query(body), body)
