import time
import logging
import openpyxl
import aiohttp
import asyncio
import requests
import os
import uuid
import botocore
import operator
import urllib.parse

from urllib.parse import quote, quote_plus

from src.config.config import baseUrl,  message_str
from src.utils.api.apis import import_rows_api
from src.utils.utils import tab_log, add_row_to_excel, timestamp_to_date_string, read_excel_to_array, group_by_column, \
    get_value_from_query, init_chunks, update_progress_input_file, url_encode_string, remove_illegal_characters, \
    extract_result_from_excel_formula, set_file_path
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

tab="keyword-search-trackers"

def data_failed_default(keyword):
    return {
        'keyword': keyword,
        "keyword_search": keyword,
        "search_volume": "Cannot find keyword",
    }

def get_cpc(price, country):
    if country == "vn":
        return price
    else:
        return price / 690

def parse_data(data, platform,input_file_id):
    print("data",data)
    results = []
    for item in data:
        results.append({
            **item,
            'platform': platform,
            'input_file_id':input_file_id
        })
    return results


def valid_data(data, platform):
    try:
        if platform == "lazada":
            if isinstance(data, dict):
                return data.get("result", {}).get("reportItem", []) != []
            else:
                return False  # Hoặc bạn có thể xử lý data ở đây tùy ý

        return data

    except Except as e:
        print(e)
        return False

def keyword_search_tracker_fetch_api(keyword,query,input_file_id):
    country = get_value_from_query(query,'country')
    platform = get_value_from_query(query,'platform')
    keyword = extract_result_from_excel_formula(keyword)

    if keyword:
        api_url = baseUrl + f"crawl/{platform}/{country}/search-keyword-volume?keyword={url_encode_string(keyword)}"
        max_retry = 3  # Số lần tối đa gọi lại API khi thất bại
        retry_count = 0 # Số lần đã gọi lại API khi thất bại

        while retry_count < max_retry:
            response = requests.get(api_url)
            print(api_url)
            if response.status_code == 200:
                res_data = response.json()
                data = res_data['data']
                valid = valid_data(data,platform)
                if valid:
                    return parse_data(data, platform, input_file_id)
                else:
                    print(tab_log(tab) + f": Retry to call api {tab}")
                    retry_count += 1
            else:
                print(tab_log(tab) + f": Retry to call api {tab}")
                retry_count += 1

    return data_failed_default(keyword)


def keyword_search_tracker_get_row(item, platform):
    keyword = item.get('keyword', '')
    search_volume = item.get('search_volume', '')

    if platform == "google":
        min_cpc = item.get('min_cpc', '')
        max_cpc = item.get('max_cpc', '')
        trend = item.get('trend', [])
        trend_str = ','.join(trend)

        return [keyword, search_volume, min_cpc, max_cpc,trend_str]
    else:
        recommend_price = item.get('recommend_price', '')
        return [keyword, search_volume, recommend_price]


async def keyword_search_tracker_crawl_data(rows, input_file_id,query):
    global tab
    # init variable
    max_workers = 1
    chunks = init_chunks(rows, max_workers)
    # try:
    # loop chunks to crawl data
    old_process = 0
    for index, chunk in enumerate(chunks):
        # try:
        results = keyword_search_tracker_process_chunk(chunk, query, input_file_id, max_workers)

        for result in results:
            print("result",result)
            import_rows_api(result, tab)

        # update process
        now_process = int(((index + 1) / len(chunks)) * 100)
        if now_process - old_process > 0:
            old_process = now_process
            update_data = await update_progress_input_file(
                {'id': input_file_id, 'progress': now_process, 'index_processed': index * max_workers})
            # handle stop
            if update_data.get('status', None) == "stop":
                logger.info(f"{tab_log(tab)}: Stop process")
                break
        # except Exception as e:
        #     logger.info(f"{tab_log(tab)}: chunk: {chunk} | error: {e}")

    # except Exception as e:
    #     # In case of any exception, save the workbook and close it before re-raising the exception
    #     raise e

    return tab


def keyword_search_tracker_process_chunk(chunk,query,input_file_id,max_workers=3):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # call api
        results = list(executor.map(lambda keyword: keyword_search_tracker_fetch_api(keyword,query,input_file_id), chunk))

    return results

def keyword_search_tracker_get_header_row(platform):
    if platform == "google":
        return ["Keyword",f"Search Volume - {platform}",f"Min CPC - {platform}", f"Max CPC - {platform}", "Trend (last 12 months)"]

    return ["Keyword",f"Search Volume - {platform}",f"Bid Price - {platform}"]


