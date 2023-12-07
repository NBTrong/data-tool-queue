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
import re

from urllib.parse import quote, quote_plus

from src.config.config import baseUrl,  message_str
from src.utils.api.apis import import_rows_api
from src.utils.utils import tab_log, add_row_to_excel, timestamp_to_date_string, read_excel_to_array, group_by_column, \
    group_by_field, update_progress_input_file, init_chunks, get_value_from_query, get_query_in_array

logger = logging.getLogger(__name__)

tab="keyword-labellings"

def get_default_failed(keyword):
    return {
        'keyword': keyword,
    }

async def keyword_labelling_crawl_data(rows, query, input_file_id):
    global tab
    max_retry = 3

    # init chunks
    chunks = init_chunks(rows, 50)
    # init model query
    id = get_value_from_query(query, 'id')
    api_url = baseUrl + f"suggest/confidence?id={id}"
    old_progress = 0

    # loop chunks to crawl data
    for index_chunk,chunk in enumerate(chunks):
        try:
            print(tab_log(tab), f": {index_chunk}/{len(chunks)}")
            url = api_url
            stop_count = 0
            while stop_count <= max_retry:
                response = requests.post(url + get_query_in_array(chunk))
                if response.status_code == 200:
                    response_data = response.json()
                    res_data = response_data['data']
                    if res_data:
                        import_rows_api(res_data,tab)
                    else:
                        stop_count += 1
                else:
                    stop_count += 1

            if stop_count > max_retry:
                for keyword in chunk:
                    sheet.append([get_default_failed(keyword)])

            now_progress = int((index_chunk / len(chunks)) * 100)
            if now_progress - old_progress > 0:
                update_data = await update_progress_input_file({'id': input_file_id, 'progress': now_progress})
                if update_data.get('status', None) == "stop":
                    logger.info(f"{tab_log(tab)}: Stop process")
                    break
                old_progress = now_progress

        except Exception as e:
            logger.info(f"{tab_log(tab)}: chunk: {chunk} | error: {e}")

    return tab

def keyword_labelling_get_row(keyword_data,keyword,properties):
    sub_categories_value = list(map(lambda property: keyword_data[0]['data'].get(property.upper(), ""), properties))
    max_confident_rate = keyword_data[0].get('max_confident_rate', 0)
    similar = ",".join([str(item.get('similar_keyword', None)) for item in keyword_data])

    return [keyword] + sub_categories_value + [max_confident_rate, similar]


def get_properties(query):
    pairs = query.split('&')

    # Khởi tạo mảng trống để lưu trữ các cột
    columns_array = []

    # Duyệt qua các cặp key-value
    for pair in pairs:
        key_value = pair.split('=')
        if key_value[0] == 'columns':
            columns_array.append(key_value[1])

    return columns_array