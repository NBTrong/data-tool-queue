import aiohttp
import asyncio
import time
import openpyxl
import os
import sys
import requests
from datetime import datetime
import datetime
from concurrent.futures import ThreadPoolExecutor
import re


current_dir = os.path.dirname(os.path.abspath(__file__))
utils_dir = os.path.abspath(os.path.join(current_dir, '..', '..','..'))
sys.path.append(utils_dir)

from src.utils.api.apis import export_to_gcs_link_api

from src.utils.utils import get_value_from_query, read_excel_to_array, update_progress_input_file, \
    get_query_in_array, group_by_field, remove_excel_file, download_and_read_excel_file, \
    init_chunks, \
    fetch_data, get_queue_of_tab, tab_log, array_find_field, get_platform, group_by_column, \
    add_row_to_excel, channel_handle_unique_koc_sheet_data, get_input_file_status, array_filter_field

from src.utils.gc_upload import upload_excel_to_gcs

from src.config.config import baseUrl, settings, message_str
import logging
from src.utils.api.keyword_explore import parse_data_kocs, youtube_keyword_explore_crawl_data, tiktok_keyword_explore_crawl_data
from src.utils.api.channel_explore import tiktok_channel_explore_crawl_data, \
    instagram_channel_explore_crawl_data, youtube_channel_explore_crawl_data
from src.utils.api.extract_data import extract_data_crawl_data
from src.utils.api.keyword_search_tracker import keyword_search_tracker_crawl_data
from src.utils.api.keyword_labelling import keyword_labelling_crawl_data
from src.utils.api.extract_comments import extract_comments_crawl_data

# Setup path to log file
current_time = datetime.datetime.now().strftime("%Y-%m-%d")
settings.setup_logging(f'/crawler/log/{current_time}-queue.log')

logger = logging.getLogger(__name__)


#---------------------------------------------Main function---------------------------------------------

def config_max_processing(tab):
    if tab == "keyword-labelling":
        return 2
    return 1

async def process_data(tab,data):
    if data and len(data) > 0:
        if array_find_field(data, 'status', 'processing'):
            print(tab_log(tab) + f": already have a queue running")
            return False
        else:
            first_object = data[0]
            # try:
            current_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(tab_log(tab) + f": Start crawling file name: " + first_object['name'])

            url = first_object.get("url")
            query = first_object['query']

            rows = await download_and_read_excel_file(url, tab)

            print(f"Row file: {rows}")
            print(f"{tab_log(tab)}: Read excel file successfully")

            await update_progress_input_file(
                {'id': first_object['id'], 'status': 'processing', 'start_time': current_datetime, 'progress': 0})

            tab = await crawl_data(tab, rows, query, first_object['id'], url)

            print("tab",tab)

            status_now = await get_input_file_status(first_object['id'])

            url = export_to_gcs_link_api(first_object['id'],tab)

            if status_now == "stop":
                await update_progress_input_file(
                    {'id': first_object['id'], 'status': 'stop'})
            else:
                await update_progress_input_file(
                    {'id': first_object['id'], 'status': 'finished','progress': 100})

            return url
            # except Exception as e:
            #     await update_progress_input_file({'id': first_object['id'], 'status': 'failed'})
            #     logger.info(message_str + f"Error {e}")
            #     return True

    else:
        print(tab_log(tab) + f": no queues found")
        return False


async def crawl_data(tab,rows,query,input_file_id,url):
    match tab:
        case "keyword-labelling":
            return await keyword_labelling_crawl_data(rows,query,input_file_id)
        case "tiktok-channel-explore":
            return await tiktok_channel_explore_crawl_data(rows, input_file_id,query,url)
        case "instagram-channel-explore":
            return await instagram_channel_explore_crawl_data(rows, input_file_id)
        case "youtube-channel-explore":
            return await youtube_channel_explore_crawl_data(rows, input_file_id)
        case "tiktok-keyword-explore":
            return await tiktok_keyword_explore_crawl_data(rows, input_file_id,query)
        case "youtube-keyword-explore":
            return await youtube_keyword_explore_crawl_data(rows, input_file_id)
        # case "instagram-keyword-explore":
        #     return await instagram_keyword_explore_crawl_data(rows, input_file_id)
        case "extract-data":
            return await extract_data_crawl_data(rows, input_file_id,query)
        case "videos-extract-data":
            return await extract_data_crawl_data(rows, input_file_id, query)
        case "comments-extract-data":
            return await extract_comments_crawl_data(rows, input_file_id, query)
        case "shopee-keyword-search-tracker":
            return await keyword_search_tracker_crawl_data(rows, input_file_id,query)
        case "lazada-keyword-search-tracker":
            return await keyword_search_tracker_crawl_data(rows, input_file_id, query)
        case "google-keyword-search-tracker":
            return await keyword_search_tracker_crawl_data(rows, input_file_id, query)

    return False
async def main():
    mess_start = "----------------------------------------------------Running xData Queue----------------------------------------------------"
    logger.info(mess_start)
    #get queue
    data = await get_queue_of_tab()
    if data and len(data):
        data_group = group_by_field(data,'tab')
        tabs = data_group.keys()

        for tab in tabs:
            valid = await process_data(tab, data_group[tab])
            if valid:
                break

    else:
        print(message_str+"No valid queue could be found")

if __name__ == "__main__":
    asyncio.run(main())
