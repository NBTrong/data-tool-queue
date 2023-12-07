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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import urllib.parse

from urllib.parse import quote, quote_plus

from src.config.config import baseUrl, message_str
from src.utils.api.apis import import_rows_api, export_to_gcs_link_api
from src.utils.time_helper import convert_timestamp_to_date_string
from src.utils.utils import tab_log, add_row_to_excel, timestamp_to_date_string, read_excel_to_array, group_by_column, \
    update_progress_input_file, init_chunks, get_post_id_by_platform, get_platform, get_value_from_query, set_file_path

logger = logging.getLogger(__name__)

tab = "extract-data"

def data_failed_default(url, platform, input_file_id):
    return {
        'post_url': url,
        'description': "This post could not be found",
        'input_file_id': input_file_id,
        'platform': platform
    }


def data_parse(data, platform, input_file_id, url):
    if platform == 'tiktok':
        data = data.get('aweme_detail', None)
        return {
            'input_file_id': input_file_id,
            'file_url': data.get('video', {}).get('play_addr', {}).get('url_list', [])[
                0] if data and 'video' in data else '',
            'uploaded_time': convert_timestamp_to_date_string(data['create_time']) if data else '',
            'koc_follower_count': 0,
            'total_saves': data['statistics']['collect_count'] if data and 'statistics' in data else 0,
            'total_comments': data['statistics']['comment_count'] if data and 'statistics' in data else 0,
            'total_shares': data['statistics']['share_count'] if data and 'statistics' in data else 0,
            'total_likes': data['statistics']['digg_count'] if data and 'statistics' in data else 0,
            'total_views': data['statistics']['play_count'] if data and 'statistics' in data else 0,
            'is_detect_voice': 0,

            'match_keywords': '',
            'transcript': '',
            'tags': '',
            'thumb_url': data['video']['ai_dynamic_cover']['url_list'][0] if data and 'video' in data else '',
            'description': data['desc'] if data else '',
            'platform': platform,

            'post_url': url
        }
    elif platform == 'youtube':
        return {

            'input_file_id': input_file_id,

            'uploaded_time': data['items'][0]['snippet']['publishedAt'] if 'items' in data and len(
                data['items']) > 0 else '',
            'koc_follower_count': 0,

            'total_comments': data['items'][0]['statistics']['commentCount'] if 'items' in data and len(
                data['items']) > 0 and 'statistics' in data['items'][0] and 'commentCount' in data['items'][0][
                                                                                    'statistics'] else 0,
            'total_shares': 0,
            'total_likes': data['items'][0]['statistics']['likeCount'] if 'items' in data and len(
                data['items']) > 0 and 'statistics' in data['items'][0] and 'likeCount' in data['items'][0][
                                                                              'statistics'] else 0,
            'total_views': data['items'][0]['statistics']['viewCount'] if 'items' in data and len(
                data['items']) > 0 and 'statistics' in data['items'][0] and 'viewCount' in data['items'][0][
                                                                              'statistics'] else 0,
            'is_detect_voice': 0,

            'match_keywords': '',
            'transcript': '',
            'tags': '',
            'thumb_url': data['items'][0]['snippet']['thumbnails']['default']['url'] if 'items' in data and len(
                data['items']) > 0 else '',
            'description': data['items'][0]['snippet']['title'] if 'items' in data and len(data['items']) > 0 else '',
            'platform': platform,

            'post_url': url
        }
    elif platform == 'instagram':
        post_image = ''
        if data and 'image_versions2' in data:
            post_image = data['image_versions2']['candidates'][-1]['url'] if 'candidates' in data[
                'image_versions2'] else ''
        elif data and 'carousel_media' in data and data['carousel_media'][0]['image_versions2']:
            post_image = data['carousel_media'][0]['image_versions2']['candidates'][-1]['url'] if 'candidates' in data[
                'carousel_media'][0]['image_versions2'] else ''

        return {
            'input_file_id': input_file_id,
            'uploaded_time': convert_timestamp_to_date_string(
                data['caption']['created_at']) if data and 'caption' in data else '',
            'koc_follower_count': 0,
            'total_comments': data['comment_count'] if 'comment_count' in data else 0,
            'total_shares': data['reshare_count'] if 'reshare_count' in data else 0,
            'total_likes': data['like_count'] if 'like_count' in data else 0,
            'total_views': data.get('play_count', 0),  # Fix for 'play_count' not being present
            'is_detect_voice': 0,

            'match_keywords': '',
            'transcript': '',
            'tags': '',
            'thumb_url': post_image,
            'description': data['caption']['text'] if data and 'caption' in data else '',
            'platform': platform,

            'post_url': url
        }


def valid_data(data, platform):
    if (platform == "youtube"):
        return data.get("items", None)
    if (platform == "tiktok"):
        return data.get("error", None) == None
    return True


def fetch_extract_data_api(post_id, platform, url, input_file_id, query):
    api_url = baseUrl + f"crawl/{platform}?post_id={post_id}"
    max_retry = 5  # Số lần tối đa gọi lại API khi thất bại
    retry_count = 0

    is_active_detect_voice = get_value_from_query(query, "is_detect_voice") == "true"
    keywords = get_value_from_query(query, "keywords")
    keywords = keywords.split(",")
    language_code = get_value_from_query(query, "language_code")
    if language_code == "undefined":
        language_code = "vi"

    while retry_count < max_retry:
        response = requests.get(api_url)
        api_url = api_url + f"&retry={retry_count}"
        if response.status_code == 200:
            res_data = response.json()
            data = res_data['data']
            valid = valid_data(data, platform)
            if valid:
                data_result = data_parse(data, platform, input_file_id, url)
                if is_active_detect_voice:
                    data_result = detect_voice(data_result, keywords, language_code)

                return data_result
            else:
                retry_count += 1
        else:
            print(tab_log("extract-data") + f": Retry to call api {platform} url: {api_url}")
            retry_count += 1

    # Đã gọi API quá 3 lần mà vẫn thất bại
    return data_failed_default(url, platform, input_file_id)


def tracking_word_occurrences(word, s):
    count = s.count(word)
    return f"{word} ({count})"


def get_keywords_occurrences(keywords_list, transcript):
    result = [tracking_word_occurrences(key, transcript) for key in keywords_list if key.strip()]
    return ','.join(result)


def get_youtube_video_url(url):
    print("get_youtube_video_url");
    postId = get_post_id_by_platform(url)
    api_url = baseUrl + f"crawl/youtube/video-url?postId={postId}"
    print(api_url)
    response = requests.get(api_url)
    if response.status_code == 200:
        res_data = response.json()
        url = res_data['data']
        return url


def detect_voice(item, keywords, language_code="vi"):
    file_url = item.get("file_url")
    if item.get("platform") == "youtube":
        file_url = get_youtube_video_url(item.get("url"))

    if file_url:
        api_url = baseUrl + f"voice-to-text?&language_code={language_code}&file_url={urllib.parse.quote(file_url)}"
        print("api_url", api_url)
        response = requests.post(api_url, timeout=600)
        print("response", response)
        if response.status_code == 200:
            res_data = response.json()
            print("res_data", res_data)
            data = res_data['data']
            logger.info(f"{tab_log(tab)}: transcript_id {data.get('id')}, file: {item.get('url')}")
            transcript = data.get('text')
            item['transcript'] = transcript
            item['match_keywords'] = get_keywords_occurrences(keywords, transcript)

    return item


async def extract_data_crawl_data(rows, input_file_id, query, tab="videos-extract-data"):
    # Initialize variables
    file_path = set_file_path(tab)
    platform = get_platform(rows[0][0])
    if platform == "tiktok":
        max_workers = 25
    else:
        max_workers = 12
    is_detect_voice = get_value_from_query(query, "is_detect_voice") == "true"
    if is_detect_voice == False:
        max_workers = 5
    else:
        tab = tab + "-detect-voice"

    # Initialize chunks
    chunks = init_chunks(rows, max_workers)

    # try:
    # Loop through chunks to crawl data
    old_process = 0
    for index, chunk in enumerate(chunks):
        # try:
        results = extract_data_process_chunk(chunk, query, input_file_id, max_workers)
        print("results", results)
        # update process
        now_process = int(((index + 1) / len(chunks)) * 100)
        print("now_process - old", now_process - old_process)

        # Append results to the sheet
        import_rows_api(results, tab)

        # Update status
        if now_process - old_process > 0:
            old_process = now_process
            update_data = await update_progress_input_file({'id': input_file_id, 'progress': now_process, 'index_processed': index * max_workers})
            # handle stop
            if update_data.get('status', None) == "stop":
                logger.info(f"{tab_log(tab)}: Stop process")
                break
        # except Exception as e:
        #     logger.info(f"{tab_log(tab)}: chunk: {chunk} | error: {e}")

    return tab


# except Exception as e:
#     # In case of any exception, save the workbook and close it before re-raising the exception
#     return file_path

def extract_data_process_chunk(chunk, query, input_file_id, max_workers=3):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # get id and platform from url in same time
        ids_and_platforms = [(get_post_id_by_platform(url), get_platform(url), url) for url in chunk]
        # call api
        results = list(
            executor.map(lambda x: fetch_extract_data_api(x[0], x[1], x[2], input_file_id, query), ids_and_platforms))

    return results
