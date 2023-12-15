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
import json

from urllib.parse import quote, quote_plus

from src.config.config import baseUrl,  message_str
from src.utils.api.apis import import_rows_api
from src.utils.time_helper import convert_timestamp_to_date_string
from src.utils.utils import tab_log, add_row_to_excel, timestamp_to_date_string, read_excel_to_array, group_by_column, \
    update_progress_input_file, init_chunks, get_post_id_by_platform, get_platform, get_value_from_query

logger = logging.getLogger(__name__)

extract_comments_header_row = ["Post URL","Platform","Total comments","Commenter","Text","Created at"]

tab="comments-extract-data"

def data_failed_default(url,platform):
    return {
        'url': url,
        'title': "This post could not be found",
        'platform':platform
    }


def data_parse_comments(data,platform="tiktok"):
    if platform == "tiktok":
        comments = []
        if data and 'comments' in data:
            for comment in data['comments']:
                user = comment.get('user', {})
                avatar_thumb_url = \
                    user.get('avatar_thumb', {}).get('url_list', [user.get('cover_url', [])[0].get('url_list', [])[0]])[
                        0]

                parsed_comment = {
                    'like_count': comment.get('digg_count'),
                    'aweme_id': comment.get('aweme_id'),
                    'cid': comment.get('cid'),
                    'create_time': comment.get('create_time'),
                    'text': comment.get('text'),
                    'no_more_replies': False,
                    'reply_comment_total': comment.get('reply_comment_total', 0),
                    'replies': [],
                    'user': {
                        'unique_id': user.get('unique_id', ''),
                        'url': "https://tiktok.com/@" + user.get('unique_id', ''),
                        'nickname': user.get('nickname', ''),
                        'avatar_thumb': avatar_thumb_url,
                    },
                }

                comments.append(parsed_comment)

        return {'comments': comments, 'hasMore': data.get('hasMore'), 'count': data.get('count')}

    if platform == "youtube":
        comments = []
        if data and 'comments' in data:
            for comment in data['comments']:
                comment_info = {
                    'like_count': comment['snippet']['topLevelComment']['snippet']['likeCount'],
                    'postId': comment['snippet']['videoId'],
                    'cid': comment['id'],
                    'create_time': comment['snippet']['topLevelComment']['snippet']['publishedAt'],
                    'text': comment['snippet']['topLevelComment']['snippet']['textOriginal'],
                    'no_more_replies': True,
                    'reply_comment_total': len(comment['replies']['comments']) if 'replies' in comment else 0,
                    'replies': []
                }

                if 'replies' in comment:
                    for reply in comment['replies']['comments']:
                        reply_info = {
                            'like_count': reply['snippet']['likeCount'],
                            'postId': reply['snippet']['videoId'],
                            'cid': comment['id'],
                            'create_time': reply['snippet']['publishedAt'],
                            'text': reply['snippet']['textOriginal'],
                            'aweme_id': reply['snippet']['videoId'],
                            'user': {
                                'unique_id': reply['snippet']['authorChannelUrl'],
                                'url': reply['snippet']['authorChannelUrl'],
                                'nickname': reply['snippet']['authorDisplayName'],
                                'avatar_thumb': reply['snippet']['authorProfileImageUrl']
                            }
                        }
                        comment_info['replies'].append(reply_info)

                user_info = {
                    'unique_id': comment['snippet']['topLevelComment']['snippet']['authorChannelUrl'],
                    'url': comment['snippet']['topLevelComment']['snippet']['authorChannelUrl'],
                    'nickname': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'avatar_thumb': comment['snippet']['topLevelComment']['snippet']['authorProfileImageUrl']
                }
                comment_info['user'] = user_info

                comments.append(comment_info)

        return {'comments': comments, 'hasMore': 'nextPageToken' in data,
                'nextPageToken': data.get('nextPageToken', False)}

def data_parse(data, platform, url,input_file_id):
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

            'thumb_url': post_image,
            'description': data['caption']['text'] if data and 'caption' in data else '',
            'platform': platform,

            'post_url': url
        }


def valid_data(data,platform):
    if(platform == "youtube"):
        return data.get("items",None)
    if (platform == "tiktok"):
        return data.get("error", None) == None
    return True

def valid_data_comments(data):
    valid = data.get("comments", None)
    if valid == None:
        valid = False
    else: valid = True
    return valid

def fetch_extract_data_api_tiktok(post_id, platform, url,input_file_id):
    api_url = baseUrl + f"crawl/{platform}?post_id={post_id}"
    max_retry = 5  # Số lần tối đa gọi lại API khi thất bại
    retry_count = 0

    while retry_count < max_retry:
        response = requests.get(api_url)
        api_url = api_url + f"&retry={retry_count}"
        print(api_url)
        if response.status_code == 200:
            res_data = response.json()
            data = res_data['data']
            valid = valid_data(data, platform)
            if valid:
                data_result = data_parse(data, platform, url,input_file_id)
                if data_result['total_comments']:
                    comments = fetch_comments_api(post_id,platform)
                    for index, comment in enumerate(comments):
                        if int(comment['reply_comment_total']) > 0:
                            comments[index]['replies'] = fetch_comments_api(post_id, comment['cid'],
                                                                            "post-comment-replies")

                    data_result['comments'] = json.dumps(comments)

                return data_result
            else:
                retry_count += 1
        else:
            print(tab_log("extract-data") + f": Retry to call api {platform} url: {api_url}")
            retry_count += 1

    # Đã gọi API quá 3 lần mà vẫn thất bại
    return data_failed_default(url, platform)

def fetch_extract_data_api_youtube(post_id, platform, url,input_file_id):
    api_url = baseUrl + f"crawl/{platform}?post_id={post_id}"
    max_retry = 5  # Số lần tối đa gọi lại API khi thất bại
    retry_count = 0

    while retry_count < max_retry:
        response = requests.get(api_url)
        api_url = api_url + f"&retry={retry_count}"
        print(api_url)
        if response.status_code == 200:
            res_data = response.json()
            data = res_data['data']
            valid = valid_data(data, platform)
            if valid:
                data_result = data_parse(data, platform, url)
                if data_result['statistics']['comment_count']:
                    comments = fetch_comments_api(post_id,platform)

                    data_result['comments'] = comments

                return data_result
            else:
                retry_count += 1
        else:
            print(tab_log("extract-data") + f": Retry to call api {platform} url: {api_url}")
            retry_count += 1

    # Đã gọi API quá 3 lần mà vẫn thất bại
    return data_failed_default(url, platform)

def fetch_extract_data_api(post_id, platform, url,query,input_file_id):
    if platform == "tiktok":
        return fetch_extract_data_api_tiktok(post_id, platform, url,input_file_id)

    if platform == "youtube":
        return fetch_extract_data_api_youtube(post_id, platform, url,input_file_id)

    return data_failed_default(url,platform)

def fetch_comments_api_tikok(post_id,comment_id="",path="post-comments"):
    max_retry = 5  # Số lần tối đa gọi lại API khi thất bại
    retry_count = 0
    cursor = 0
    base_api_url = baseUrl + f"crawl/tiktok/{path}?aweme_id={post_id}&comment_id={comment_id}"
    has_more = 1

    comments = []

    while retry_count < max_retry and has_more:
        api_url = base_api_url + f"&cursor={cursor}&retry={retry_count}"
        response = requests.get(api_url)
        print(api_url)
        if response.status_code == 200:
            res_data = response.json()
            data = res_data['data']
            valid = valid_data_comments(data)
            if valid:
                retry_count = 0
                parse_data = data_parse_comments(data,"tiktok")
                has_more = parse_data['hasMore']
                comments += parse_data['comments']
                cursor += parse_data['count']
            else:
                retry_count += 1

        else:
            retry_count += 1

    # Đã gọi API quá 3 lần mà vẫn thất bại
    return comments

def fetch_comments_api_youtube(post_id):
    max_retry = 5  # Số lần tối đa gọi lại API khi thất bại
    retry_count = 0
    pageToken = ""
    has_more = 1

    base_api_url = baseUrl + f"crawl/youtube/post-comments?postId={post_id}"

    comments = []

    while retry_count < max_retry and has_more:
        api_url = base_api_url + f"&pageToken={pageToken}&retry={retry_count}"
        response = requests.get(api_url)
        print(api_url)
        if response.status_code == 200:
            res_data = response.json()
            data = res_data['data']
            valid = valid_data_comments(data)
            if valid:
                retry_count = 0
                parse_data = data_parse_comments(data,"youtube")
                has_more = parse_data['hasMore']
                comments += parse_data['comments']
                pageToken = parse_data['nextPageToken']
            else:
                retry_count += 1

        else:
            retry_count += 1

    # Đã gọi API quá 3 lần mà vẫn thất bại
    return comments

def fetch_comments_api(post_id,platform,comment_id="",path="post-comments"):
    if platform == "tiktok":
        return fetch_comments_api_tikok(post_id,comment_id,path)
    if platform == "youtube":
        return fetch_comments_api_youtube(post_id)


async def extract_comments_crawl_data(rows, input_file_id,query, tab="comments-extract-data"):
    # Initialize variables
    max_workers = 1

    # Initialize chunks
    chunks = init_chunks(rows, max_workers)

    try:
    # Loop through chunks to crawl data
        old_process = 0

        for index, chunk in enumerate(chunks):
            try:
                results = extract_data_video_process_chunk(chunk, query, input_file_id,max_workers)
                print("results",results)

                # Append results to the sheet
                import_rows_api(results,tab)

                # update process
                now_process = int(((index + 1) / len(chunks)) * 100)
                if now_process - old_process > 0:
                    old_process = now_process
                    update_data = await update_progress_input_file({'id': input_file_id, 'progress': now_process})
                    # handle stop
                    if update_data.get('status', None) == "stop":
                        logger.info(f"{tab_log(tab)}: Stop process")
                        break

            except Exception as e:
                logger.info(f"{tab_log(tab)}: chunk: {chunk} | error: {e}")

        return tab
    except Exception as e:
        # In case of any exception, save the workbook and close it before re-raising the exception
        raise e

def extract_data_video_process_chunk(chunk,query,input_file_id,max_workers=3):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # get id and platform from url in same time
        ids_and_platforms = [(get_post_id_by_platform(url), get_platform(url),url) for url in chunk]
        filtered_items = [item for item in ids_and_platforms if item[1] in ['tiktok', 'youtube']]
        # call api
        results = list(executor.map(lambda x: fetch_extract_data_api(x[0], x[1],x[2],query,input_file_id), filtered_items))

    return results
