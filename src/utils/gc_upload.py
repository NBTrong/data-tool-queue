from google.cloud import storage
import os
import time
from google.oauth2 import service_account
import uuid
#
# def get_local_file_path(file_name):
#     if os.path.exists(f"/{file_name}"):
#         print("file name is file path")
#         return file_name
#     else:
#         local_path = f"/root/{file_name}"
#         if os.path.exists(local_path):
#             # run by crontab
#             return local_path
#         else:
#             # run directly
#             return f"/src/job/queue/{file_name}"


def upload_excel_to_gcs(local_file_path, tab=''):
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    path_account_file = os.getenv('GCS_SERVICE_ACCOUNT_PATH')

    gcs_file_path = f"xdata-file/{tab}/data-crawled"
    short = str(uuid.uuid4())
    file_name_on_gcs = f"{gcs_file_path}/crawled-{tab}/{short}.{local_file_path.split('.')[-1]}"

    credentials = service_account.Credentials.from_service_account_file(path_account_file)
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(file_name_on_gcs)

    try:
        blob.upload_from_filename(local_file_path)
        url = f"https://storage.googleapis.com/{bucket_name}/{file_name_on_gcs}"
        return url
    except Exception as e:
        print("Upload failed:", e)
        return False

def generate_short_uuid():
    full_uuid = str(uuid.uuid4())
    short_uuid = full_uuid.split('-')[0][:8]
    return short_uuid
