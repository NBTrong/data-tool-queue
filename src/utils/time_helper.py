from datetime import datetime
def convert_timestamp_to_date_string(timestamp):
    try:
        date_string = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        return date_string
    except Exception as e:
        print("e",e)
        return f"{timestamp} "