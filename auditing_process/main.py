import json
import datetime
import boto3
import logging
import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict
import concurrent.futures
import os

from utils.db_query import query_db
from utils.s3_management import upload_df_to_s3, upload_success_to_s3, upload_df_to_s3_as_parquet


def get_measures(missing_rows):
    """
    Retrieves the dataframes for the measures of each device, and the set of devices reporting
    measures, both in the previous week, from the db.
    :return: a dataframe with the measures, and another dataframe with those devices actively reporting.
    """


    from_45_minutes_ago = datetime.datetime.now() - datetime.timedelta(minutes=45)
    from_1_hour_ago = datetime.datetime.now() - datetime.timedelta(hours=1)
    weekly_readings_file_name = 'hour_readings.csv'
    weekly_readings_columns = ['unitId', 'rowId', 'unitTime', 'address', 'latitude', 'longitude', 'speedLimit', 'state']
    if missing_rows:
        weekly_readings_query = f"select unitId, rowId, unitTime, address, latitude, longitude, speedLimit, state from device_readings where unitTime<'{from_45_minutes_ago}' and unitTime>='{from_1_hour_ago}' and unitType != 'Queclink' and unitType!='Waylens' " \
                                f"order by unitTime"
    else:
        weekly_readings_query = f"select unitId, rowId, unitTime, address, latitude, longitude, speedLimit, state from device_readings where unitType != 'Queclink' and unitType!='Waylens' and unitTime<'{from_45_minutes_ago}' and unitTime>='{from_1_hour_ago}' and (latitude is NULL or longitude is NULL or address is NULL or address = '{{ address: null, city: null, state: null, zip: null }}')" \
                                f"order by unitTime"
    measures = query_db(weekly_readings_file_name,
                        weekly_readings_columns, weekly_readings_query)
    measures['unitTime'] = pd.to_datetime(measures['unitTime'], utc=False)

    return measures

def missing_rows():
    last_hours_measures_df = get_measures(missing_rows=True)
    last_hour_events_grouped = last_hours_measures_df.groupby('unitId')
    missing_intervals = []
    for unitId, unitIdGroup in last_hour_events_grouped:
        unitIdGroup = unitIdGroup.sort_values('rowId')
        minimum = unitIdGroup.iloc[0]['rowId']
        latest_row_id = minimum - 1
        for grouprow in unitIdGroup.iterrows():
            row = grouprow[1]
            rowId = row['rowId']
            if rowId == latest_row_id + 1:
                latest_row_id = rowId
            else:
                missing_interval = {
                    'USN': str(unitId), 'Starting row ID': str(latest_row_id + 1), 'Finishing row ID': str(rowId - 1),
                    'Address': str(row['address']), 'Latitude': str(row['latitude']), 'Longitude': str(row['longitude']), 
                    'Speed limit': str(row['speedLimit']), 'State': str(row['state'])
                    }
                missing_intervals.append(missing_interval)
                latest_row_id = rowId
    send_requests(missing_intervals, missing_rows=True)


def partial_rows():
    last_hours_measures_df = get_measures(missing_rows=False)
    missing_intervals = []
    for index, row in last_hours_measures_df.iterrows():
        missing_interval = {
            'USN': str(row["unitId"]), 'Starting row ID': str(row["rowId"]), 'Finishing row ID': str(row["rowId"]),
            'Address': str(row['address']), 'Latitude': str(row['latitude']), 'Longitude': str(row['longitude']), 
            'Speed limit': str(row['speedLimit']), 'State': str(row['state'])
            }
        missing_intervals.append(missing_interval)

    send_requests(missing_intervals, missing_rows=False)

def create_request(missing_interval):
    try:
        s3 = boto3.resource('s3')
        bucket_name = 'ingest-backup'
        s3_key = f'rgeo/{str(missing_interval["USN"])}/{str(missing_interval["Starting row ID"])}.json'
        obj = s3.Object(bucket_name, s3_key)
        row_info = obj.get()['Body'].read().decode('utf-8')
        row_json = json.loads(row_info)
        address = row_json['address']
        city = address['city']
        try:
            state = address['state']
            zip_code = address['zip']
        except:
            state = ''
            zip_code = ''
        row_has_address_in_s3 = False
        recover_address = 'false'
        data = """
                    {
                        "USN": "%s",
                        "startRID": "%s",
                        "endRID": "%s",
                        "address": "%s",
                        "city": "%s",
                        "state": "%s",
                        "zip": "%s",
                        "recoverAddress": "%s"
                    }
            """ % (missing_interval["USN"], missing_interval["Starting row ID"], missing_interval["Finishing row ID"], address, city, state, zip_code, recover_address)
    except Exception as e: 
        row_has_address_in_s3 = False
        recover_address = 'true'
        data = """
                    {
                        "USN": "%s",
                        "startRID": "%s",
                        "endRID": "%s",
                        "recoverAddress": "%s"
                    }
            """ % (missing_interval["USN"], missing_interval["Starting row ID"], missing_interval["Finishing row ID"], recover_address)
    response = send_request(missing_interval, data)
    return (response, missing_interval, row_has_address_in_s3)

def send_request(missing_interval, data):
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["x-api-key"] = "85lJUdDSaD9vGyepQsard22o0BaubRwR5lX6vTY0"
    url = "https://sazku5imk9.execute-api.us-east-1.amazonaws.com/stage/legacyToReadings"
    response = requests.post(url, headers=headers, data=data.encode('utf-8'))
    return response 


def send_requests(missing_intervals, missing_rows):
    intervals_with_addresses = []
    intervals_without_addresses = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=int(os.environ['CONNECTIONS'])) as executor:
        future_to_requests = {executor.submit(
            create_request, missing_interval): missing_interval for missing_interval in missing_intervals}
        for future in concurrent.futures.as_completed(future_to_requests):
            (response, missing_interval, row_has_address_in_s3) = future.result() 
            if(row_has_address_in_s3):
                intervals_with_addresses.append(missing_interval)
            else:
                intervals_without_addresses.append(missing_interval)

    logging.warning("The number of intervals was: " + str(len(missing_intervals)))

    df_intervals_with_address = pd.DataFrame(intervals_with_addresses)
    df_intervals_without_address = pd.DataFrame(intervals_without_addresses)
    minute = datetime.datetime.now().minute
    if missing_rows:
        file_type = 'missing_rows'
        intervals_with_address_file_name = f'missing_rows_with_address_{minute}.parquet'
        intervals_without_address_file_name = f'missing_rows_without_address_{minute}.parquet'
    else:
        file_type = 'partial_rows'
        intervals_with_address_file_name = f'partial_rows_with_address_{minute}.parquet'
        intervals_without_address_file_name = f'partial_rows_without_address_{minute}.parquet'

    upload_df_to_s3_as_parquet(df_intervals_with_address, intervals_with_address_file_name, file_type)
    upload_df_to_s3_as_parquet(df_intervals_without_address, intervals_without_address_file_name, file_type)

if __name__ == '__main__':
    if(os.environ['MISSING_ROWS'] == "True"):
        missing_rows()
    else:
        partial_rows()
