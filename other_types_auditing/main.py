import datetime
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
    weekly_readings_columns = ['unitId', 'rowId', 'address', 'latitude', 'longitude', 'speedLimit', 'unitType', 'state']
    if missing_rows:
        weekly_readings_query = f"select unitId, rowId, address, latitude, longitude, speedLimit, unitType, state from device_readings where unitTime<'{from_45_minutes_ago}' and unitTime>='{from_1_hour_ago}' and (unitType = 'Queclink' or unitType = 'Waylens') " \
                                f"order by unitTime"
    else:
        weekly_readings_query = f"select unitId, rowId, address, latitude, longitude, speedLimit, unitType, state from device_readings where (unitType = 'Queclink' or unitType = 'Waylens') and unitTime<'{from_45_minutes_ago}' and unitTime>='{from_1_hour_ago}' and (latitude is NULL or longitude is NULL or address is NULL or address = '{{ address: null, city: null, state: null, zip: null }}')" \
                                f"order by unitTime"
    measures = query_db(weekly_readings_file_name,
                        weekly_readings_columns, weekly_readings_query)

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
                    'Address': str(row['address']), 'Latitude': str(row['latitude']), 'Longitude': str(row['longitude']), 'Speed limit': str(row['speedLimit']),
                    'Unit Type': str(row['unitType']), 'State': str(row['state'])
                    }
                missing_intervals.append(missing_interval)
                latest_row_id = rowId
    log_to_s3(missing_intervals, missing_rows=True)


def partial_rows():
    last_hours_measures_df = get_measures(missing_rows=False)
    missing_intervals = []
    for index, row in last_hours_measures_df.iterrows():
        missing_interval = {
            'USN': str(row["unitId"]), 'Starting row ID': str(row["rowId"]), 'Finishing row ID': str(row["rowId"]),
            'Address': str(row['address']), 'Latitude': str(row['latitude']), 'Longitude': str(row['longitude']), 'Speed limit': str(row['speedLimit']),
            'Unit Type': str(row['unitType']), 'State': str(row['state'])}

        missing_intervals.append(missing_interval)

    log_to_s3(missing_intervals, missing_rows=False)


def log_to_s3(missing_intervals, missing_rows):
    df_missing_data = pd.DataFrame(missing_intervals)
    minute = datetime.datetime.now().minute
    if missing_rows:
        file_name = f'missing_rows_{minute}.parquet'
        file_type = 'missing_rows'
    else:
        file_name = f'partial_rows_{minute}.parquet'
        file_type = 'partial_rows'

    upload_df_to_s3_as_parquet(df_missing_data, file_name, file_type)


if __name__ == '__main__':
    if(os.environ['MISSING_ROWS'] == "True"):
        missing_rows()
    else:
        partial_rows()
