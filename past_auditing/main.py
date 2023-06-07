import datetime
import logging
import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict
import concurrent.futures
import os
import time 

from utils.db_query import query_db
from utils.s3_management import upload_df_to_s3, upload_success_to_s3


def get_measures(missing_rows):
    """
    Retrieves the dataframes for the measures of each device, and the set of devices reporting
    measures, both in the previous week, from the db.
    :return: a dataframe with the measures, and another dataframe with those devices actively reporting.
    """
    weekly_readings_file_name = 'hour_readings_0.csv'
    weekly_readings_columns = ['unitId', 'rowId', 'unitTime']
    if missing_rows:
        weekly_readings_query = f"select unitId, rowId, unitTime from device_readings where unitTime>='{date_lower_bound}' and unitTime<'{date_higher_bound}' and unitType != 'Queclink' " \
                                f"order by unitTime"
    else:
        weekly_readings_query = f"select unitId, rowId, unitTime from device_readings where unitType != 'Queclink' and unitTime>='{date_lower_bound}' and unitTime<'{date_higher_bound}' and (latitude is NULL or longitude is NULL or address is NULL or address = '{{ address: null, city: null, state: null, zip: null }}')" \
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
        unitIdGroup = unitIdGroup.sort_values("rowId")
        rowIdsList = list(unitIdGroup['rowId'])
        minimum = rowIdsList[0]
        latest_row_id = minimum - 1
        for rowId in rowIdsList:
            if rowId == latest_row_id + 1:
                latest_row_id = rowId
            else:
                missing_interval = {
                    'USN': unitId, 'Starting row ID': latest_row_id + 1, 'Finishing row ID': rowId - 1}
                missing_intervals.append(missing_interval)
                latest_row_id = rowId
    if do_cleanup:
        send_requests(missing_intervals, missing_rows=True)
    else:
        auditing_to_s3(missing_intervals, missing_rows=True)

def partial_rows():
    last_hours_measures_df = get_measures(missing_rows=False)
    missing_intervals = []
    for index, row in last_hours_measures_df.iterrows():
        missing_interval = {
            'USN': str(row["unitId"]), 'Starting row ID': str(row["rowId"]), 'Finishing row ID': str(row["rowId"])}
        missing_intervals.append(missing_interval)
    if do_cleanup:
        send_requests(missing_intervals, missing_rows=False)
    else:
        auditing_to_s3(missing_intervals, missing_rows=False)

def auditing_to_s3(missing_intervals, missing_rows):
    df_success_data = pd.DataFrame(missing_intervals)
    hour = date_lower_bound.hour
    today = date_lower_bound.strftime('%y-%m-%d')
    if missing_rows:
        file_name = f'missing_rows_{hour}.csv'
    else:
        file_name = f'partial_rows_{hour}.csv'
    df_audited_data = pd.DataFrame(missing_intervals)
    upload_df_to_s3(df_audited_data, file_name, hour, today, do_cleanup)

def send_request(missing_interval):
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["x-api-key"] = "85lJUdDSaD9vGyepQsard22o0BaubRwR5lX6vTY0"
    url = "https://sazku5imk9.execute-api.us-east-1.amazonaws.com/stage/legacyToReadings"
    data = """
                    {
                        "USN": "%s",
                        "startRID": "%s",
                        "endRID": "%s",
                        "recoverAddress": true
                    }
            """ % (missing_interval["USN"], missing_interval["Starting row ID"], missing_interval["Finishing row ID"])
    response = requests.post(url, headers=headers, data=data)
    return (response, missing_interval)


def send_requests(missing_intervals, missing_rows):
    failures = 0
    failed_intervals = []
    succesful_intervals = []
    weekday = datetime.datetime.today().weekday()
    hour = datetime.datetime.today().hour
    high_connections_condition = weekday > 4 or hour < 7 or hour > 21
    hourly_connections = 8 if high_connections_condition else 24
    with concurrent.futures.ThreadPoolExecutor(max_workers=hourly_connections) as executor:
        future_to_requests = {executor.submit(
            send_request, missing_interval): missing_interval for missing_interval in missing_intervals}
        for future in concurrent.futures.as_completed(future_to_requests):
            try:
                (response, missing_interval) = future.result()
                if(response.status_code != 200):
                    failures += 1
                    failed_intervals.append(missing_interval)
                else:
                    succesful_intervals.append(missing_interval)
            except:
                failed_intervals.append(missing_interval)

    logging.warning("The number of intervals was: " + str(len(missing_intervals)))

    df_failed_data = pd.DataFrame(failed_intervals)
    df_success_data = pd.DataFrame(succesful_intervals)
    hour = date_lower_bound.hour
    today = date_lower_bound.strftime('%y-%m-%d')
    if missing_rows:
        failed_file_name = f'missing_rows_failed_{hour}.csv'
        success_file_name = f'missing_rows_success_{hour}.csv'
    else:
        failed_file_name = f'partial_rows_failed_{hour}.csv'
        success_file_name = f'partial_rows_success_{hour}.csv'

    upload_df_to_s3(df_failed_data, failed_file_name, hour, today, do_cleanup)
    upload_df_to_s3(df_success_data, success_file_name, hour, today, do_cleanup)
  
if __name__ == '__main__':
    initial_date = datetime.datetime.fromisoformat(
        os.environ['INITIAL_DATE'])
    final_date = datetime.datetime.fromisoformat(
        os.environ['FINAL_DATE'])
    do_cleanup = os.environ['CLEANUP'] == "True"
    date_higher_bound = final_date 
    date_lower_bound = final_date - datetime.timedelta(hours=1)
    while date_lower_bound >= initial_date:
        start_time = time.time()
        if(os.environ['MISSING_ROWS'] == "True"):
            missing_rows()
        else:
            partial_rows()
        logging.warning('The following time interval was audited: ' +
                        str(date_lower_bound) + " to " + str(date_higher_bound))
        date_higher_bound = date_higher_bound - datetime.timedelta(hours=1)
        date_lower_bound = date_higher_bound - datetime.timedelta(hours=1)
        end_time = time.time()
        logging.warning("It took " + str(end_time - start_time) + " seconds to finish")
        logging.warning("    ")
