from utils.s3_management import upload_df_to_s3
import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict
import concurrent.futures
import json 
from utils.db_query import query_db

MAX_CONNECTIONS = 500

def lambda_handler(event, context):
    global do_cleanup
    unitId = event['unitId']
    do_cleanup = event['cleanup'] == "True" 
    missing_rows(unitId)
    partial_rows(unitId)
    return {
        'statusCode': 200,
        'body': json.dumps(str(unitId) + " audited")
    }

def get_unitId_rows(missing_rows, unitId):
    unitId_readings_file_name = 'unitId_readings.csv'
    unitId_readings_columns = ['rowId']
    if missing_rows:
        unitId_readings_query = f"select rowId from device_readings where unitId='{unitId}'" 
    else:
        unitId_readings_query = f"select rowId from device_readings where unitType != 'Queclink' and unitId='{unitId}' and (latitude is NULL or longitude is NULL or address is NULL or address = '{{ address: null, city: null, state: null, zip: null }}' or state is NULL)" 
    
    unitId_rows_df = query_db(unitId_readings_file_name,
                        unitId_readings_columns, unitId_readings_query)

    return unitId_rows_df

def missing_rows(unitId):
    unitId_rows_df = get_unitId_rows(missing_rows=True, unitId=unitId)
    unitId_rows_sorted = unitId_rows_df.sort_values(
        by='rowId')
    missing_intervals = []
    rowIds = unitId_rows_sorted['rowId']
    minimum = rowIds[0]
    latest_row_id = minimum - 1
    for rowId in rowIds:
        if rowId == latest_row_id + 1:
            latest_row_id = rowId
        else:
            missing_interval = {
                'USN': unitId, 'Starting row ID': latest_row_id + 1, 'Finishing row ID': rowId - 1}
            missing_intervals.append(missing_interval)
            latest_row_id = rowId
    if do_cleanup:
        send_requests(missing_intervals, missing_rows=True, unitId=unitId)
    else:
        auditing_to_s3(missing_intervals, missing_rows=True, unitId=unitId)

def partial_rows(unitId):
    unitId_rows_df = get_unitId_rows(missing_rows=False, unitId=unitId)
    missing_intervals = []
    for _, row in unitId_rows_df.iterrows():
        missing_interval = {
            'USN': str(unitId), 'Starting row ID': str(row["rowId"]), 'Finishing row ID': str(row["rowId"])}
        missing_intervals.append(missing_interval)
    if do_cleanup:
        send_requests(missing_intervals, missing_rows=False, unitId=unitId)
    else:
        auditing_to_s3(missing_intervals, missing_rows=False, unitId=unitId)

def auditing_to_s3(missing_intervals, missing_rows, unitId):
    if missing_rows:
        audit_file_name = f'missing_rows_audited_{unitId}.csv'
    else:
        audit_file_name = f'partial_rows_audited_{unitId}.csv'
    df_audited_data = pd.DataFrame(missing_intervals)
    upload_df_to_s3(df_audited_data, audit_file_name, unitId)

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

def send_requests(missing_intervals, missing_rows, unitId):
    failures = 0
    failed_intervals = []
    succesful_intervals = []
    with concurrent.futures.ThreadPoolExecutor(MAX_CONNECTIONS) as executor:
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

    df_failed_data = pd.DataFrame(failed_intervals)
    df_success_data = pd.DataFrame(succesful_intervals)
    if missing_rows:
        failed_file_name = f'missing_rows_failed_{unitId}.csv'
        success_file_name = f'missing_rows_success_{unitId}.csv'
    else:
        failed_file_name = f'partial_rows_failed_{unitId}.csv'
        success_file_name = f'partial_rows_success_{unitId}.csv'

    upload_df_to_s3(df_failed_data, failed_file_name, unitId)
    upload_df_to_s3(df_success_data, success_file_name, unitId)



