from utils.s3_management import upload_df_to_s3
import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict
import concurrent.futures
import json 
from utils.db_query import query_db

MAX_CONNECTIONS = 10

def lambda_handler(event, context):
    global do_cleanup
    print(event)
    accountId = event['accountId']
    do_cleanup = event['cleanup'] == "True" 
    missing_rows(accountId)
    partial_rows(accountId)
    return {
        'statusCode': 200,
        'body': json.dumps(str(accountId) + " audited")
    }

def get_accountId_rows(missing_rows, accountId):
    accountId_readings_file_name = 'accountId_readings.csv'
    accountId_readings_columns = ['rowId','unitId']
    if missing_rows:
        accountId_readings_query = f"select rowId, unitId from device_readings where accountId='{accountId}'" 
    else:
        accountId_readings_query = f"select rowId, unitId from device_readings where unitType != 'Queclink' and accountId='{accountId}' and (latitude is NULL or longitude is NULL or address is NULL or address = '{{ address: null, city: null, state: null, zip: null }}' or state is NULL)" 
    
    accountId_rows_df = query_db(accountId_readings_file_name,
                        accountId_readings_columns, accountId_readings_query)

    return accountId_rows_df

def missing_rows(accountId):
    accountId_rows_df = get_accountId_rows(missing_rows=True, accountId=accountId)
    accountId_rows_grouped = accountId_rows_df.groupby('unitId')
    for unitId, unitId_group_df in accountId_rows_grouped:
        unitId_rows_sorted = unitId_group_df.sort_values(
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
        send_requests(missing_intervals, missing_rows=True, accountId=accountId)
    else:
        auditing_to_s3(missing_intervals, missing_rows=True, accountId=accountId)

def partial_rows(accountId):
    accountId_rows_df = get_accountId_rows(missing_rows=False, accountId=accountId)
    missing_intervals = []
    for _, row in accountId_rows_df.iterrows():
        missing_interval = {
             'USN': str(row["unitId"]), 'Starting row ID': str(row["rowId"]), 'Finishing row ID': str(row["rowId"])}
        missing_intervals.append(missing_interval)
    if do_cleanup:
        send_requests(missing_intervals, missing_rows=False, accountId=accountId)
    else:
        auditing_to_s3(missing_intervals, missing_rows=False, accountId=accountId)

def auditing_to_s3(missing_intervals, missing_rows, accountId):
    if missing_rows:
        audit_file_name = f'missing_rows_audited_{accountId}.csv'
    else:
        audit_file_name = f'partial_rows_audited_{accountId}.csv'
    df_audited_data = pd.DataFrame(missing_intervals)
    upload_df_to_s3(df_audited_data, audit_file_name, accountId)

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

def send_requests(missing_intervals, missing_rows, accountId):
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
        failed_file_name = f'missing_rows_failed_{accountId}.csv'
        success_file_name = f'missing_rows_success_{accountId}.csv'
    else:
        failed_file_name = f'partial_rows_failed_{accountId}.csv'
        success_file_name = f'partial_rows_success_{accountId}.csv'

    upload_df_to_s3(df_failed_data, failed_file_name, accountId)
    upload_df_to_s3(df_success_data, success_file_name, accountId)
