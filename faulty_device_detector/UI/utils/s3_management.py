import datetime
import os
import boto3
from datetime import timedelta, date
import pandas as pd
import time
import logging

bucket_name = os.environ['BUCKET_NAME']

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)



def get_device_measures_with_success_by_date(today_date):
    """
    Returns all measures of devices in the latest date of data uploaded with success.
    :param today_date: today's date.
    :return: Dataframe with measures of all devices from previous 7 days
    """
    return_dict = {}
    measures_dataframes_list = []
    latest_date = get_latest_date_with_success_from_s3(today_date)
    i = 0
    for obj in bucket.objects.filter(Prefix=str(latest_date)):
        return_dict[i] = obj.key
        i = i +1
        file_path = obj.key
        split_path = file_path.split('/')
        return_dict[i] = split_path
        i = i + 1
        
        if len(split_path) == 3:
            date = split_path[0]
            hour = split_path[1]
            file_name = split_path[2]
            if file_name.split('_')[0] == "daily":
                hourly_measures_file = load_csv_file_from_s3_to_df(file_name, f'{date}/{hour}/{file_name}')
                if hourly_measures_file is not None:
                    measures_dataframes_list.append(hourly_measures_file)

    try:
        daily_readings_df = pd.concat(measures_dataframes_list)
    
    except ValueError as e:
        return pd.DataFrame(columns=['unitTime', 'ecuEngineOdometer', 'deviceId', 'ignitionStatus', 'unitId', 'serverTime', 'rawMessage'])

    return daily_readings_df



def get_latest_device_measures_with_success(today_date, number_of_days):
    """
    Returns all measures of devices in the latest date of data uploaded with success.
    :param today_date: today's date.
    :return: Dataframe with measures of all devices from previous 7 days
    """
    return_dict = {}
    
    date_to_check = today_date
    for i in range(0,5):
        latest_date = get_latest_date_with_success_from_s3(date_to_check)
        measures_dataframes_list = []
        for obj in bucket.objects.filter(Prefix=str(latest_date)):
            file_path = obj.key
            split_path = file_path.split('/')
        
            if len(split_path) == 3:
                date = split_path[0]
                hour = split_path[1]
                file_name = split_path[2]
                if file_name.split('_')[0] == "daily":
                    hourly_measures_file = load_csv_file_from_s3_to_df(file_name, f'{date}/{hour}/{file_name}')
                    if hourly_measures_file is not None:
                        measures_dataframes_list.append(hourly_measures_file)
        
        try:
            daily_readings_df = pd.concat(measures_dataframes_list)

        except ValueError as e: 
            return pd.DataFrame(columns=['unitTime', 'ecuEngineOdometer', 'deviceId', 'ignitionStatus', 'unitId', 'serverTime', 'rawMessage'])

        date_key = date_to_check.strftime("%Y-%m-%d")
        return_dict[date_key] = daily_readings_df
        date_to_check = latest_date - timedelta(days=1)

    return return_dict
   

def get_model_classifications_with_success_by_date(today_date):
    """
    Returns all measures of devices in the latest date of data uploaded with success.
    :param today_date: today's date.
    :return: Dataframe with measures of all devices from previous 7 days
    """
    return_dict = {}
    models_dataframes_list = []
    latest_date = str(get_latest_date_with_success_from_s3(today_date))
    i = 0
    for obj in bucket.objects.filter(Prefix=latest_date):
        return_dict[i] = obj.key
        i = i +1
        file_path = obj.key
        split_path = file_path.split('/')
        return_dict[i] = split_path
        i = i + 1
        
        if len(split_path) == 3:
            date = split_path[0]
            hour = split_path[1]
            file_name = split_path[2]
            if file_name.split('_')[0] == "model":
                hourly_models_file = load_csv_file_from_s3_to_df(file_name, f'{date}/{hour}/{file_name}')
                if hourly_models_file is not None:
                    models_dataframes_list.append(hourly_models_file)

    try:
        model_classification_df = pd.concat(models_dataframes_list)

    except ValueError as e:
        return pd.DataFrame()

    return model_classification_df.drop_duplicates()


def get_latest_model_classifications_with_success(today_date, number_of_days):
    """
    Returns all measures of devices in the latest date of data uploaded with success.
    :param today_date: today's date.
    :return: Dataframe with measures of all devices from previous 7 days
    """
    return_dict = {}
    
    date_to_check = today_date
    for i in range(0,5):
        latest_date = get_latest_date_with_success_from_s3(date_to_check)
        measures_dataframes_list = []
        for obj in bucket.objects.filter(Prefix=str(latest_date)):
            file_path = obj.key
            split_path = file_path.split('/')
        
            if len(split_path) == 3:
                date = split_path[0]
                hour = split_path[1]
                file_name = split_path[2]
                if file_name.split('_')[0] == "model":
                    hourly_models_file = load_csv_file_from_s3_to_df(file_name, f'{date}/{hour}/{file_name}')
                    if hourly_models_file is not None:
                        measures_dataframes_list.append(hourly_models_file)
        
        try:
            daily_readings_df = pd.concat(measures_dataframes_list)
        
        except ValueError as e:
            daily_readings_df =  pd.DataFrame()


        date_key = date_to_check.strftime("%Y-%m-%d")
        return_dict[date_key] = daily_readings_df
        date_to_check = latest_date - timedelta(days=1)

    return return_dict


def get_external_classifications_with_success_by_date(date):
    
    try:
    
        file_name = f'external_classifications_{date}.csv'
        external_classifications  = load_csv_file_from_s3_to_df(file_name, f'{date}/{file_name}')

    except Exception as e:
        
        external_classifications = pd.DataFrame()

    return external_classifications


def get_latest_external_classifications_with_success(today_date, number_of_days):
    """
    Returns all measures of devices in the latest date of data uploaded with success.
    :param today_date: today's date.
    :return: Dataframe with measures of all devices from previous 7 days
    """
    return_dict = {}
    
    date_to_check = today_date
    for i in range(0,5):
        latest_date = get_latest_date_with_success_from_s3(today_date)
        external_classifications =  get_external_classifications_with_success_by_date(date_to_check)
        date_key = latest_date.strftime("%Y-%m-%d")
        return_dict[date_key] = external_classifications
        date_to_check = latest_date - timedelta(days=1)

    return return_dict


def get_latest_date_with_success_from_s3(today_date):
    """
    :param today_date: today's date.
    :return: latest date of data uploaded to s3 with success.
    """
    success_exists = False
    try:
        latest_date = datetime.strptime(today_date, "%Y-%m-%d")
    except Exception as e:
        latest_date = today_date
    while not success_exists:
        success_file_name = f'{latest_date}/success_{latest_date}.txt'
        bucket = s3.Bucket(bucket_name)
        objects_in_s3 = list(bucket.objects.filter(Prefix=success_file_name))
        if any(objects_in_s3):
            success_exists = True
        else:
            latest_date = latest_date- timedelta(days=1)
    return latest_date


def load_csv_file_from_s3_to_df(local_file_name, s3_key):
    """
    Loads the indicated csv file from s3 to a pandas dataframe.
    :param local_file_name: the local name of the file to be downloaded.
    :param s3_key: the s3 key in which the file can be found in s3.
    :return: pandas dataframe with the csv file loaded.
    """
    try:
        logging.info(f"Downloading {local_file_name}")
        s3.meta.client.download_file(bucket_name, s3_key, local_file_name)
        logging.info(f"{local_file_name} exists = {os.path.isfile(local_file_name)}")
        dataframe_loaded = pd.read_csv(local_file_name, index_col=0) 
        os.remove(local_file_name)
    except Exception as e:
        logging.warning(e)
        return None
    
    return dataframe_loaded


def upload_df_to_s3(df_to_upload, file_name):
    """
    Uploads dataframe to bucket in s3, and waits until the file is uploaded.
    :param df_to_upload: df to upload to s3 in csv form.
    :param file_name: local name of the csv file.
    """
    #latest_date = get_latest_date_with_success_from_s3(date.today())
    latest_date = date.today()
    s3_key = f'{latest_date}/{file_name}_{latest_date}.csv'
    df_to_upload.to_csv(file_name)
    s3.meta.client.upload_file(file_name, bucket_name, s3_key)
    waiter = s3.meta.client.get_waiter('object_exists')
    waiter.wait(Bucket=bucket_name, Key=s3_key)
    os.remove(file_name)


current_hour = datetime.datetime.now()
current_date_data = get_latest_date_with_success_from_s3(datetime.date.today())


def should_fetch_data():
    """
    Determines whether to make the request for data to AWS, or remain with the existing cached dataframes.
    To make the request to AWS, an hour should have passed since the last refresh of cache and latest date
    with success in AWS should have changed from the latest one observed.
    """
    global current_hour
    global current_date_data

    should_request = False
    latest_date = datetime.datetime.now()
    if (latest_date - current_hour).total_seconds() > 3000:
        current_hour = latest_date
        #latest_date = get_latest_date_with_success_from_s3(datetime.date.today())
        #if current_date_data != latest_date:
        should_request = True
        current_date_data = latest_date

    return should_request
