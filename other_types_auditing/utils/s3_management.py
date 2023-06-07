import os
import datetime
import boto3
import pandas as pd

today = datetime.date.today()
hour = datetime.datetime.now().hour
minute = datetime.datetime.now().minute
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')


def upload_df_to_s3(df_to_upload, file_name):
    """
    Uploads dataframe to s3, with the file_name indicated.
    Waits until the file is uploaded.
    :param df_to_upload: df to upload to s3.
    :param file_name: name of the file to upload to s3.
    """
    bucket_name = os.environ['BUCKET_NAME']
    s3_key = f'device_readings_auditing/{today}/{hour}/{file_name}'
    df_to_upload.to_csv(file_name)
    s3.upload_file(file_name, bucket_name, s3_key)
    waiter = s3.get_waiter('object_exists')
    waiter.wait(Bucket=bucket_name, Key=s3_key)
    os.remove(file_name)


def upload_success_to_s3(file_name):
    """
    Uploads success file to s3, after both files (weekly readings and models classifications)
    have finished uploading.
    :param file_name: name of the file to upload to s3.
    """
    bucket_name = os.environ['BUCKET_NAME']
    s3_key = f'device_readings_auditing/{today}/{hour}/{file_name}'
    txt = open(file_name, 'x')
    txt.close()
    s3.upload_file(file_name, bucket_name, s3_key)
    waiter = s3.get_waiter('object_exists')
    waiter.wait(Bucket=bucket_name, Key=s3_key)
    os.remove(file_name)


def load_file_from_s3(local_file_name, s3_key):
    """
    Loads the indicated file from s3.
    :param local_file_name: the local name of the file to be downloaded.
    :param s3_key: the s3 key in which the file can be found in s3.
    """
    bucket_name = os.environ['BUCKET_NAME']
    s3_resource.meta.client.download_file(bucket_name, s3_key, local_file_name)


def load_csv_file_from_s3_to_df(local_file_name, s3_key):
    """
    Loads the indicated csv file from s3 to a pandas dataframe.
    :param local_file_name: the local name of the file to be downloaded.
    :param s3_key: the s3 key in which the file can be found in s3.
    :return: pandas dataframe with the csv file loaded.
    """
    bucket_name = os.environ['BUCKET_NAME']
    s3_resource.meta.client.download_file(bucket_name, s3_key, local_file_name)
    dataframe_loaded = pd.read_csv(local_file_name, index_col=0)
    os.remove(local_file_name)

    return dataframe_loaded


def upload_df_to_s3_as_parquet(df_to_upload, file_name, file_type):
    """
    Uploads dataframe to s3, with the file_name indicated.
    Waits until the file is uploaded.
    :param bucket_name:
    :param df_to_upload: df to upload to s3.
    :param file_name: name of the file to upload to s3.
    :param file_date:
    :param file_hour:
    """
    bucket_name = os.environ['BUCKET_NAME']
    s3_key = f'device_readings_auditing/other_types_auditing/{file_type}/{today}/{hour}/{minute}/{file_name}'
    df_to_upload.to_parquet(file_name, index=False)
    s3.upload_file(file_name, bucket_name, s3_key)
    waiter = s3.get_waiter('object_exists')
    waiter.wait(Bucket=bucket_name, Key=s3_key)
    os.remove(file_name)
