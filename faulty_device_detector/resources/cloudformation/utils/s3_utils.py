import boto3
import datetime
import pandas as pd
import os

s3 = boto3.client('s3')


def create_bucket(bucket_name, region):
    """
    Creates a bucket with the name indicated and uploads empty files.
    :param region: the region in which to create the bucket.
    :param bucket_name: name of the bucket to create.
    """
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
    upload_success_to_s3(f'success_{yesterday}.txt', bucket_name, yesterday)
    upload_df_to_s3(pd.DataFrame(columns=['deviceId', 'unitId', 'statLowStatus', 'statHighStatus', 'anomalyStatus',
                                          'lateServerTime', 'serverTimeFailures', 'deviceStatus']),
                    f'model_classifications_{yesterday}.csv', bucket_name, yesterday)
    upload_df_to_s3(pd.DataFrame(columns=['unitTime', 'ecuEngineOdometer', 'deviceId',
                                          'ignitionStatus', 'unitId', 'serverTime']),
                    f'daily_readings_{yesterday}.csv', bucket_name, yesterday)
    upload_df_to_s3(pd.DataFrame(columns=['deviceId', 'date', 'faulty', 'comment']),
                    f'external_classifications_{yesterday}.csv', bucket_name, yesterday)
    upload_lstm_model_to_s3('lstm_model_2022-02-25.pth', bucket_name)
    upload_lstm_success_to_s3('success_2022-02-25.txt', bucket_name)


def upload_df_to_s3(df_to_upload, file_name, bucket_name, date):
    """
    Uploads dataframe to s3, with the file_name indicated.
    Waits until the file is uploaded.
    :param bucket_name: name of the bucket to upload to.
    :param df_to_upload: df to upload to s3.
    :param file_name: name of the file to upload to s3.
    :param date: date to which is has to be uploaded.
    """
    s3_key = f'{date}/{file_name}'
    df_to_upload.to_csv(file_name)
    s3.upload_file(file_name, bucket_name, s3_key)
    waiter = s3.get_waiter('object_exists')
    waiter.wait(Bucket=bucket_name, Key=s3_key)
    os.remove(file_name)


def upload_success_to_s3(file_name, bucket_name, date):
    """
    Uploads success file to s3, and waits until it is uploaded.
    :param bucket_name: name of the bucket to upload to.
    :param file_name: name of the file to upload to s3.
    :param date: date to which is has to be uploaded.
    """
    s3_key = f'{date}/{file_name}'
    txt = open(file_name, 'x')
    txt.close()
    s3.upload_file(file_name, bucket_name, s3_key)
    waiter = s3.get_waiter('object_exists')
    waiter.wait(Bucket=bucket_name, Key=s3_key)
    os.remove(file_name)


def upload_lstm_success_to_s3(file_name, bucket_name):
    """
    Uploads success file to s3, after the lstm_model has finished uploading.
    :param file_name: name of the file to upload to s3.
    :param bucket_name: name of the bucket.
    """
    s3_key = f'lstm-models/{file_name}'
    txt = open(file_name, 'x')
    txt.close()
    s3.upload_file(file_name, bucket_name, s3_key)
    waiter = s3.get_waiter('object_exists')
    waiter.wait(Bucket=bucket_name, Key=s3_key)
    os.remove(file_name)


def upload_lstm_model_to_s3(file_name, bucket_name):
    """
    Uploads lstm_model to s3, with the file_name indicated.
    Waits until the file is uploaded.
    :param file_name: name of the file to upload to s3.
    :param bucket_name: name of the bucket.
    """
    s3_key = f'lstm-models/{file_name}'
    s3.upload_file(file_name, bucket_name, s3_key)
    waiter = s3.get_waiter('object_exists')
    waiter.wait(Bucket=bucket_name, Key=s3_key)
