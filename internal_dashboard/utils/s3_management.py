import os
import datetime
import boto3

today = datetime.date.today()
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
folder_name = 'internal_dashboard_data'


def upload_df_to_s3(df_to_upload, file_name):
    """
    Uploads dataframe to s3, with the file_name indicated.
    Waits until the file is uploaded.
    :param df_to_upload: df to upload to s3.
    :param file_name: name of the file to upload to s3.
    """
    bucket_name = os.environ['BUCKET_NAME']
    s3_key = f'{folder_name}/{file_name}'
    df_to_upload.to_csv(file_name, index=False)
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
    s3_key = f'{folder_name}/{file_name}'
    txt = open(file_name, 'x')
    txt.close()
    s3.upload_file(file_name, bucket_name, s3_key)
    waiter = s3.get_waiter('object_exists')
    waiter.wait(Bucket=bucket_name, Key=s3_key)
    os.remove(file_name)
