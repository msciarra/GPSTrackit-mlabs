
import os
import boto3

s3_client = boto3.client('s3')


def upload_file(file_name: str):
    
    bucket = os.environ['BUCKET_NAME']
    file_to_path = f"device_readings_auditing/odometer/{file_name}"

    s3_client.upload_file(file_name, bucket, file_to_path)

    os.remove(file_name)
    
    
    
