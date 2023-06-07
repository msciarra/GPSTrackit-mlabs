import os
import pandas as pd
import mysql.connector
import boto3

from utils.file_management import create_csv, read_dataframe_from_file

client = boto3.client('rds', region_name=os.environ['REGION'])


def query_db(file_name, df_columns, query):

    gps_trackit_db = connect_to_db()
    db_cursor = gps_trackit_db.cursor()
    
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    if (len(rows) == 0 ):
        df = pd.DataFrame(columns = df_columns)
        return df

    create_csv(file_name, rows)
    dataframe_result = read_dataframe_from_file(file_name, df_columns)
    os.remove(file_name)
        
    return dataframe_result


def connect_to_db():
   """
   Establishes an SSL connection the db indicated in the env and returns this connection.
   """
   token = client.generate_db_auth_token(DBHostname=os.environ['GPS_DB_HOST'], Port=int(os.environ['GPS_DB_PORT']),
                                         DBUsername=os.environ['GPS_DB_USER'])
   gps_trackit_db = mysql.connector.connect(
       host=os.environ['GPS_DB_HOST'],
       user=os.environ['GPS_DB_USER'],
       password=token,
       port=int(os.environ['GPS_DB_PORT']),
       database=os.environ['GPS_DB_NAME'],
       ssl_ca="/tmp/rds-combined-ca-bundle.pem",
       ssl_cipher="HIGH:!DH:!aNULL")

   return gps_trackit_db   
