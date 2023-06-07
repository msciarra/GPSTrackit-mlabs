import os
import mysql.connector
import boto3
import pandas as pd 

from utils.file_management import create_csv, read_dataframe_from_file

client = boto3.client('rds', region_name=os.environ['REGION'])


def query_db(file_name, df_columns, query):
    """
    Queries the database according to the query indicated and returns a dataframe with the results.
    :param file_name: name of the file where to momentarily store the query's data.
    :param df_columns: columns of the data.
    :param query: query to perform in the db.
    :return: returns the dataframe with the query's results.
    """
    gps_trackit_db = connect_to_db()
#    db_cursor = gps_trackit_db.cursor()

#    db_cursor.execute(query)
#    rows = db_cursor.fetchall()
#    create_csv(file_name, rows)
#    dataframe_result = read_dataframe_from_file(file_name, df_columns)
#    os.remove(file_name)
    dataframe_result = pd.read_sql(query, con=gps_trackit_db)
    print(dataframe_result.head())
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
        ssl_ca="./rds-combined-ca-bundle.pem",
        ssl_cipher="HIGH:!DH:!aNULL")

    return gps_trackit_db
