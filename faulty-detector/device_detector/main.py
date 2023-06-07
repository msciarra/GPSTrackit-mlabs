import datetime
import pandas as pd
import logging

from model.faulty_device_detector import FaultyDeviceDetector
from domain.weekly_device_readings import WeeklyDeviceReadings
from utils.s3_management import upload_success_to_s3, upload_df_to_s3, upload_external_classifications_to_s3
from utils.db_query import query_db
from utils.cloudwatch_metrics import push_all_model_metrics


def run_model():
    """
    Returns a dataframe with the classification by all models for each device, and its overall device status.
    """
    devices_classified = []

    for device in measures_df['deviceId'].unique():
        df_device = measures_df[measures_df['deviceId'] == device]
        weekly_readings = WeeklyDeviceReadings(df_device)

        detector_model = FaultyDeviceDetector()
        device_status = detector_model.identify_faulty(weekly_readings)
        devices_classified.append(device_status)

    classifications = pd.DataFrame([s.to_dict() for s in devices_classified])
    # push_all_model_metrics(classifications)
    return classifications


def get_measures():
    """
    Retrieves the dataframes for the measures of each device, and the set of devices reporting
    measures, both in the previous week, from the db.
    :return: a dataframe with the measures, and another dataframe with those devices actively reporting.
    """
   
    day_ago_date = (datetime.datetime.now() - datetime.timedelta(hours=3)).date()
    initial_query_time = initial_time
    final_query_time = final_time
    daily_readings_file_name = 'daily_readings.csv'
    daily_readings_columns = ['unitTime', 'ecuEngineOdometer', 'deviceId', 'ignitionStatus', 'unitId', 'serverTime', 'rawMessage']
    daily_readings_query = "select unitTime, ecuEngineOdometer, deviceId, ignitionStatus, unitId, serverTime, rawMessage " \
                            f"from device_readings where unitTime >= '{initial_query_time}' and unitTime < '{final_query_time}' " \
                            "order by unitTime desc"
    measures = query_db(daily_readings_file_name, daily_readings_columns, daily_readings_query)
    logging.warning("Measures queried succesfully")
    measures = measures.sort_values('unitTime', ascending=True)
    formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]
    measures['unitTime'] = pd.to_datetime(measures['unitTime'], format="mixed", utc=False)

    return measures


def format_hour(hour: int) -> str:
    formatted_hour = str(hour)
    if hour < 10:
        formatted_hour = '0' + str(hour)
    
    return formatted_hour


if __name__ == '__main__':
    process_time = datetime.datetime.now()
    initial_time = (process_time - datetime.timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
    logging.warning(f"initial time: {initial_time}")
    today = datetime.date.today()
    
    final_time = initial_time + datetime.timedelta(hours=1)
    logging.warning(f"final time: {final_time}")
      
    initial_hour = format_hour(initial_time.hour)
    final_hour = format_hour(final_time.hour)
    logging.warning(f"Start processing {initial_hour}-{final_hour} at {process_time}")

    s3_measures_df = get_measures()
    measures_df = s3_measures_df.drop('rawMessage', axis=1)
    logging.warning("Measures collected")
    logging.warning("Running model")
    models_classifications = run_model()
    
    
    if initial_hour == '23' or initial_hour == '22':
        today = today - datetime.timedelta(days=1)
    
    s3_measures_df = s3_measures_df.loc[(s3_measures_df['unitTime'] >= initial_time) & (s3_measures_df['unitTime'] < final_time)]
    logging.warning("Start uploading files to s3")
    upload_df_to_s3(models_classifications, f'model_classifications_{today}_{initial_hour}.csv', initial_hour)
    upload_df_to_s3(s3_measures_df, f'daily_readings_{today}.csv', initial_hour)

    upload_external_classifications_to_s3(pd.DataFrame(columns=['deviceId', 'date', 'faulty', 'comment']),
         f'external_classifications_{today}.csv')

    upload_success_to_s3(f'success_{today}.txt')
    logging.warning("Files uploaded successfully")


