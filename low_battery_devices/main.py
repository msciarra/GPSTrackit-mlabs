import datetime
import os
import pandas as pd
import logging

from utils.db_query import query_db
from utils.ses import send_email_with_attachment, send_plain_email
from utils.sns import publish_message


def get_measures():
    """
    Retrieves the dataframes for the measures of each device, and the set of devices reporting
    measures, both in the previous week, from the db.
    :return: a dataframe with the measures, and another dataframe with those devices actively reporting.
    """
   
    final_time = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    initial_time = final_time - datetime.timedelta(days=1) 
    initial_query_time = initial_time
    final_query_time = final_time
    daily_readings_file_name = 'daily_readings.csv'
    daily_readings_columns = ['unitTime', 'deviceId', 'unitId', 'eventName', 'eventCode', 'unitType']
    daily_readings_query = "select unitTime, deviceId, unitId, eventName, eventCode, unitType " \
                            f"from device_readings where unitTime >= '{initial_query_time}' and unitTime < '{final_query_time}' and eventCode = 25 and unitType='Calamp' " \
                            "order by unitTime desc"
    measures = query_db(daily_readings_file_name, daily_readings_columns, daily_readings_query)
    logging.warning("Measures queried succesfully")

    measures = measures.sort_values('unitTime', ascending=True)
    formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]

    measures['unitTime'] = pd.to_datetime(measures['unitTime'], format="mixed", utc=False)
 
    return measures


def get_top_devices(measures_df):
 
    unit_counts = measures_df.groupby(['deviceId', 'unitId']).size().reset_index(name='count')
    unit_counts = unit_counts.sort_values('count', ascending=False)
    top_units = unit_counts.head(10)
    
    return top_units


def get_over_1000_events_devices(measures_df):
    
    unit_counts = measures_df.groupby(['deviceId', 'unitId']).size().reset_index(name='count')
    unit_counts = unit_counts.sort_values('count', ascending=False)
    selected_units_df = unit_counts[unit_counts['count'] > 1000]
    unit_list = [row['unitId'] for _, row in selected_units_df.iterrows()]

    return unit_list

def get_string_from_list(units_list):
    
    return_str = ''

    for unit in units_list:
        return_str = return_str + ', ' + str(unit)

    return return_str

if __name__ == '__main__':
   
    logging.warning(f"Start getting measures at {datetime.datetime.now()}")
    measures_df = get_measures()
    logging.warning(f"Finish getting measures at {datetime.datetime.now()}")  

    logging.warning(f"Getting top 10 devices")
    top_10_devices_df = get_top_devices(measures_df) 
    
    logging.warning(f"Sending email with top 10 devices")
    send_email_with_attachment(top_10_devices_df)
    logging.warning(f"Email sended")
    
    logging.warning(f"Getting units with +1000 low battery events") 
    over_1000_events_units = get_over_1000_events_devices(measures_df)
    
    logging.warning(f"Sending SMS with +1000 low battery events units")
    for unit in over_1000_events_units:
        publish_message(f'Unit id: {unit} !R3,31,17')

    unit_list_str = get_string_from_list(over_1000_events_units)
    send_plain_email(unit_list_str)
