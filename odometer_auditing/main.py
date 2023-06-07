import datetime
import pandas as pd
import requests
import time
import logging
import os

from utils.db_query import query_db 
from utils.s3_management import upload_file


def get_measures():
    
    incorrect_rows_file_name = "incorrect_odometer.csv"
    data_columns = ['accountId', 'unitId', 'rowId', 'unitTime', 'engineOdometer', 'unitType', 'eventCode']
    odometer_query = f"select accountId, unitId, rowId, unitTime, engineOdometer, unitType, eventCode " \
                     f"from device_readings " \
                     f"where unitTime>='{date_lower_bound}' and unitTime<'{date_higher_bound}' and unitType != 'Queclink' and (engineOdometer is NULL or engineOdometer = 0 or engineOdometer < 0) " \
                     f"order by unitTime"

    data = query_db(incorrect_rows_file_name, data_columns, odometer_query)
    data['unitTime'] = pd.to_datetime(data['unitTime'], utc=False)

    return data


def partial_data():
    last_hour_measures_df = get_measures()
    log_in_console(last_hour_measures_df)

    
    measures_grouped_by_type_and_event_df = last_hour_measures_df.groupby(['unitType','eventCode'])
    data_list = []
    date = date_lower_bound.strftime("%Y-%m-%d")
    initial_hour = date_lower_bound.strftime("%H")
    final_hour = date_higher_bound.strftime("%H")
    hour_period = f"{initial_hour}-{final_hour} hs"

    for group_name , group_df in measures_grouped_by_type_and_event_df:
        unitType = group_name[0]
        eventCode = group_name[1]
        count = group_df.shape[0]

        data_list.append([date, hour_period, str(unitType), str(eventCode), str(count)]) 

    return data_list


def log_in_console(hourly_measures_df):
    missing_intervals = []
    for index, row in hourly_measures_df.iterrows():
        missing_interval = {
            'USN': str(row["unitId"]), 'Starting row ID': str(row["rowId"]), 'Finishing row ID': str(row["rowId"])}
        missing_intervals.append(missing_interval) 
    logging.warning("The number of intervals was: " + str(len(missing_intervals)))

    


if __name__ == '__main__':
    logging.warning("Start auditing process")
    logging.warning("   ")
    initial_date = datetime.datetime.fromisoformat(os.environ['INITIAL_DATE'])
    final_date = datetime.datetime.fromisoformat(os.environ['FINAL_DATE'])
    date_higher_bound = final_date
    date_lower_bound = final_date - datetime.timedelta(hours=1) 
    total_data = []
    while date_lower_bound >= initial_date:
        start_time = time.time()    
        
        hourly_data = partial_data()
        total_data.extend(hourly_data)

        logging.warning('The following time interval was audited: ' + 
                        str(date_lower_bound) + " to " + str(date_higher_bound))

        date_higher_bound = date_higher_bound - datetime.timedelta(hours=1)
        date_lower_bound = date_higher_bound - datetime.timedelta(hours=1)
        end_time = time.time()
        logging.warning("It took " + str(end_time - start_time) + " seconds to finish")
        logging.warning("     ")

    print(f"total data: {total_data}")
    output_dataframe = pd.DataFrame(total_data, columns=["date", "hour", "unitType", "eventCode", "count"])
    output_dataframe.to_csv("test.csv", index = False) 
    upload_file("test.csv")

    logging.info("Finished auditing odometer field")
    
