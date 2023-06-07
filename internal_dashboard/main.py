import datetime
import logging
import os

from utils.s3_management import upload_success_to_s3, upload_df_to_s3
from utils.db_query import query_db
from transformations.transformations import transform_behavior_events, fix_trip_columns, merge_data, add_industry, \
                                            eliminate_anomalies

limit_date = datetime.datetime.strptime(os.environ['LIMIT_DATE'], '%d %b %Y')


def get_trip_measures():
    """
    Retrieves a dataframe with the information of each trip done by each vehicle grouped by date.
    """
    trips_file_name = 'trip_events.csv'
    trips_columns = ['accountId', 'unitId', 'deviceId', 'date1', 'travel_time', 'idle_time',
                     'total_distance', 'avg_speed', 'max_speed', 'number_of_idles',
                     'number_of_stops', 'engine_off', 'engine_on']
    trips_query = "select accountId,unitId,deviceId,date(tripStartUnittime) as date1," \
                  "sum(travelTime) as travel_time,sum(idleTime) as idle_time,sum(distance) as total_distance," \
                  " avg(averageSpeed) as avg_speed,max(maxSpeed) as max_speed," \
                  " sum(numberOfIdles) as number_of_idles, sum(numberOfStops) as number_of_stops," \
                  " sum(engineOff) as engine_off, sum(engineOn) as engine_on" \
                  f" from trip_summary where date(tripStartUnittime)>'{limit_date}' and distance IS NOT NULL and " \
                  f"distance>=0 group by accountId,unitId,date(tripStartUnittime) " \
                  "order by date(tripStartUnittime) desc;"

    measures = query_db(trips_file_name, trips_columns, trips_query)

    return measures


def get_behavior_events():
    """
    Retrieves a dataframe with the information of each behavior event done by each vehicle grouped by date.
    """
    behavior_events_file_name = 'behavior_events.csv'
    behavior_events_columns = ['accountId', 'unitId', 'metric', 'date', 'total_events']

    metric1 = "SpeedOverPosted"
    metric2 = "ECUSpeedOverPosted"
    metric3 = "SpeedOver"
    metric4 = "ECUSpeedOver"

    metrics = (metric1, metric2, metric3, metric4)
    behavior_events_query = f"select accountId,unitId,metric,date,sum(events) as total_events " \
                            f"from behavior_events_iot where date>'{limit_date}' and metric in {metrics} " \
                            f"group by accountId,unitId,metric,date order by date desc;"

    measures = query_db(behavior_events_file_name, behavior_events_columns, behavior_events_query)

    return measures


if __name__ == '__main__':
    logging.warning('started to run')

    trip_measures_df = get_trip_measures()
    logging.warning('loaded trip measures')

    behavior_events_df = get_behavior_events()
    logging.warning('loaded behavior events')

    trip_measures_df = fix_trip_columns(trip_measures_df)
    logging.warning('fixed trip measures')

    trip_measures_df = eliminate_anomalies(trip_measures_df)
    logging.warning('eliminated anomalies')

    behavior_events_df, beh_columns = transform_behavior_events(behavior_events_df)
    logging.warning('transformed behavior events')

    global_df = merge_data(behavior_events_df, trip_measures_df, beh_columns)
    logging.warning('merged data')

    global_df = add_industry(global_df)
    logging.warning('added industry')

    today = datetime.date.today()
    upload_df_to_s3(global_df, f'global_data.csv')
    logging.warning('saved df in s3')

    upload_success_to_s3(f'success_{today}.txt')
    logging.warning('saved success in s3')
