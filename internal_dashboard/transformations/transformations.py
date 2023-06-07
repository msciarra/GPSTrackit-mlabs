import pandas as pd
from datetime import datetime


def transform_behavior_events(dataframe):
    """
    Unstack the dataframe to obtain one row per vehicle per day with all the data.
    :param dataframe: name of the dataframe.
    :return: returns the dataframe unstacked.
    """
    df_columns = list(dataframe.metric.unique())
    df_trans = dataframe.set_index(['accountId', 'unitId', 'date', 'metric']).unstack('metric').fillna(0)
    df_trans.columns = df_trans.columns.levels[1].rename(None)
    df_trans = df_trans.reset_index()
    df_trans = df_trans.rename({'unitId': 'deviceId'}, axis=1)

    df_trans['deviceId'] = df_trans['deviceId'].astype(str)
    df_trans['accountId'] = df_trans['accountId'].astype(str)

    return df_trans, df_columns


def fix_trip_columns(dataframe):
    """
    Transforms the columns of deviceId and accountId to string.
    :param dataframe: name of the dataframe.
    :return: transformed dataframe.
    """
    dataframe['deviceId'] = dataframe['deviceId'].astype(str)
    dataframe['accountId'] = dataframe['accountId'].astype(str)
    return dataframe


def merge_data(df_behaviors, df_trips, columns):
    """
    Merges the two dataframes inputted.
    :param df_behaviors: name of the dataframe with all the behavior events.
    :param df_trips: name of the dataframe with all trips.
    :return: merged dataframe.
    """
    df_cd = pd.merge(df_behaviors, df_trips, how='outer', left_on=['deviceId', 'date', 'accountId'],
                     right_on=['deviceId', 'date1', 'accountId'])

    df_cd['date'] = pd.to_datetime(df_cd['date'])
    df_cd['date1'] = pd.to_datetime(df_cd['date1'])

    datetime_object = datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')
    df_cd['dates'] = df_cd.apply(lambda row: row['date'] if row['date'] > datetime_object else row['date1'], axis=1)
    df_cd = df_cd.drop(['date', 'date1'], 1)

    dev_dict = unique_devices_dict(df_trips)

    df_cd['unitId'] = df_cd.apply(lambda row: add_unit_id(row['deviceId'], dev_dict), axis=1)

    for column in columns:
        df_cd[column] = df_cd[column].fillna(0)

    df_cd = df_cd.rename({'IdleTime': 'number_of_idlings',
                          'StopTime': 'number_of_stops_events',
                          'HardBrake': 'hard_brake',
                          'HardTurn': 'hard_turn',
                          'RapidAcceleration': 'rapid_acceleration',
                          'SpeedOver': 'speed_over',
                          'SpeedOverPosted': 'speed_over_posted',
                          'unitId': 'unit_id',
                          'deviceId': 'device_id',
                          'EngineOffTime': 'engine_off_time',
                          'EngineOnTime': 'engine_on_time',
                          'accountId': 'account_id',
                          'ECUSpeedOver': 'ecu_speed_over',
                          'ECUSpeedOverPosted': 'ecu_speed_over_posted',
                          'SuddenStop': 'sudden_stop',
                          'TravelTime': 'travel_time'
                          }, axis=1)
    return df_cd


def unique_devices_dict(dataframe):
    """
    Creates a dictionary that has the device ids as keys and unit ids as values.
    :param dataframe: name of the dataframe.
    :return: dictionary.
    """
    df_dev = dataframe.groupby(['unitId', 'deviceId']).size().reset_index().rename(columns={0: 'count'})

    dev_dict = dict(zip(df_dev.deviceId, df_dev.unitId))
    return dev_dict


def add_unit_id(dev_id, dev_dict):
    """
    Adds the unit id to those rows that do not contain one.
    :param dev_id: device id of the row.
    :param dev_dict: dictionary to pair the device id to a unit id.
    :return: corresponding unit id.
    """
    try:
        return dev_dict[dev_id]
    except:
        return 'no unit Id'


def add_industry(dataframe):
    """
    Adds the industry for each trip according to the corresponding device that made the trip.
    :param dataframe: name of dataframe.
    :return: dataframe with new column category.
    """
    file_name = 'accountList.csv'
    df_category = pd.read_csv(file_name, sep=',', encoding='cp1252')
    id_category_df = df_category[['Account ID', 'Lead Industry Category']]

    dataframe['category'] = dataframe.apply(lambda row: define_industry(row['account_id'], id_category_df), axis=1)

    return dataframe


def define_industry(account_id, df_category_id):
    """
    Defines the industry of the corresponding device.
    :param account_id: account id of the device.
    :param df_category_id: dataframe with all the account ids and their industry.
    :return: category of the device.
    """
    try:
        category = df_category_id[df_category_id['Account ID'] == float(account_id)]['Lead Industry Category'].iloc[0]

        if isinstance(category, str):
            return category
        else:
            return 'no category'
    except:
        return 'no category'


def eliminate_anomalies(dataframe):
    """
    Eliminates from the dataframe those trips that have anomalous distances covered.
    :param dataframe: name of the dataframe.
    :return: filtered dataframe.
    """
    filter_df = dataframe['total_distance'] < 2000
    dataframe = dataframe[filter_df]
    return dataframe
