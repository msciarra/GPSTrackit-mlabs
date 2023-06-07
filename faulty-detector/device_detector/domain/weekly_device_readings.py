import datetime
import pandas as pd
from datetime import timedelta

pd.options.mode.chained_assignment = None


def center_in_origin(measure, first_odometer_measure):
    """
    This function centers an odometer value in the origin, by subtracting the
    first odometer measure to the measure.
    :param measure: measure to be centered.
    :param first_odometer_measure: the first odometer measure registered by the device
    in the weekly device readings time series.
    :return: the subtraction of first_odometer_measure to the measure.
    """
    return measure - first_odometer_measure


class WeeklyDeviceReadings:
    """
    This class contains the weekly odometer time series for a specific device. The series is
    stored as a dataframe, containing columns ['deviceId', 'unitTime', 'ecuEngineOdometer', 'ignitionStatus',
    'serverTime'].
    """
    ODOMETER_PROPERTY_NAME = 'ecuEngineOdometer'
    UNIT_TIME_PROPERTY_NAME = 'unitTime'
    IGNITION_STATUS_PROPERTY_NAME = 'ignitionStatus'
    SERVER_TIME_PROPERTY_NAME = 'serverTime'
    DEVICE_ID_PROPERTY_NAME = 'deviceId'
    UNIT_ID_PROPERTY_NAME = 'unitId'

    def __init__(self, df_device_readings):
        self.weekly_readings = df_device_readings.sort_values(self.UNIT_TIME_PROPERTY_NAME, ascending=True)

    def return_device_id(self):
        """
        :return: returns the device id of the device.
        """
        return self.weekly_readings[self.DEVICE_ID_PROPERTY_NAME].iloc[0]

    def return_unit_id(self):
        """
        :return: returns the unit id of the device.
        """
        return self.weekly_readings[self.UNIT_ID_PROPERTY_NAME].iloc[0]

    def restrict_today(self):
        """
        Restricts measures to those registered in the last 24 hours.
        """
        self.transform_to_times_format()
        time_24_hours_before_now = datetime.datetime.now() - timedelta(days=1)
        self.weekly_readings = self.weekly_readings[
            self.weekly_readings[self.UNIT_TIME_PROPERTY_NAME] >= time_24_hours_before_now]

    def return_first_odometer_measure(self):
        """
        :return: Returns the first odometer measure in weekly_readings dataframe time series.
        """
        return self.weekly_readings[self.ODOMETER_PROPERTY_NAME].head(1).iloc[0]

    def transform_to_times_format(self):
        """
        Transforms the 'unitTime' column in weekly_readings dataframe to datetime format.
        """
        self.weekly_readings[self.UNIT_TIME_PROPERTY_NAME] = pd.to_datetime(
            self.weekly_readings[self.UNIT_TIME_PROPERTY_NAME], utc=False)

    def center_odometers_in_origin(self):
        """
        Applies centering to odometers values in weekly_readings dataframe, using center_in_origin function.
        """
        first_odometer_measure = self.return_first_odometer_measure()
        self.weekly_readings[self.ODOMETER_PROPERTY_NAME] = self.weekly_readings[self.ODOMETER_PROPERTY_NAME] \
            .apply(lambda odometer_measure: center_in_origin(odometer_measure, first_odometer_measure))

    def drop_time_duplicate_measures(self):
        """
        Drops all measures with same unitTime, keeping the last measure of each duplicates.
        """
        self.weekly_readings = self.weekly_readings.drop_duplicates(subset=self.UNIT_TIME_PROPERTY_NAME, keep="last")

    def set_time_as_index(self):
        """
        Sets the time of the measure as the index of the dataframe.
        """
        self.weekly_readings = self.weekly_readings.set_index(self.UNIT_TIME_PROPERTY_NAME)

    def return_evenly_spaced_odometer_series(self):
        """
        :return: evenly spaced (10 sec. frequency) odometer series, with time as index.
        """
        self.transform_to_times_format()
        self.drop_time_duplicate_measures()
        odometer_series = self.weekly_readings.set_index(self.UNIT_TIME_PROPERTY_NAME)
        odometer_series = odometer_series[self.ODOMETER_PROPERTY_NAME]
        odometer_series = odometer_series.squeeze()
        return odometer_series.asfreq('10S', method='pad')

    def return_distribution_measures(self):
        """
        This function returns the three lists (odometers, unit times and ignition status) with the
        measures registered in the previous days to the last day of the weekly readings.
        :return: list of odometers, unitTimes and ignitionStatus registered the previous days to the last day
        of the weekly readings.
        """
        self.transform_to_times_format()
        time_24_hours_before_now = datetime.datetime.now() - timedelta(days=1)
        for_distribution = self.weekly_readings[
            self.weekly_readings[self.UNIT_TIME_PROPERTY_NAME] < time_24_hours_before_now]

        return for_distribution[self.ODOMETER_PROPERTY_NAME].tolist(), \
               [str(i) for i in for_distribution[self.UNIT_TIME_PROPERTY_NAME].tolist()], \
               for_distribution[self.IGNITION_STATUS_PROPERTY_NAME].tolist()

    def return_last_day_measures(self):
        """
        This function returns the three lists (odometers, unit times and ignition status) with the
        measures registered the last day of the weekly readings.
        :return: list of odometers, unitTimes and ignitionStatus registered the last day of the weekly readings.
        """
        self.transform_to_times_format()
        time_24_hours_before_now = datetime.datetime.now() - timedelta(days=1)
        last_day = self.weekly_readings[self.weekly_readings[self.UNIT_TIME_PROPERTY_NAME] >= time_24_hours_before_now]

        return last_day[self.ODOMETER_PROPERTY_NAME].tolist(), \
               [str(i) for i in last_day[self.UNIT_TIME_PROPERTY_NAME].tolist()], \
               last_day[self.IGNITION_STATUS_PROPERTY_NAME].tolist()

    def return_unit_and_server_times(self):
        """
        This function returns the unitTime and serverTime for each event in the weekly readings.
        :return: a dataframe containing unitTime and serverTime for each event.
        """
        return self.weekly_readings[[self.UNIT_TIME_PROPERTY_NAME, self.SERVER_TIME_PROPERTY_NAME]]
