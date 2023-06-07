from model.model_interface import ModelInterface
from datetime import timedelta


class ServerTimeModel(ModelInterface):
    """
    This class consists of a model which identifies devices which presented events whose server time is more than 3
    days past the unit time.
    """
    SERVER_TIME_THRESHOLD = 3

    def classify_faulty(self, weekly_device_readings):
        """
        Classifies a device's status as faulty or not.
        :param weekly_device_readings: a copy of the object WeeklyDeviceReadings containing week time measures for the
        device being classified.
        :return: boolean containing the classification by the ServerTimeModel for the device.
        """
        weekly_device_readings.transform_to_times_format()
        unit_and_server_times_df = weekly_device_readings.return_unit_and_server_times()
        late_server_time = unit_and_server_times_df[unit_and_server_times_df['serverTime']
                                                    > unit_and_server_times_df['unitTime'] +
                                                    timedelta(days=self.SERVER_TIME_THRESHOLD)]
        is_faulty = False
        server_time_failures = []
        if len(late_server_time) > 0:
            is_faulty = True
            server_time_failures = list(zip(late_server_time['unitTime'].astype(str),
                                            late_server_time['serverTime'].astype(str)))

        return is_faulty, server_time_failures
