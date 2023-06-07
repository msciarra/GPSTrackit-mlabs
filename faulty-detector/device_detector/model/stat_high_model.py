from model.model_interface import ModelInterface
from model.model_utils.distribution_utils import calculate_normal_rate
from model.model_utils.measure_utils import odometer_increased, update_list_of_previous_ignitions, \
    calculate_time_difference_in_hours


class StatHighModel(ModelInterface):
    """
    This class consists of a model which detects high speeds in the speed distribution of a device,
    and uses this criteria to classify the device status.
    """
    THRESHOLD_HIGH = 200
    THRESHOLD_ODOMETER = 15
    THRESHOLD_PROB = 0.01
    IGNITION_STAT_THRESHOLD = 15
    IGNITION_STAT_THRESHOLD_DISTRIBUTION = 4
    MIN_IG_LIST_SIZE = 5
    MIN_GRADIENT_VECTOR_SIZE = 10

    def __init__(self):
        self.last_value_odometer = 0
        self.last_value_time = 0
        self.last_values_ignition = []

    def classify_faulty(self, weekly_device_readings):
        """
        Classifies a device's status as faulty or not.
        :param weekly_device_readings: a copy of the object WeeklyDeviceReadings containing week time measures for the
        device being classified.
        :return: boolean with the classification for the device by StatHighModel.
        """
        weekly_device_readings.drop_time_duplicate_measures()
        device_normal_rate = calculate_normal_rate(weekly_device_readings, self)
        list_odometers, list_times, list_ignitions = weekly_device_readings.return_last_day_measures()

        if device_normal_rate > StatHighModel.THRESHOLD_HIGH:
            return True
        elif len(list_odometers) == 0:
            return False

        self.initialize_attributes_in_case_no_previous_measures_exist(list_ignitions, list_odometers, list_times)

        for index in range(len(list_odometers)):
            if odometer_increased(self.last_value_odometer, list_odometers[index]):
                diff_odometer = list_odometers[index] - self.last_value_odometer
                diff_time = calculate_time_difference_in_hours(self.last_value_time, list_times[index])
                gradient = diff_odometer / diff_time

                if not (has_off_previous_ignitions(self.last_values_ignition)) and list_ignitions[index] != 'off':
                    if gradient > device_normal_rate and diff_odometer > self.THRESHOLD_ODOMETER:
                        return True

                self.last_value_odometer = list_odometers[index]
                self.last_value_time = list_times[index]

                update_list_of_previous_ignitions(list_ignitions[index], self)

        return False

    def initialize_attributes_in_case_no_previous_measures_exist(self, list_ignitions, list_odometers,
                                                                 list_times):
        """
        This function initializes the model's attributes in case no previous measures for distribution exist.
        :param list_ignitions: ignitions registered in the previous 24 hs by the device.
        :param list_odometers: odometers registered in the previous 24 hs by the device.
        :param list_times: times registered in the previous 24 hs by the device.
        """
        if self.last_value_time == 0:
            self.last_value_time = list_times[0]
            self.last_value_odometer = list_odometers[0]
            self.last_values_ignition.append(list_ignitions[0])


def has_off_previous_ignitions(previous_ignitions):
    """
    :param previous_ignitions: list of the previous measures of ignition status for a device.
    :return: returns true if there are any "off" ignition status in the list, false otherwise.
    """
    return previous_ignitions.count('off') > 0
