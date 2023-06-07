import copy

from model.faulty_device_detector_interface import FaultyDeviceDetectorInterface
from domain.device_status import DeviceStatus
from model.stat_low_model import StatLowModel
from model.anomaly_model import AnomalyModel
from model.stat_high_model import StatHighModel
from model.server_time_model import ServerTimeModel


class FaultyDeviceDetector(FaultyDeviceDetectorInterface):
    """
    This class represents the FaultyDeviceDetector model as a whole, using the 3 models described below
    to classify a device as faulty or not.
    """
    def identify_faulty(self, weekly_device_readings):
        """
        This function classifies if a device is faulty or not based on the 3 models. If at least two of the three
        models classifies the device as faulty, then the device will be classified as faulty. If only one model
        (or none) classify the device as faulty, then the device is considered to be working fine.

        @param weekly_device_readings: Readings of a certain device from previous week.
        @return: returns a DeviceStatus object containing the device id, unit id, classification of each model,
        server time failures if applies, and its overall classification.

        """
        weekly_readings_device_stat_low_model = weekly_device_readings
        weekly_readings_device_stat_high_model = copy.copy(weekly_device_readings)
        weekly_readings_device_anomaly_model = copy.copy(weekly_device_readings)
        weekly_device_readings_server_time_model = copy.copy(weekly_device_readings)

        stat_low_status = StatLowModel().classify_faulty(weekly_readings_device_stat_low_model)
        stat_high_status = StatHighModel().classify_faulty(weekly_readings_device_stat_high_model)
        anomaly_status = AnomalyModel().classify_faulty(weekly_readings_device_anomaly_model)
        late_server_time_status, server_time_failures = ServerTimeModel().\
            classify_faulty(weekly_device_readings_server_time_model)

        device_status = DeviceStatus()
        device_status.device_id = weekly_device_readings.return_device_id()
        device_status.unit_id = weekly_device_readings.return_unit_id()
        device_status.server_time_failures = server_time_failures
        device_status.late_server_time = late_server_time_status
        device_status.anomaly_status = anomaly_status
        device_status.stat_high_status = stat_high_status
        device_status.stat_low_status = stat_low_status
        device_status.is_faulty = at_least_two(stat_low_status, stat_high_status, anomaly_status)
        return device_status


def at_least_two(stat_low_status, stat_high_status, anomaly_status):
    """
    Returns true if at least two of the three model predictions are faulty.
    :param stat_low_status: status of the device predicted by StatLow model.
    :param stat_high_status: status of the device predicted by StatHigh model.
    :param anomaly_status: status of the device predicted by Anomaly model.
    """
    return (stat_low_status and stat_high_status) or (stat_high_status and anomaly_status) or \
        (anomaly_status and stat_low_status)

