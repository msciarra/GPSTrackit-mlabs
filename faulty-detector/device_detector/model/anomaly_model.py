from adtk.data import validate_series
from adtk.transformer import ClassicSeasonalDecomposition
import math

from model.model_interface import ModelInterface


def prepare_daily_device_readings(weekly_device_readings):
    """
    Prepares the daily readings for the anomaly model, by applying several transformations.
    :param weekly_device_readings: a copy of the object WeeklyDeviceReadings containing week time measures for the
    device being classified.
    """
    weekly_device_readings.restrict_today()
    weekly_device_readings.center_odometers_in_origin()


def return_max_residual(weekly_device_readings_anomalies):
    """
    Returns max residual in weekly device readings, after applying abs. value and non NAN filters.
    :param weekly_device_readings_anomalies: List of anomalies in weekly device readings.
    :return: maximum absolute value residual.
    """
    residuals_abs_values = [abs(anomaly) for anomaly in weekly_device_readings_anomalies]
    residuals_abs_values_not_nan = [anomaly for anomaly in residuals_abs_values if not math.isnan(anomaly)]

    return max(residuals_abs_values_not_nan)


class AnomalyModel(ModelInterface):
    """
    This class consists of a model which detects anomalies in the device's odometer time series,
    and uses this criteria to classify the device's status.
    """
    THRESHOLD_SEAS = 50

    def classify_faulty(self, weekly_device_readings):
        """
        Classifies a device's status as faulty or not.
        :param weekly_device_readings: a copy of the object WeeklyDeviceReadings containing week time measures for the
        device being classified.
        :return: boolean containing the classification by the AnomalyModel for the device.
        """
        try:
            prepare_daily_device_readings(weekly_device_readings)

            evenly_spaced_odometer_series = weekly_device_readings.return_evenly_spaced_odometer_series()
            validated_series = validate_series(evenly_spaced_odometer_series)
            device_readings_anomalies = ClassicSeasonalDecomposition(freq=4, trend=True).fit_transform(validated_series)

            max_residual = return_max_residual(device_readings_anomalies)

            return max_residual > self.THRESHOLD_SEAS
        except:
            return False
