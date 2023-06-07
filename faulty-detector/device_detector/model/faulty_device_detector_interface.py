from abc import ABC, abstractmethod


class FaultyDeviceDetectorInterface(ABC):
    """
    Defines the function to be implemented by the faulty device detector metamodel (integration of several models).
    """
    @abstractmethod
    def identify_faulty(self, weekly_device_readings):
        """
        This function classifies if a device is faulty or not based on the 3 models. If at least two of the three
        models classifies the device as faulty, then the device will be classified as faulty. If only one model
        (or none) classified the device as faulty, then the device is considered to be working fine.

        @param weekly_device_readings: Readings of a certain device from previous week.
        @return: Returns 4 DeviceStatus: 3 DeviceStatus with the classification of each of the 3 models
        (StatHigh, StatLow, Anomaly), and another DeviceStatus with the overall classification considering
        the criteria detailed above.
        """
        pass
