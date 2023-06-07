from abc import ABC, abstractmethod


class ModelInterface(ABC):
    """
    Defines the function to be implemented by all models integrated in the faulty device detector metamodel.
    """
    @abstractmethod
    def classify_faulty(self, weekly_device_readings):
        """
        Returns the DeviceStatus prediction for a certain device, identifying whether it is faulty or not.
        :param weekly_device_readings: contains all the weekly device readings for a specific device.
        """
        pass
