from model.model_interface import ModelInterface


class StatLowModel(ModelInterface):
    """
    This class consists of a model which detects decreases in odometer in a device measure history,
    and uses this information to classify the device status.
    """
    DOWN_THRESHOLD = 2
    IGNITION_STAT_THRESHOLD = 15
    MIN_IG_LIST_SIZE = 5

    def classify_faulty(self, weekly_device_readings):
        """
        This function classifies if a device is faulty or not. The criteria analyzes those cases when
        the odometer measure decreases and certain conditions are true. The decrease has to be greater than
        the "down_threshold". The second condition that needs to be considered, is that if in the last
        "ignition_stat_threshold" measures of the odometer there is at least one measure that was registered
        with ignition status off, then the device cannot be classified as faulty.

        @param weekly_device_readings: Readings of a certain device from previous week.
        @return: boolean containing the prediction of faultiness for the device.
        """

        daily_readings_odometer, daily_readings_unit_time, daily_readings_ignition = \
            weekly_device_readings.return_last_day_measures()

        for index in range(len(daily_readings_odometer)):
            if index < (len(daily_readings_odometer) - 1):
                if odometer_decreased_with_significance(daily_readings_odometer[index], daily_readings_odometer[index+1]):
                    has_off = has_off_ignitions_in_previous_values(index + 1, daily_readings_ignition)
                    if not has_off:
                        return True
        return False


def odometer_decreased_with_significance(first_value_od, second_value_od):
    """
    This function evaluates if the decrease in odometer is big enough to be considered faulty.
    @param first_value_od: Previous odometer measure.
    @param second_value_od: Current odometer measured being analyzed.
    @return: Returns True if decrease is considered faulty.
    """
    return first_value_od > second_value_od + StatLowModel.DOWN_THRESHOLD


def has_off_ignitions_in_previous_values(position, list_previous_ignitions):
    """
    This function checks if there are any ignition status off in any of the previous
    "ignition_stat_threshold"  measures.
    @param position: position of the list from which the checking is done.
    @param list_previous_ignitions: list of ignition status for the device.
    @return: True if there is any ignition status "off" in previous "ignition_stat_threshold" measures.

    """
    has_off = True
    if len(list_previous_ignitions[0:position]) > StatLowModel.MIN_IG_LIST_SIZE:
        has_off = list_previous_ignitions[position] == 'off'
        for i in range(1, StatLowModel.IGNITION_STAT_THRESHOLD):
            if position - i > 0:
                if list_previous_ignitions[position - i] == 'off':
                    has_off = True
    return has_off
