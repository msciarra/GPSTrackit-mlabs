from dateutil import parser


def odometer_increased(first_value_od, second_value_od):
    """
    :param first_value_od: first value of odometer to be compared.
    :param second_value_od: second value of odometer to be compared.
    :return: returns true if the second value of the odometer is greater than the first value, false otherwise.
    """
    return first_value_od < second_value_od


def calculate_time_difference_in_hours(first_time, second_time):
    """
    :param first_time: first timestamp registered.
    :param second_time: second timestamp registered.
    :return: returns the time difference in hours between the two timestamps.
    """
    diff_time = parser.parse(second_time) - parser.parse(first_time)
    hours = ((diff_time.total_seconds()) / 60.0) / 60.0
    return hours


def update_list_of_previous_ignitions(ignition_status, stat_high_model):
    """
    This function updates the list of previous ignitions of a device with the new measure.
    It also deletes the first measure if the threshold for previous measures is surpassed.
    :param stat_high_model: the StatHighModel instantiated to analyse the device in question.
    :param ignition_status: the new measure of ignitions status registered by a device.
    """
    stat_high_model.last_values_ignition.insert(0, ignition_status)
    if surpassed_threshold_length(stat_high_model.last_values_ignition, stat_high_model.IGNITION_STAT_THRESHOLD):
        stat_high_model.last_values_ignition.pop()


def surpassed_threshold_length(last_values_ignition, max_amount_previous_ignitions):
    """
    :param last_values_ignition: the list of the previous ignition measures of a device.
    :return: returns true if the length of the list of the previous ignition measures of the device
    surpassed the StatHighModel.ignition_stat_threshold defined, false otherwise.
    """
    return len(last_values_ignition) == max_amount_previous_ignitions + 1
