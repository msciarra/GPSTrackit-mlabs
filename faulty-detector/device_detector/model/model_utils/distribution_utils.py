import numpy as np
from scipy.stats import gamma

from model.model_utils.measure_utils import odometer_increased, calculate_time_difference_in_hours, \
    update_list_of_previous_ignitions


def calculate_normal_rate(weekly_device_readings, stat_high_model):
    """
    This function calculates and returns the speed normal rate for a device by constructing the speeds distribution
    for the device.
    :param stat_high_model: the Statistical High model which was instantiated to analyse the device in question.
    :param weekly_device_readings: the weekly readings for the device.
    """
    speeds_vector = speeds_distribution(weekly_device_readings, stat_high_model, stat_high_model.MIN_IG_LIST_SIZE)

    if len(speeds_vector) > 1:
        rate = return_gamma_percentile(speeds_vector, stat_high_model)

        if gamma_wrong_fit(rate, speeds_vector, stat_high_model):
            rate = stat_high_model.THRESHOLD_HIGH
    else:
        rate = stat_high_model.THRESHOLD_HIGH

    return rate


def return_gamma_percentile(speeds_vector, stat_high_model):
    """
    Returns the speed value of the speeds distribution percentile indicated (StatHighModel.threshold_prob threshold).
    :param speeds_vector: the Statistical High model which was instantiated to analyse the device in question.
    :param stat_high_model:
    """
    mean = np.mean(speeds_vector)
    sdev = np.std(speeds_vector)

    shape = (mean / sdev) ** 2
    scale = sdev ** 2 / mean

    prob_function = gamma(a=shape, scale=scale)
    rate = prob_function.isf(stat_high_model.THRESHOLD_PROB)

    return rate


def speeds_distribution(weekly_device_readings, stat_high_model, index_start):
    """
    This function returns a speeds vector with all the speeds registered by a device in its series.
    :param index_start: the starting index in the device series to start considering speeds.
    :param stat_high_model: the StatHighModel instantiated to analyse the device in question.
    :param weekly_device_readings: weekly readings registered by a device.
    :return: speeds vector with all speeds registered by a device.
    If an exception occurs due to no measures registered, [] is returned.
    If an exception occurs due to few measures, the speeds distribution starting in index 0 is returned.
    """
    list_odometers, list_times, list_ignitions = weekly_device_readings.return_distribution_measures()
    gradients = []

    try:
        min_time = list_times[index_start]
        stat_high_model.last_value_time = min_time
        stat_high_model.last_value_odometer = list_odometers[index_start]

        for index in range(index_start, len(list_odometers)):
            if not is_last_position(index, list_odometers) and odometer_increased(list_odometers[index],
                                                                                  list_odometers[index + 1]):
                diff_odometer = list_odometers[index + 1] - list_odometers[index]
                diff_time_hours = calculate_time_difference_in_hours(min_time, list_times[index + 1])

                if diff_time_hours > 0:
                    gradient = diff_odometer / diff_time_hours
                    if not (is_a_big_gradient_in_short_time(diff_odometer, gradient, stat_high_model)) and not vehicle_is_off(index, list_ignitions):
                        has_off = has_off_ignitions_in_previous_measures(index, list_ignitions, stat_high_model)
                        if not has_off:
                            gradients.append(diff_odometer / diff_time_hours)

                min_time = list_times[index + 1]
                stat_high_model.last_value_odometer = list_odometers[index + 1]
                stat_high_model.last_value_time = min_time

            update_list_of_previous_ignitions(list_ignitions[index], stat_high_model)

        return gradients
    except:
        if len(list_odometers) == 0:
            return []
        else:
            return speeds_distribution(weekly_device_readings, stat_high_model, 0)


def vehicle_is_off(index, list_ignitions):
    """
    :param index: index in the list of ignitions.
    :param list_ignitions: list of ignition measures registered by a device.
    :return: returns true if the ignition status of the vehicle is off in the index measure or in the next,
    false otherwise.
    """
    return list_ignitions[index] == "off" or list_ignitions[index + 1] == "off"


def has_off_ignitions_in_previous_measures(index, list_ignitions, stat_high_model):
    """
    :param index: index in the list of ignitions.
    :param list_ignitions: list of ignition measures registered by a device.
    :param stat_high_model: the model which was instantiated to analyse the device in question.
    :return: returns true if the device registered "off" ignitions in the previous two measures, false otherwise.
    """
    has_off = False
    for i in range(1, stat_high_model.IGNITION_STAT_THRESHOLD_DISTRIBUTION):
        if index - i >= 0:
            if list_ignitions[index - i] == 'off':
                has_off = True
    return has_off


def is_last_position(index, list_odometers):
    """
    :param index: index in the list of odometer measures.
    :param list_odometers: list of odometer measures registered by a device.
    :return: returns true if the index is the last position in the list of odometers, false otherwise.
    """
    return index == len(list_odometers) - 1


def is_a_big_gradient_in_short_time(diff_odometer, gradient, stat_high_model):
    """
    :param diff_odometer: the difference between two measures of odometer.
    :param gradient: speed registered in this difference (speed = diff_odometer/diff_time).
    :param stat_high_model: the Statistical High model which was instantiated to analyse the device in question.
    :return: returns true if the speed is high due and this is due to a short period of time and not due to
    a huge difference in odometer.
    """
    return diff_odometer < stat_high_model.THRESHOLD_ODOMETER and gradient > stat_high_model.THRESHOLD_HIGH


def gamma_wrong_fit(rate, speeds_vector, stat_high_model):
    """
    Returns True if the gamma had a wrong fit due to only a few measures registered by a device, False otherwise.
    Wrong fit: gamma presenting a high rate, when there is actually no speed higher than that rate.
    :param rate: rate percentile of gamma distribution.
    :param speeds_vector: vector with the speeds registered by the device.
    :param stat_high_model: the model which was instantiated to analyse the device in question.
    :return: True if the gamma had a wrong fit due to only a few measures registered by a device, False otherwise.
    """
    return len(speeds_vector) < stat_high_model.MIN_GRADIENT_VECTOR_SIZE and not (any(x > rate for x in speeds_vector))
