from domain.device_status import DeviceStatus


def return_non_reporting_status(device):
    """
    :param device: the device to which return the status of.
    :return: returns a device status corresponding to a non-reporting device.
    """
    device_status = DeviceStatus()
    device_status.device_id = device
    device_status.non_reporting_status = True
    return device_status


def run_non_reporting_model(active_devices, reported_devices):
    """
    Returns all active devices which haven't reported any measures in the previous week.
    :param active_devices: list of all active devices.
    :param reported_devices: list of devices which have reported measures in the previous week.
    """
    non_reported_devices = [device for device in active_devices if not (device in reported_devices)]
    non_report_devices_status = [return_non_reporting_status(device) for device in non_reported_devices]
    return non_report_devices_status
