
class DeviceStatus:
    """
    This class represents the prediction of status for a device (faulty or not faulty).
    """
    def __init__(self):
        self.device_id = None
        self.unit_id = None
        self.stat_high_status = False
        self.stat_low_status = False
        self.anomaly_status = False
        self.late_server_time = False
        self.server_time_failures = []
        self.is_faulty = False

    def to_dict(self):
        """
        :return: dict of the object with its attributes.
        """
        return {
            'deviceId': self.device_id,
            'unitId': self.unit_id,
            'statLowStatus': self.stat_low_status,
            'statHighStatus': self.stat_high_status,
            'anomalyStatus': self.anomaly_status,
            'lateServerTime': self.late_server_time,
            'serverTimeFailures': self.server_time_failures,
            'deviceStatus': self.is_faulty
        }


