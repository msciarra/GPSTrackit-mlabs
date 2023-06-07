import boto3
import os

cw_client = boto3.client('cloudwatch', region_name=os.environ['REGION'])


def push_all_model_metrics(classifications):
    """
    Pushes to Cloudwatch all the metrics regarding to the running of the model.
    """
    push_metric_data('ModelClassifiedDaily', 'Count', len(classifications['deviceId'].unique()))
    push_metric_data('PercentageFaultyDaily', 'Percent',
                     100 * len(classifications[classifications['deviceStatus']]) / len(classifications))
    push_metric_data('FaultyDaily', 'Count', len(classifications[classifications['deviceStatus']]))
    push_metric_data('ModelRunDaily', 'Count', 1)


def push_metric_data(metric_name, unit, value):
    """
    Pushes metric data to Cloudwatch. This metric is given a timestamp of the current UTC time.
    :param metric_name: name of the metric.
    :param unit: unit of the metric.
    :param value: value of the metric.
    """
    namespace = os.environ['NAMESPACE']

    cw_client.put_metric_data(Namespace=namespace, MetricData=[{'MetricName': metric_name, 'Unit': unit,
                                                                'Value': value}])
