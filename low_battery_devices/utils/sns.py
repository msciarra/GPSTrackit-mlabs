import boto3

client = boto3.client('sns', region_name="us-east-1")


def publish_message(message: str):

    client.publish(
        TopicArn='arn:aws:sns:us-east-1:430192569516:low-battery-sns-topic',
        Message=message,
        Subject='Low battery device'
    )

