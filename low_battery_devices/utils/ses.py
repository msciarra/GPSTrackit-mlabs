import boto3
import pandas as pd
import os
from email import encoders
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText



ses_client = boto3.client("ses", region_name="us-east-1")


def send_email_with_attachment(low_battery_devices_df):

    SENDER = "iotanalytics@gpstrackit.net"
    RECEIVER = "msciarra@gpstrackit.net"
    msg = MIMEMultipart()
    msg["Subject"] = "Daily low battery devices report"
    msg["From"] = SENDER
    msg["To"] = RECEIVER

    # Set message body
    body = MIMEText("Please see the attached file", "plain")
    msg.attach(body)

    filename = "top_low_battery_devices.csv"  
    low_battery_devices_df.to_csv(filename, index=False)

    with open(filename, "rb") as attachment:
        part = MIMEApplication(attachment.read())
        part.add_header("Content-Disposition",
                        "attachment",
                        filename=filename)
    msg.attach(part)

    # Convert message to string and send
    
    response = ses_client.send_raw_email(
        Source="msciarra@gpstrackit.net",
        Destinations=["msciarra@gpstrackit.net", "ghough@gpstrackit.net"], 
        RawMessage={"Data": msg.as_string()}
    )
    print(response)

    os.remove(filename)


def send_plain_email(message_str):

    CHARSET = "UTF-8"

    response = ses_client.send_email(
        Destination={
            "ToAddresses": [
                "msciarra@gpstrackit.net",
            ],
        },
        Message={
            "Body": {
                "Text": {
                    "Charset": CHARSET,
                    "Data": message_str,
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": "Units with +1000 low battery events",
            },
        },
        Source="iotanalytics@gpstrackit.net",
    )
