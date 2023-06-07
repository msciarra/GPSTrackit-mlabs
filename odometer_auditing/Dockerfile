FROM python:3.7

COPY . .

RUN python3 -m pip install --upgrade pip
RUN pip3 install -r requirements.txt

ENV AWS_ACCESS_KEY_ID ''
ENV AWS_SECRET_ACCESS_KEY ''
ENV BUCKET_NAME=proactive-data-analytics
ENV GPS_DB_HOST=prod-master-readings.cluster-ro-csenymje0qku.us-east-1.rds.amazonaws.com
ENV GPS_DB_USER=analytics_user
ENV GPS_DB_NAME=fleet_db
ENV GPS_DB_PORT=3306
ENV ONLY_METAMODEL=''
ENV NAMESPACE=''
ENV REGION=us-east-1
ENV FINAL_DATE ''
ENV INITIAL_DATE ''

ADD https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem /tmp

ENTRYPOINT ["python3", "main.py"]
CMD ["python3", "./main.py"]
