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
ENV ONLY_METAMODEL ''
ENV REGION=us-east-1
ENV NAMESPACE ''
ENV MISSING_ROWS ''
ENV INITIAL_DATE ''
ENV FINAL_DATE ''
ENV CONNECTIONS '1000'
ENV CLEANUP=True

ADD https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem /tmp
run chmod +x ./excecute_past_auditing.sh

ENTRYPOINT ["python3", "main.py"]
CMD ["python3", "./main.py"]
