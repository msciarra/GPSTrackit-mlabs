FROM public.ecr.aws/lambda/python:3.8

COPY main.py ${LAMBDA_TASK_ROOT}
COPY . .

RUN python3 -m pip install --upgrade pip
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

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
ENV CONNECTIONS ''

ADD https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem ${LAMBDA_TASK_ROOT}

RUN chmod 777 ${LAMBDA_TASK_ROOT}
RUN chmod 777 ${LAMBDA_TASK_ROOT}/rds-combined-ca-bundle.pem
RUN chmod 777 .

CMD [ "main.lambda_handler" ] 
