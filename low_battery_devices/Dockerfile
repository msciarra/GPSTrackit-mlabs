FROM python:3

COPY . .

RUN python3 -m pip install --upgrade pip
RUN pip3 install -r requirements.txt

ENV AWS_ACCESS_KEY_ID ''
ENV AWS_SECRET_ACCESS_KEY ''
ENV GPS_DB_HOST ''
ENV GPS_DB_USER ''
ENV GPS_DB_NAME ''
ENV GPS_DB_PORT ''
ENV REGION ''

ADD https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem /tmp

ENTRYPOINT ["python3", "main.py"]
CMD ["python3", "./main.py"]
