FROM python:3

COPY . .

RUN pip3 install -r requirements.txt

ENV BUCKET_NAME ''
ENV AWS_ACCESS_KEY_ID ''
ENV AWS_SECRET_ACCESS_KEY ''

ENTRYPOINT ["python3", "faulty_devices.py"]
CMD ["python3", "./faulty_devices.py"]
