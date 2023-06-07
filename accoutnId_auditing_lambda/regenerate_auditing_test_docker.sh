docker stop `docker ps -a -f "label=name" -q`
docker rm `docker ps -a -f "label=name" -q`
docker image rm `docker images auditing_test -q`
docker build --tag auditing_test .
nohup sudo docker run --label name=auditing_test -e BUCKET_NAME=proactive-data-analytics -e GPS_DB_HOST=prod-master-readings.cluster-ro-csenymje0qku.us-east-1.rds.amazonaws.com -e GPS_DB_USER=analytics_user -e GPS_DB_NAME=fleet_db -e GPS_DB_PORT=3306 -e REGION=us-east-1 auditing_test &
