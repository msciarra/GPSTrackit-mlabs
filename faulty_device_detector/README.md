## GPSTrackit Faulty Device Detector

Both ``services`` and ``ETL`` modules can run in EC2 instances or be deployed to independent ECS clusters using cloudformation (therefore running as ECS tasks). 

### Deployment using Cloudformation (recommended):

Deployment of both docker images (services, ETL) to ECS/ECR (the images are pushed to separate ECR repositories, and then run as ECS tasks). The ETL task has a configured schedule that runs the task each 
day at 5:00 am UTC. 

1. Stand in ``resources/cloudformation`` directory.
2. Run the following command to deploy the stack, indicating the region for deploy, the bucket name where data will be stored, subnet id and VPC id (same as the db's VPC), db
parameters, model criteria and a namespace container for metrics: ``python3 deploy_stack.py -r <REGION> -b <BUCKET_NAME> -s <SUBNET_ID> -v <VPC_ID> -h <DB_HOST> -u <DB_USER> -p <DB_PORT> -n <DB_NAME> -m <NAMESPACE> -o``. 

If `-o` (only metamodel) flag is present, only with the metamodel will be employed for the classification. Otherwise, the metamodel and the neural network will be used. 

``To be able to access the database within the same VPC from the ETL task, remember to add to the db's Security Group the inbound rule corresponding to the VPC's or subnet (as preferred) IPv4 CIDR. The ECS task will be running in the VPC/Subnet indicated, therefore accessing the db from there.``

When the stack is deployed, no data will be available as no ETL has executed yet, meaning the S3 bucket has no data. Therefore, we will need to wait until the next ETL runs to see device and model data in the UI.

Example command:

``
python3 deploy_stack.py -v vpc-123456 -s subnet-123456 -h example_host.us-east-1.rds.amazonaws.com -u example_user -n example_db -r us-east-1 -p 3306 -b faulty-detector-bucket
``

### Manual deployment/hosting in EC2:

First of all, a bucket needs to be created for the ETL module to store the data queried from the database to run the model, and the metamodel's 
outcome. The EC2 must have an IAM role with permission to read and write files in S3, otherwise keys must be specified 
in docker running commands. The EC2 must have access to the database ``fleet_db``.

Secondly, a namespace must be created in CloudMap to group metrics. 

#### To deploy the services module (which runs the UI):
1. Zip/compress the ``UI`` folder.
2. Send it to EC2 host (tmp folder) using ec2-user: ``scp -i <PATH_TO_KEYS> <PATH_TO_ZIP> ec2-user@<ec2 host IP>:/tmp``
3. Access via SSH to EC2 host.
4. Unzip the ``UI.zip`` in tmp folder.
5. Build the docker image with the following command: ``sudo docker build --tag services .``<br /> 
Be sure to be standing on ``UI`` folder where the ``DockerFile`` is to build the image.
6. Add to the EC2 the inbound rule for port 5000 (TCP).
7. Run the docker image in a docker container with the following command:``sudo docker run -p 5000:5000 -e BUCKET_NAME=<bucket_name_defined> services``. If
the EC2 host does not have the IAM role, run the following command:
`` sudo docker run -p 5000:5000 -e AWS_ACCESS_KEY_ID=<your_access_key> -e AWS_SECRET_ACCESS_KEY=<your_secret_key> -e BUCKET_NAME=<bucket_name_defined> -e REGION=<region> -e NAMESPACE=<namespace> services``.

Once running, the UI will listen in port 5000.

#### To deploy the ETL module (which runs the model for the previous day using last week's data):
1. Zip/compress the ``device_detector`` folder.
2. Send it to EC2 host (tmp folder) using ec2-user: ``scp -i <PATH_TO_KEYS> <PATH_TO_ZIP> ec2-user@<ec2 host IP>:/tmp``
3. Access via SSH to EC2 host.
4. Unzip the ``device_detector.zip`` in tmp folder.
5. Build the docker image using ``sudo docker build --tag etl .`` 
Be sure to be standing on ``device_detector`` folder where the ``DockerFile`` is to build the image.
6. Run the following commands to create a cron job to run this docker image on a daily basis (fixed schedule):
   1. ``sudo service crond start``
   2. ``crontab -e``
7. Here you will create the following cronjob and save it:`` 0 5 * * * nohup sudo docker run -e BUCKET_NAME=<bucket_name_defined> -e GPS_DB_HOST=<your_db_host> -e GPS_DB_USER=<your_db_user> -e GPS_DB_PASSWORD=<your_db_password> -e GPS_DB_NAME=<your_db_name> -e ONLY_METAMODEL=False  etl &``.
If the EC2 host does not have IAM role remember to specify keys as above.
8. Once the cronjob is configured the ETL will run every day at 5:00 am UTC.

Once the ETL is deployed, a CloudWatch dashboard can be created to view the metrics. 
Make sure to add `Number/SingleValue` widgets for each of the following metrics in the namespace created:
- ModelRunDaily, Statistic: 'Average', Period: 1 day
- PercentageFaultyDaily, Statistic: 'Average', Period: 1 day,
- ModelClassifiedDaily, Statistic: 'Average', Period: 1 day,
- FaultyDaily, Statistic: 'Average', Period: 1 day,
- ClassifiedExternallyDaily, Statistic: 'Maximum', Period: 12 hours,
- TotalClassifiedExternally, Statistic: 'Maximum', Period: 1 month
