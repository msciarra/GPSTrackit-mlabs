import base64
import json
import docker
import boto3

from utils.stack_utils import get_stack_params
from utils.s3_utils import create_bucket

stack_params = get_stack_params()
cloudformation = boto3.client('cloudformation', region_name=stack_params['region'])
ecr = boto3.client('ecr', region_name=stack_params['region'])

ecr_repo_service_name = 'faulty-detector-services-repository'
ecr_service_path = "../../UI"
ecr_service_tag = "services"

ecr_repo_etl_name = "faulty-detector-etl-repository"
ecr_etl_path = "../../device_detector"
ecr_etl_tag = "etl"

stack_name = 'faulty-detector-stack'


def deploy_stack():
    """
    Deploys the stack, first creating an ECR repository where the services image is pushed, and afterwards
    creating the stack in Cloudformation.
    """
    ecr_repository_service_uri = create_ecr_repository_and_get_uri(ecr_repo_service_name)
    build_docker_image_and_push_to_ecr(ecr_service_path, ecr_service_tag, ecr_repo_service_name)
    service_image_uri = f'{ecr_repository_service_uri}:latest'

    ecr_repository_etl_uri = create_ecr_repository_and_get_uri(ecr_repo_etl_name)
    build_docker_image_and_push_to_ecr(ecr_etl_path, ecr_etl_tag, ecr_repo_etl_name)
    etl_image_uri = f'{ecr_repository_etl_uri}:latest'

    create_bucket(stack_params['bucket_name'], stack_params['region'])

    template_file_location = 'templates/stack_template.json'

    with open(template_file_location, 'r') as content_file:
        content = json.load(content_file)
    content = json.dumps(content)

    cloudformation.create_stack(
        StackName=stack_name,
        TemplateBody=content,
        Parameters=[
            {
                'ParameterKey': 'VpcId',
                'ParameterValue': stack_params['vpc_id']
            },
            {
                'ParameterKey': 'SubnetId',
                'ParameterValue': stack_params['subnet_id']
            },
            {
                'ParameterKey': 'BucketName',
                'ParameterValue': stack_params['bucket_name']
            },
            {
                'ParameterKey': 'ImageServiceUri',
                'ParameterValue': service_image_uri
            },
            {
                'ParameterKey': 'ImageEtlUri',
                'ParameterValue': etl_image_uri
            },
            {
                'ParameterKey': 'GpsDbHost',
                'ParameterValue': stack_params['db_host']
            },
            {
                'ParameterKey': 'GpsDbUser',
                'ParameterValue': stack_params['db_user']
            },
            {
                'ParameterKey': 'GpsDbName',
                'ParameterValue': stack_params['db_name']
            },
            {
                'ParameterKey': 'Region',
                'ParameterValue': stack_params['region']
            },
            {
                'ParameterKey': 'GpsDbPort',
                'ParameterValue': stack_params['db_port']
            },
            {
                'ParameterKey': 'OnlyMetamodel',
                'ParameterValue': stack_params['only_metamodel']
            },
            {
                'ParameterKey': 'Namespace',
                'ParameterValue': stack_params['namespace']
            }
        ],
        Capabilities=['CAPABILITY_NAMED_IAM', 'CAPABILITY_AUTO_EXPAND']
    )
    waiter = cloudformation.get_waiter('stack_create_complete')
    waiter.wait(StackName=stack_name)
    add_widgets_to_dashboard('faulty-detector-dashboard', stack_params['namespace'], stack_params['region'])


def create_ecr_repository_and_get_uri(repo_name):
    """
    Builds a repository in ecr and returns the uri of the repository.
    :param repo_name: Name of repository created.
    """
    ecr_repository_response = ecr.create_repository(repositoryName=repo_name)
    return ecr_repository_response['repository']['repositoryUri']


def build_docker_image_and_push_to_ecr(path, tag, repo_name):
    """
    Builds the docker image for services (UI) and pushes it to the ECR repository indicated.
    :param path: Path to Dockerfile.
    :param tag: Tag of the image.
    :param repo_name: Name of repository where image will be stored.
    """
    docker_client = docker.from_env()
    image, build_log = docker_client.images.build(path=path, tag=tag, rm=True)
    ecr_credentials = (ecr.get_authorization_token()['authorizationData'][0])
    ecr_password = (base64.b64decode(ecr_credentials['authorizationToken']).replace(b'AWS:', b'').decode('utf-8'))
    ecr_username = 'AWS'
    ecr_url = ecr_credentials['proxyEndpoint']
    docker_client.login(username=ecr_username, password=ecr_password, registry=ecr_url)
    ecr_repo_to_push = '{}/{}'.format(ecr_url.replace('https://', ''), repo_name)
    image.tag(ecr_repo_to_push, tag='latest')
    docker_client.images.push(ecr_repo_to_push, tag='latest')


def add_widgets_to_dashboard(dashboard_name, namespace, region):
    """
    Adds widgets to an existing cloudwatch dashboard showing all metrics as single value metrics.
    :param dashboard_name: name of the dashboard.
    :param namespace: name of the Cloud Map namespace where metrics are.
    :param region: AWS region.
    """
    list_widgets = []
    seconds_in_day = 24*60*60
    seconds_in_half_day = 12*60*60
    seconds_in_month = 30*seconds_in_day
    metrics = [('ModelRunDaily', 'Average', seconds_in_day),
               ('PercentageFaultyDaily', 'Average', seconds_in_day),
               ('ModelClassifiedDaily', 'Average', seconds_in_day),
               ('FaultyDaily', 'Average', seconds_in_day),
               ('ClassifiedExternallyDaily', 'Maximum', seconds_in_half_day),
               ('TotalClassifiedExternally', 'Maximum', seconds_in_month)]

    for metric in metrics:
        prop_metrics = {"metrics": [[f"{namespace}", f"{metric[0]}"]], "view": "singleValue", "region": f"{region}",
                        'period': metric[2], 'stat': f"{metric[1]}"}
        widget = {"type": "metric", "properties": prop_metrics}
        list_widgets.append(widget)

    dashboard_body = f'"widgets": {list_widgets}'
    dashboard_body = "{" + dashboard_body + "}"
    dashboard_body = dashboard_body.replace("'", '"')
    
    cw_client = boto3.client('cloudwatch')
    cw_client.put_dashboard(DashboardName=dashboard_name, DashboardBody=dashboard_body)


if __name__ == "__main__":
    deploy_stack()
