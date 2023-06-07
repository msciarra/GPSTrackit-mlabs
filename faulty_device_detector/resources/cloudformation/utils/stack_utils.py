import getopt
import sys


def get_stack_params():
    """
    Returns the stack params for: the region in which the stack should be deployed,
    and the name of the bucket to be created in s3.
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "r:b:s:h:u:n:v:p:m:o", ["region=", "bucket_name=", "subnet_id=",
                                                                         "db_host=", "db_user=", "db_name=", "vpc_id=",
                                                                         "db_port=", "namespace=", "only_metamodel"])
    except getopt.GetoptError:
        print('python3 deploy_stack.py -r <region> -b <bucket_name> -s <subnet_id> -h <db_host> '
              '-u <db_user> -n <db_name> -v <vpc_id> -p <db_port> -m <namespace> -o')
        sys.exit(2)

    args = {
        'region': 'us-east-2',
        'bucket_name': 'faulty-detector-bucket',
        'only_metamodel': 'False',
        'namespace': 'faulty-detector-namespace'
    }

    for opt, arg in opts:
        if opt in ("-r", "-region"):
            args['region'] = arg
        if opt in ("-b", "-bucket_name"):
            args['bucket_name'] = arg
        if opt in ("-s", "-subnet_id"):
            args['subnet_id'] = arg
        if opt in ("-v", "-vpc_id"):
            args['vpc_id'] = arg
        if opt in ("-h", "-db_host"):
            args['db_host'] = arg
        if opt in ("-u", "-db_user"):
            args['db_user'] = arg
        if opt in ("-n", "-db_name"):
            args['db_name'] = arg
        if opt in ("-p", "-db_port"):
            args['db_port'] = arg
        if opt in ("-o", "-only_metamodel"):
            args['only_metamodel'] = 'True'
        if opt in ("-m", "-namespace"):
            args['namespace'] = arg
    return args
