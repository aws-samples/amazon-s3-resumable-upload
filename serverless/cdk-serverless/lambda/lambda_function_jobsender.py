# PROJECT LONGBOW - JOBSENDER FOR COMPARE AMAZON S3 AND CREATE DELTA JOB LIST TO SQS

import json
import logging
import os
import ssl
import urllib.request
import urllib.error
from operator import itemgetter
from s3_migration_lib import get_des_file_list, get_src_file_list, job_upload_sqs_ddb, delta_job_list, check_sqs_empty
from botocore.config import Config
import boto3

# 环境变量
table_queue_name = os.environ['table_queue_name']
StorageClass = os.environ['StorageClass']
ssm_parameter_credentials = os.environ['ssm_parameter_credentials']
checkip_url = os.environ['checkip_url']
sqs_queue_name = os.environ['sqs_queue']
ssm_parameter_ignore_list = os.environ['ssm_parameter_ignore_list']
ssm_parameter_bucket = os.environ['ssm_parameter_bucket']
JobType = os.environ['JobType']
MaxRetry = int(os.environ['MaxRetry'])  # 最大请求重试次数
JobsenderCompareVersionId = os.environ['JobsenderCompareVersionId'].upper() == 'TRUE'

# Set environment
s3_config = Config(max_pool_connections=50, retries={'max_attempts': MaxRetry})  # 最大连接数
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_queue_name)
sqs = boto3.client('sqs')
sqs_queue = sqs.get_queue_url(QueueName=sqs_queue_name)['QueueUrl']

# Get credentials of the other account
ssm = boto3.client('ssm')
logger.info(f'Get ssm_parameter_credentials: {ssm_parameter_credentials}')
credentials = json.loads(ssm.get_parameter(
    Name=ssm_parameter_credentials,
    WithDecryption=True
)['Parameter']['Value'])
credentials_session = boto3.session.Session(
    aws_access_key_id=credentials["aws_access_key_id"],
    aws_secret_access_key=credentials["aws_secret_access_key"],
    region_name=credentials["region"]
)

# Get buckets information
logger.info(f'Get ssm_parameter_bucket: {ssm_parameter_bucket}')
load_bucket_para = json.loads(ssm.get_parameter(Name=ssm_parameter_bucket)['Parameter']['Value'])
logger.info(f'Recieved ssm {json.dumps(load_bucket_para)}')

# Default Jobtype is PUT
s3_src_client = boto3.client('s3', config=s3_config)
s3_des_client = credentials_session.client('s3', config=s3_config)
if JobType.upper() == "GET":
    s3_src_client, s3_des_client = s3_des_client, s3_src_client

try:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    response = urllib.request.urlopen(
        urllib.request.Request(checkip_url), timeout=3, context=context
    ).read()
    instance_id = "lambda-" + response.decode('utf-8')
except urllib.error.URLError as e:
    logger.warning(f'Fail to connect to checkip api: {checkip_url} - {str(e)}')
    instance_id = 'lambda-ip-timeout'


# handler
def lambda_handler(event, context):

    # Get ignore file list
    ignore_list = []
    try:
        logger.info('Try to get ignore list from ssm parameter')
        ignore_list = ssm.get_parameter(Name=ssm_parameter_ignore_list)['Parameter']['Value'].splitlines()
        logger.info(f'Get ignore list: {str(ignore_list)}')
    except Exception as e:
        logger.info(f'No ignore list in ssm parameter - {str(e)}')

    # Check SQS is empty or not
    if check_sqs_empty(sqs, sqs_queue):
        logger.info('Job sqs queue is empty, now process comparing s3 bucket...')
        for bucket_para in load_bucket_para:
            src_bucket = bucket_para['src_bucket']
            src_prefix = bucket_para['src_prefix']
            des_bucket = bucket_para['des_bucket']
            des_prefix = bucket_para['des_prefix']

            # Get List on S3
            logger.info('Get source bucket')
            src_file_list = get_src_file_list(
                s3_client=s3_src_client,
                bucket=src_bucket,
                S3Prefix=src_prefix,
                JobsenderCompareVersionId=JobsenderCompareVersionId
            )
            logger.info('Get destination bucket')
            des_file_list = get_des_file_list(
                s3_client=s3_des_client,
                bucket=des_bucket,
                S3Prefix=des_prefix,
                table=table,
                JobsenderCompareVersionId=JobsenderCompareVersionId
            )
            # Generate job list
            job_list, ignore_records = delta_job_list(
                src_file_list=src_file_list,
                des_file_list=des_file_list,
                src_bucket=src_bucket,
                src_prefix=src_prefix,
                des_bucket=des_bucket,
                des_prefix=des_prefix,
                ignore_list=ignore_list,
                JobsenderCompareVersionId=JobsenderCompareVersionId
            )

            # Upload jobs to sqs
            if len(job_list) != 0:
                job_upload_sqs_ddb(
                    sqs=sqs,
                    sqs_queue=sqs_queue,
                    job_list=job_list
                )
                max_object = max(job_list, key=itemgetter('Size'))
                MaxChunkSize = int(max_object['Size'] / 10000) + 1024
                if MaxChunkSize < 5 * 1024 * 1024:
                    MaxChunkSize = 5 * 1024 * 1024
                logger.warning(f'Max object size is {max_object["Size"]}. Require AWS Lambda memory > '
                               f'MaxChunksize({MaxChunkSize}) x MaxThread(default: 1) x MaxParallelFile(default: 50)')
            else:
                logger.info('Source list are all in Destination, no job to send.')
    else:
        logger.error('Job sqs queue is not empty or fail to get_queue_attributes. Stop process.')
    # print('Completed and logged to file:', os.path.abspath(log_file_name))
