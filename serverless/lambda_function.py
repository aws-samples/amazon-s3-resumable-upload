import json, os, urllib, ssl, logging, time
import boto3
from s3_migration_lib import step_function, wait_sqs_available
from botocore.config import Config
from pathlib import PurePosixPath

# 常量
Des_bucket_default = os.environ['Des_bucket_default']
Des_prefix_default = os.environ['Des_prefix_default']
aws_access_key_id = os.environ['aws_access_key_id']
aws_secret_access_key = os.environ['aws_secret_access_key']

table_queue_name = os.environ['table_queue_name']
StorageClass = os.environ['StorageClass']

MaxRetry = 10
MaxThread = 50
MaxParallelFile = 1
JobTimeout = 3000

ResumableThreshold = 5 * 1024 * 1024  # Accelerate to ignore get list
CleanUnfinishedUpload = False  # For debug
LocalProfileMode = False  # For debug
ChunkSize = 5 * 1024 * 1024  # For debug
ifVerifyMD5Twice = False  # For debug

s3_config = Config(max_pool_connections=50)  # boto default 10

# Set environment
logger = logging.getLogger()
logger.setLevel(logging.INFO)

region = os.environ['Des_region']
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
ssm = boto3.client('ssm')

# 取另一个Account的credentials
credentials_session = boto3.session.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region
)

s3_src_client = boto3.client('s3', config=s3_config)
s3_des_client = credentials_session.client('s3', config=s3_config)

table = dynamodb.Table(table_queue_name)
table.wait_until_exists()
sqs_queue = wait_sqs_available(sqs, table_queue_name)

try:
    context = ssl._create_unverified_context()
    response = urllib.request.urlopen(
        urllib.request.Request("https://checkip.amazonaws.com"), timeout=3, context=context
    ).read()
    instance_id = "lambda-" + response.decode('utf-8')[0:-1]
except Exception as e:
    logger.warning(f'Fail to connect to checkip.amazonaws.com')
    instance_id = 'lambda-ip-timeout'


class TimeoutOrMaxRetry(Exception):
    pass


def lambda_handler(event, context):

    print("Lambda or NAT IP Address:", instance_id)
    logger.info(json.dumps(event, default=str))

    trigger_body = event['Records'][0]['body']
    job = json.loads(trigger_body)
    logger.info(json.dumps(job, default=str))

    # 判断是S3来的消息，而不是jodsender来的就转换一下
    if 'Records' in job:  # S3来的消息带着'Records'
        for One_record in job['Records']:
            if 's3' in One_record:
                Src_bucket = One_record['s3']['bucket']['name']
                Src_key = One_record['s3']['object']['key']
                Src_key = urllib.parse.unquote_plus(Src_key)
                Size = One_record['s3']['object']['size']
                if Size == 0:
                    return {
                        'statusCode': 200,
                        'body': "Zero size file"
                    }
                Des_bucket, Des_prefix = Des_bucket_default, Des_prefix_default
                job = {
                    'Src_bucket': Src_bucket,
                    'Src_key': Src_key,
                    'Size': Size,
                    'Des_bucket': Des_bucket,
                    'Des_key': str(PurePosixPath(Des_prefix) / Src_key)
                }
    if 'Des_bucket' not in job:  # 消息结构不对
        logger.warning(f'Wrong sqs job: {json.dumps(job, default=str)}')
        logger.warning('Try to handle next message')
        return {
                'statusCode': 200,
                'body': "Wrong sqs job return"
            }
    #
    # # S3直接触发SQS的把Size等信息补上，不补是不影响业务流程的，只是方便统计和补数据
    # with table.batch_writer() as ddb_batch:
    #     # write to ddb, auto batch
    #     for retry in range(MaxRetry + 1):
    #         try:
    #             ddb_key = str(PurePosixPath(job["Src_bucket"]) / job["Src_key"])
    #             ddb_batch.put_item(Item={
    #                 "Key": ddb_key,
    #                 "Src_bucket": job["Src_bucket"],
    #                 "Des_bucket": job["Des_bucket"],
    #                 "Des_key": job["Des_key"],
    #                 "Size": job["Size"]
    #             })
    #             break
    #         except Exception as e:
    #             logger.error(f'Fail writing to DDB: {ddb_key}, {str(e)}')
    #             if retry >= MaxRetry:
    #                 logger.error(f'Fail writing to DDB: {ddb_key}')
    #             else:
    #                 time.sleep(5 * retry)

    upload_etag_full = step_function(job, table, s3_src_client, s3_des_client, instance_id,
                                     StorageClass, ChunkSize, MaxRetry, MaxThread, ResumableThreshold,
                                     JobTimeout, ifVerifyMD5Twice, CleanUnfinishedUpload)

    if upload_etag_full != "TIMEOUT" and upload_etag_full != "ERR":
        return {
            'statusCode': 200,
            'body': upload_etag_full
        }
    else:
        raise TimeoutOrMaxRetry
