# PROJECT LONGBOW
# AWS LAMBDA WORKER NODE FOR TRANSMISSION BETWEEN AMAZON S3

import json
import logging
import os
import ssl
import urllib
import urllib.error
import urllib.parse
import urllib.request
from pathlib import PurePosixPath

import boto3
from botocore.config import Config

from s3_migration_lib import step_function, step_fn_small_file

# 环境变量
table_queue_name = os.environ['table_queue_name']
StorageClass = os.environ['StorageClass']
try:
    Des_bucket_default = os.environ['Des_bucket_default']
    Des_prefix_default = os.environ['Des_prefix_default']
except Exception as e:
    print('No Env Des_bucket_default/Des_prefix_default ', e)
    Des_bucket_default, Des_prefix_default = "", ""
ssm_parameter_credentials = os.environ['ssm_parameter_credentials']
checkip_url = os.environ['checkip_url']
JobType = os.environ['JobType']
MaxRetry = int(os.environ['MaxRetry'])  # 最大请求重试次数
MaxThread = int(os.environ['MaxThread'])  # 最大线程数
MaxParallelFile = int(os.environ['MaxParallelFile'])  # Lambda 中暂时没用到
JobTimeout = int(os.environ['JobTimeout'])
UpdateVersionId = os.environ['UpdateVersionId'].upper() == 'TRUE'  # get lastest version id from s3 before get object
GetObjectWithVersionId = os.environ['GetObjectWithVersionId'].upper() == 'TRUE'  # get object with version id

# 内部参数
ResumableThreshold = 5 * 1024 * 1024  # Accelerate to ignore small file
CleanUnfinishedUpload = False  # For debug
ChunkSize = 5 * 1024 * 1024  # For debug, will be auto-change
ifVerifyMD5Twice = False  # For debug
s3_config = Config(max_pool_connections=200,
                   retries={'max_attempts': MaxRetry})  # 最大连接数

# Set environment
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_queue_name)

# 取另一个Account的credentials
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


class TimeoutOrMaxRetry(Exception):
    pass


class WrongRecordFormat(Exception):
    pass


def lambda_handler(event, context):
    print("Lambda or NAT IP Address:", instance_id)
    logger.info(json.dumps(event, default=str))

    for trigger_record in event['Records']:
        trigger_body = trigger_record['body']
        job = json.loads(trigger_body)
        logger.info(json.dumps(job, default=str))

        # 跳过初次配置时候， S3 自动写SQS的访问测试记录
        if 'Event' in job:
            if job['Event'] == 's3:TestEvent':
                logger.info('Skip s3:TestEvent')
                continue

        # 判断是S3来的消息，而不是jodsender来的就转换一下
        if 'Records' in job:  # S3来的消息带着'Records'
            for One_record in job['Records']:
                if 's3' in One_record:
                    Src_bucket = One_record['s3']['bucket']['name']
                    Src_key = One_record['s3']['object']['key']
                    Src_key = urllib.parse.unquote_plus(Src_key)  # 加号转回空格
                    Size = One_record['s3']['object']['size']
                    if "versionId" in One_record['s3']['object']:
                        versionId = One_record['s3']['object']['versionId']
                    else:
                        versionId = 'null'
                    Des_bucket, Des_prefix = Des_bucket_default, Des_prefix_default
                    Des_key = str(PurePosixPath(Des_prefix) / Src_key)
                    if Src_key[-1] == '/':  # 针对空目录对象
                        Des_key += '/'
                    job = {
                        'Src_bucket': Src_bucket,
                        'Src_key': Src_key,
                        'Size': Size,
                        'Des_bucket': Des_bucket,
                        'Des_key': Des_key,
                        'versionId': versionId
                    }
        if 'Des_bucket' not in job:  # 消息结构不对
            logger.warning(f'Wrong sqs job: {json.dumps(job, default=str)}')
            logger.warning('Try to handle next message')
            raise WrongRecordFormat
        if 'versionId' not in job:
            job['versionId'] = 'null'

        # TODO: 如果是一次多条Job并且出现一半失败的问题未处理，所以目前只设置SQS Batch=1
        if job['Size'] > ResumableThreshold:
            upload_etag_full = step_function(
                                job=job,
                                table=table,
                                s3_src_client=s3_src_client,
                                s3_des_client=s3_des_client,
                                instance_id=instance_id,
                                StorageClass=StorageClass,
                                ChunkSize=ChunkSize,
                                MaxRetry=MaxRetry,
                                MaxThread=MaxThread,
                                JobTimeout=JobTimeout,
                                ifVerifyMD5Twice=ifVerifyMD5Twice,
                                CleanUnfinishedUpload=CleanUnfinishedUpload,
                                UpdateVersionId=UpdateVersionId,
                                GetObjectWithVersionId=GetObjectWithVersionId
                            )
        else:
            upload_etag_full = step_fn_small_file(
                                job=job,
                                table=table,
                                s3_src_client=s3_src_client,
                                s3_des_client=s3_des_client,
                                instance_id=instance_id,
                                StorageClass=StorageClass,
                                MaxRetry=MaxRetry,
                                UpdateVersionId=UpdateVersionId,
                                GetObjectWithVersionId=GetObjectWithVersionId
                            )
        if upload_etag_full != "TIMEOUT" and upload_etag_full != "ERR":
            # 如果是超时或ERR的就不删SQS消息，是正常结束就删
            # 大文件会在退出线程时设 MaxRetry 为 TIMEOUT，小文件则会返回 MaxRetry
            # 小文件出现该问题可以认为没必要再让下一个worker再试了，不是因为文件下载太大导致，而是权限设置导致
            # 直接删除SQS，并且DDB并不会记录结束状态
            # 如果希望小文件也继续让SQS消息恢复，并让下一个worker再试，则在上面判断加upload_etag_full != "MaxRetry"
            continue
        else:
            raise TimeoutOrMaxRetry

    return {
        'statusCode': 200,
        'body': 'Jobs completed'
    }
