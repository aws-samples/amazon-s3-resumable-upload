# PROJECT LONGBOW - JOBSENDER FOR COMPARE AMAZON S3 AND CREATE DELTA JOB LIST TO SQS

import json
import logging
import os
import ssl
import time
import urllib.request
from fnmatch import fnmatchcase
from pathlib import PurePosixPath

import boto3

# 环境变量
table_queue_name = os.environ['table_queue_name']
StorageClass = os.environ['StorageClass']
ssm_parameter_credentials = os.environ['ssm_parameter_credentials']
checkip_url = os.environ['checkip_url']
sqs_queue_name = os.environ['sqs_queue']
ssm_parameter_ignore_list = os.environ['ssm_parameter_ignore_list']
ssm_parameter_bucket = os.environ['ssm_parameter_bucket']

# 内部参数
JobType = "PUT"

# Set environment
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
s3_src_client = boto3.client('s3')
s3_des_client = credentials_session.client('s3')
if JobType.upper() == "GET":
    s3_src_client, s3_des_client = s3_des_client, s3_src_client

try:
    context = ssl._create_unverified_context()
    response = urllib.request.urlopen(
        urllib.request.Request(checkip_url), timeout=3, context=context
    ).read()
    instance_id = "lambda-" + response.decode('utf-8')
except Exception as e:
    logger.warning(f'Fail to connect to checkip.amazonaws.com')
    instance_id = 'lambda-ip-timeout'


def job_upload_sqs_ddb(sqs, sqs_queue, table, job_list, MaxRetry=30):
    sqs_batch = 0
    sqs_message = []
    logger.info(f'Start uploading jobs to queue: {sqs_queue}')
    # create ddb writer, 这里写table是为了一次性发数十万的job到sqs时在table有记录可以核对
    with table.batch_writer() as ddb_batch:
        for job in job_list:
            # write to ddb, auto batch
            for retry in range(MaxRetry + 1):
                try:
                    ddb_key = str(PurePosixPath(job["Src_bucket"]) / job["Src_key"])
                    if job["Src_key"][-1] == '/':
                        ddb_key += '/'
                    ddb_batch.put_item(Item={
                        "Key": ddb_key,
                        # "Src_bucket": job["Src_bucket"],
                        # "Des_bucket": job["Des_bucket"],
                        # "Des_key": job["Des_key"],
                        "Size": job["Size"]
                    })
                    break
                except Exception as e:
                    logger.warning(f'Fail to writing to DDB: {ddb_key}, {str(e)}')
                    if retry >= MaxRetry:
                        logger.error(f'Fail writing to DDB: {ddb_key}')
                    else:
                        time.sleep(5 * retry)

            # construct sqs messages
            sqs_message.append({
                "Id": str(sqs_batch),
                "MessageBody": json.dumps(job),
            })
            sqs_batch += 1

            # write to sqs in batch 10 or is last one
            if sqs_batch == 10 or job == job_list[-1]:
                for retry in range(MaxRetry + 1):
                    try:
                        sqs.send_message_batch(QueueUrl=sqs_queue, Entries=sqs_message)
                        break
                    except Exception as e:
                        logger.warning(f'Fail to send sqs message: {str(sqs_message)}, {str(e)}')
                        if retry >= MaxRetry:
                            logger.error(f'Fail MaxRetry {MaxRetry} send sqs message: {str(sqs_message)}')
                        else:
                            time.sleep(5 * retry)
                sqs_batch = 0
                sqs_message = []

    logger.info(f'Complete upload job to queue: {sqs_queue}')
    return


def delta_job_list(src_file_list, des_file_list, src_bucket, src_prefix, des_bucket, des_prefix, ignore_list):
    # Delta list
    logger.info(f'Compare source s3://{src_bucket}/{src_prefix} and '
                f'destination s3://{des_bucket}/{des_prefix}')
    start_time = int(time.time())
    job_list = []
    ignore_records = []
    for src in src_file_list:

        # 排除掉 ignore_list 里面列的 bucket/key
        src_bucket_key = src_bucket + '/' + src['Key']
        ignore_match = False
        # 每个 ignore key 匹配一次，匹配上任何一个就跳过这个scr_key
        for ignore_key in ignore_list:
            if fnmatchcase(src_bucket_key, ignore_key):
                ignore_match = True
                break
            # 匹配不上，循环下一个 ignore_key
        if ignore_match:
            ignore_records.append(src_bucket_key)
            continue  # 跳过当前 src

        # 比对源文件是否在目标中
        if src in des_file_list:
            # 下一个源文件
            continue
        # 把源文件加入job list
        else:
            Des_key = str(PurePosixPath(des_prefix) / src["Key"])
            if src["Key"][-1] == '/':  # 源Key是个目录的情况，需要额外加 /
                Des_key += '/'
            job_list.append(
                {
                    "Src_bucket": src_bucket,
                    "Src_key": src["Key"],  # Src_key已经包含了Prefix
                    "Des_bucket": des_bucket,
                    "Des_key": Des_key,
                    "Size": src["Size"],
                }
            )
    spent_time = int(time.time()) - start_time
    logger.info(f'Delta list: {len(job_list)}, ignore list: {len(ignore_records)} - SPENT TIME: {spent_time}S')
    return job_list, ignore_records


def check_sqs_empty(sqs, sqs_queue):
    try:
        sqs_in_flight = sqs.get_queue_attributes(
            QueueUrl=sqs_queue,
            AttributeNames=['ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessages']
        )
    except Exception as e:
        logger.error(f'Fail to get_queue_attributes: {str(e)}')
        return False  # Can't get sqs status, then consider it is not empty
    NotVisible = sqs_in_flight['Attributes']['ApproximateNumberOfMessagesNotVisible']
    Visible = sqs_in_flight['Attributes']['ApproximateNumberOfMessages']
    logger.info(f'ApproximateNumberOfMessagesNotVisible: {NotVisible}, ApproximateNumberOfMessages: {Visible}')
    if NotVisible == '0' and (Visible == '0' or Visible == '1'):
        # In init state, the new created bucket trigger SQS will send one test message to SQS.
        # So here to ignore the case Visible == '1'
        return True  # sqs is empty
    return False  # sqs is not empty


def get_s3_file_list(s3_client, bucket, S3Prefix, del_prefix=False):

    # For delete prefix in des_prefix
    if S3Prefix == '' or S3Prefix == '/':
        # 目的bucket没有设置 Prefix
        dp_len = 0
    else:
        # 目的bucket的 "prefix/"长度
        dp_len = len(S3Prefix) + 1

    # get s3 file list with loop retry every 5 sec
    des_file_list = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for retry in range(5):
        try:
            logger.info(f'Get s3 file list from: {bucket}/{S3Prefix}')
            response_iterator = paginator.paginate(
                Bucket=bucket,
                Prefix=S3Prefix
            )
            for page in response_iterator:
                if "Contents" in page:
                    for n in page["Contents"]:
                        key = n["Key"]

                        # For delete prefix in des_prefix
                        if del_prefix:
                            key = key[dp_len:]

                        des_file_list.append({
                            "Key": key,
                            "Size": n["Size"]
                        })
            break
        except Exception as err:
            logger.warning(f'Fail to get s3 list objests: {str(err)}')
            time.sleep(5)

    logger.info(f'Bucket list length：{str(len(des_file_list))}')

    return des_file_list


# handler
def lambda_handler(event, context):

    # Get ignore file list
    ignore_list = []
    try:
        logger.info('Try to get ignore list from ssm parameter')
        ignore_list = ssm.get_parameter(Name=ssm_parameter_ignore_list)['Parameter']['Value'].splitlines()
        logger.info(f'Get ignore list: {str(ignore_list)}')
    except Exception:
        logger.info(f'No ignore list in ssm parameter: {ssm_parameter_ignore_list}')

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
            src_file_list = get_s3_file_list(s3_src_client, src_bucket, src_prefix)
            logger.info('Get destination bucket')
            des_file_list = get_s3_file_list(s3_des_client, des_bucket, des_prefix, True)
            # Generate job list
            job_list, ignore_records = delta_job_list(src_file_list, des_file_list,
                                                      src_bucket, src_prefix, des_bucket, des_prefix, ignore_list)

            # Output for debug
            print("job_list: ")
            if job_list:
                for n in job_list:
                    print(str(n))
            print("ignore_records: ")
            if ignore_records:
                for n in ignore_records:
                    print(str(n))

            # Upload jobs to sqs
            if len(job_list) != 0:
                job_upload_sqs_ddb(sqs, sqs_queue, table, job_list)
            else:
                logger.info('Source list are all in Destination, no job to send.')
    else:
        logger.error('Job sqs queue is not empty or fail to get_queue_attributes. Stop process.')
    # print('Completed and logged to file:', os.path.abspath(log_file_name))


if __name__ == '__main__':
    lambda_handler("", "")

