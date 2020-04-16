# PROJECT LONGBOW - JOBSENDER FOR COMPARE AMAZON S3 AND CREATE DELTA JOB LIST TO SQS

import json
import os
import sys
import time
from configparser import ConfigParser
from fnmatch import fnmatchcase
from pathlib import PurePosixPath
from s3_migration_lib import set_env, set_log
from operator import itemgetter

# Read config.ini
cfg = ConfigParser()
try:
    file_path = os.path.split(os.path.abspath(__file__))[0]
    cfg.read(f'{file_path}/s3_migration_cluster_config.ini', encoding='utf-8-sig')
    table_queue_name = cfg.get('Basic', 'table_queue_name')
    ssm_parameter_bucket = cfg.get('Basic', 'ssm_parameter_bucket')
    ssm_parameter_credentials = cfg.get('Basic', 'ssm_parameter_credentials')
    LocalProfileMode = cfg.getboolean('Debug', 'LocalProfileMode')
    JobType = cfg.get('Basic', 'JobType')
    LoggingLevel = cfg.get('Debug', 'LoggingLevel')
except Exception as e:
    print("s3_migration_cluster_config.ini", str(e))
    sys.exit(0)

# Set Logging
logger, log_file_name = set_log(LoggingLevel, 'jobsender')


# Get Environment
sqs, sqs_queue, table, s3_src_client, s3_des_client, instance_id, ssm = \
    set_env(JobType, LocalProfileMode, table_queue_name, ssm_parameter_credentials)


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


def delta_job_list(src_file_list, des_file_list, bucket_para, ignore_list):
    src_bucket = bucket_para['src_bucket']
    src_prefix = str(PurePosixPath(bucket_para['src_prefix']))
    des_bucket = bucket_para['des_bucket']
    des_prefix = str(PurePosixPath(bucket_para['des_prefix']))
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


# Main
if __name__ == '__main__':

    # Get ignore file list
    ignore_list_path = os.path.split(os.path.abspath(__file__))[0] + '/s3_migration_ignore_list.txt'
    ignore_list = []
    try:
        with open(ignore_list_path, 'r') as f:
            ignore_list = f.read().splitlines()
        logger.info(f'Found ignore files list Length: {len(ignore_list)}, in {ignore_list_path}')
    except Exception as e:
        if e.args[1] == 'No such file or directory':
            logger.info(f'No ignore files list in {ignore_list_path}')
            print(f'No ignore files list in {ignore_list_path}')
        else:
            logger.info(str(e))

    # Check SQS is empty or not
    if check_sqs_empty(sqs, sqs_queue):
        logger.info('Job sqs queue is empty, now process comparing s3 bucket...')

        # Load Bucket para from ssm parameter store
        logger.info(f'Get ssm_parameter_bucket: {ssm_parameter_bucket}')
        try:
            load_bucket_para = json.loads(ssm.get_parameter(Name=ssm_parameter_bucket)['Parameter']['Value'])
            logger.info(f'Recieved ssm {json.dumps(load_bucket_para)}')
        except Exception as e:
            logger.error(f'Fail to get buckets info from ssm_parameter_bucket, fix and restart Jobsender. {str(e)}')
            sys.exit(0)
        for bucket_para in load_bucket_para:
            src_bucket = bucket_para['src_bucket']
            src_prefix = bucket_para['src_prefix']
            des_bucket = bucket_para['des_bucket']
            des_prefix = bucket_para['des_prefix']

            # Get List on S3
            src_file_list = get_s3_file_list(s3_src_client, src_bucket, src_prefix)
            des_file_list = get_s3_file_list(s3_des_client, des_bucket, des_prefix, True)
            # Generate job list
            job_list, ignore_records = delta_job_list(src_file_list, des_file_list, bucket_para, ignore_list)

            # Just backup for debug
            logger.info('Writing job and ignore list to local file backup...')
            t = time.localtime()
            start_time = f'{t.tm_year}-{t.tm_mon}-{t.tm_mday}-{t.tm_hour}-{t.tm_min}-{t.tm_sec}'
            log_path = os.path.split(os.path.abspath(__file__))[0] + '/s3_migration_log'
            if job_list:
                local_backup_list = f'{log_path}/job-list-{src_bucket}-{start_time}.json'
                with open(local_backup_list, 'w') as f:
                    json.dump(job_list, f)
                logger.info(f'Write Job List: {os.path.abspath(local_backup_list)}')
            if ignore_records:
                local_ignore_records = f'{log_path}/ignore-records-{src_bucket}-{start_time}.json'
                with open(local_ignore_records, 'w') as f:
                    json.dump(ignore_records, f)
                logger.info(f'Write Ignore List: {os.path.abspath(local_ignore_records)}')

            # Upload jobs to sqs
            if len(job_list) != 0:
                job_upload_sqs_ddb(sqs, sqs_queue, table, job_list)
                max_object = max(job_list, key=itemgetter('Size'))
                if max_object['Size'] > 50*1024*1024*1024:
                    logger.warning(f'Max object in job_list is {str(max_object)}, '
                                   f'be carefull to tune the concurrency threads and instance memory')
                else:
                    logger.info(f'Max object in job_list is {str(max_object)}')
            else:
                logger.info('Source list are all in Destination, no job to send.')


    else:
        logger.error('Job sqs queue is not empty or fail to get_queue_attributes. Stop process.')
    print('Completed and logged to file:', os.path.abspath(log_file_name))
