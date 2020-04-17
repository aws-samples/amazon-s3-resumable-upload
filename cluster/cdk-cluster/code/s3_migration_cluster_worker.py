# PROJECT LONGBOW - WORKER NODE FOR TRANSMISSION BETWEEN AMAZON S3

import os
import sys
import concurrent.futures
from configparser import ConfigParser

from s3_migration_lib import set_env, set_log, job_looper

# Read config.ini
cfg = ConfigParser()
try:
    file_path = os.path.split(os.path.abspath(__file__))[0]
    cfg.read(f'{file_path}/s3_migration_cluster_config.ini', encoding='utf-8-sig')
    table_queue_name = cfg.get('Basic', 'table_queue_name')
    sqs_queue_name = cfg.get('Basic', 'sqs_queue_name')
    ssm_parameter_bucket = cfg.get('Basic', 'ssm_parameter_bucket')
    ssm_parameter_credentials = cfg.get('Basic', 'ssm_parameter_credentials')
    JobType = cfg.get('Basic', 'JobType')
    StorageClass = cfg.get('Mode', 'StorageClass')
    ifVerifyMD5Twice = cfg.getboolean('Debug', 'ifVerifyMD5Twice')
    Megabytes = 1024 * 1024
    ChunkSize = cfg.getint('Debug', 'ChunkSize') * Megabytes
    ResumableThreshold = cfg.getint('Mode', 'ResumableThreshold') * Megabytes
    MaxRetry = cfg.getint('Mode', 'MaxRetry')
    MaxThread = cfg.getint('Mode', 'MaxThread')
    MaxParallelFile = cfg.getint('Mode', 'MaxParallelFile')
    JobTimeout = cfg.getint('Mode', 'JobTimeout')
    LoggingLevel = cfg.get('Debug', 'LoggingLevel')
    CleanUnfinishedUpload = cfg.getboolean('Debug', 'CleanUnfinishedUpload')
    LocalProfileMode = cfg.getboolean('Debug', 'LocalProfileMode')
    try:
        Des_bucket_default = cfg.get('Basic', 'Des_bucket_default')
    except Exception as e:
        Des_bucket_default = 'foo'
    try:
        Des_prefix_default = cfg.get('Basic', 'Des_prefix_default')
    except Exception as e:
        Des_prefix_default = ''
except Exception as e:
    print("ERR loading s3_migration_cluster_config.ini", str(e))
    sys.exit(0)

# if CDK deploy, get para from environment variable
try:
    table_queue_name = os.environ['table_queue_name']
    sqs_queue_name = os.environ['sqs_queue_name']
    ssm_parameter_bucket = os.environ['ssm_parameter_bucket']
except Exception:
    print("No Environment Variable from CDK, use the para from config.ini")

# Main
if __name__ == '__main__':

    # Set Logging
    logger, log_file_name = set_log(LoggingLevel, 'ec2-worker')

    # Get Environment
    sqs, sqs_queue, table, s3_src_client, s3_des_client, instance_id, ssm = \
        set_env(JobType, LocalProfileMode, table_queue_name, sqs_queue_name, ssm_parameter_credentials)

    #######
    # Program start processing here
    #######

    # For concur jobs(files)
    logger.info(f'Start concurrent {MaxParallelFile} jobs.')
    with concurrent.futures.ThreadPoolExecutor(max_workers=MaxParallelFile) as job_pool:
        for i in range(MaxParallelFile):  # 这里只控制多个Job同时循环进行，每个Job的并发和超时在内层控制
            job_pool.submit(job_looper,
                            sqs, sqs_queue, table, s3_src_client, s3_des_client, instance_id,
                            StorageClass, ChunkSize, MaxRetry, MaxThread, ResumableThreshold,
                            JobTimeout, ifVerifyMD5Twice, CleanUnfinishedUpload,
                            Des_bucket_default, Des_prefix_default
                            )
