import json
import os
import sys
from configparser import ConfigParser

from s3_migration_lib import get_s3_file_list, job_upload_sqs_ddb, set_env, \
    delta_job_list, set_log

# Read config.ini
cfg = ConfigParser()
try:
    file_path = os.path.split(os.path.abspath(__file__))[0]
    cfg.read(f'{file_path}/s3_migration_cluster_config.ini')
    table_queue_name = cfg.get('Basic', 'table_queue_name')
    ssm_parameter_bucket = cfg.get('Basic', 'ssm_parameter_bucket')
    ssm_parameter_credentials = cfg.get('Basic', 'ssm_parameter_credentials')
    LocalProfileMode = cfg.getboolean('Debug', 'LocalProfileMode')
    JobType = cfg.get('Basic', 'JobType')
    LoggingLevel = cfg.get('Debug', 'LoggingLevel')
except Exception as e:
    print("s3_migration_cluster_config.ini", str(e))
    sys.exit(0)


# Main
if __name__ == '__main__':

    # Set Logging
    logger, log_file_name = set_log(LoggingLevel)

    # Get Environment
    sqs, sqs_queue, table, s3_src_client, s3_des_client, instance_id, ssm = \
        set_env(JobType, LocalProfileMode, table_queue_name, ssm_parameter_credentials)

    #######
    # Program start processing here
    #######

    # Load Bucket para from ssm parameter store
    load_bucket_para = json.loads(ssm.get_parameter(Name=ssm_parameter_bucket)['Parameter']['Value'])
    for bucket_para in load_bucket_para:
        src_bucket = bucket_para['src_bucket']
        src_prefix = bucket_para['src_prefix']
        des_bucket = bucket_para['des_bucket']
        des_prefix = bucket_para['des_prefix']

        # Get List on S3
        src_file_list = get_s3_file_list(s3_src_client, src_bucket, src_prefix)
        des_file_list = get_s3_file_list(s3_des_client, des_bucket, des_prefix)
        # Generate job list
        job_list = delta_job_list(src_file_list, des_file_list, bucket_para)

        logger.info('Writing job list to local file backup...')
        log_path = os.path.split(os.path.abspath(__file__))[0] + '/s3_migration_log'
        local_backup_list = f'{log_path}/job-list-{src_bucket}.json'
        with open(local_backup_list, 'w') as f:
            json.dump(job_list, f)  # 仅做备份检查用
        logger.info(f'Finish writing: {os.path.abspath(local_backup_list)}')

        # Upload jobs to sqs
        if len(job_list) != 0:
            job_upload_sqs_ddb(sqs, sqs_queue, table, job_list)
        else:
            logger.info('Source and Destination are the same, no job to send.')

    print('Logged to file:', os.path.abspath(log_file_name))
