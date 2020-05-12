# PROJECT LONGBOW - LIB FOR TRANSMISSION BETWEEN AMAZON S3

import datetime
import logging
import hashlib
import concurrent.futures
import threading
import base64
import urllib.request
import urllib.parse
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
# from boto3.dynamodb import conditions
import json
import os
import sys
import time
from fnmatch import fnmatchcase
from pathlib import PurePosixPath, Path

logger = logging.getLogger()
Max_md5_retry = 2


# Configure logging
def set_log(LoggingLevel, this_file_name):
    logger.setLevel(logging.WARNING)
    if LoggingLevel == 'INFO':
        logger.setLevel(logging.INFO)
    elif LoggingLevel == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    # File logging
    log_path = Path(__file__).parent.parent / 'amazon-s3-migration-log'
    if not Path.exists(log_path):
        Path.mkdir(log_path)
    start_time = datetime.datetime.now().isoformat().replace(':', '-')[:19]
    _log_file_name = str(log_path / f'{this_file_name}-{start_time}.log')
    print('Log file:', _log_file_name)
    fileHandler = logging.FileHandler(filename=_log_file_name)
    fileHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
    logger.addHandler(fileHandler)
    return logger, _log_file_name


# Set environment
def set_env(*, JobType, LocalProfileMode, table_queue_name, sqs_queue_name, ssm_parameter_credentials, MaxRetry):
    s3_config = Config(max_pool_connections=200, retries={'max_attempts': MaxRetry})  # boto default 10

    if os.uname()[0] == 'Linux' and not LocalProfileMode:  # on EC2, use EC2 role
        logger.info('Get instance-id and running region')
        instance_id = urllib.request.urlopen(urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/instance-id"
        )).read().decode('utf-8')
        region = json.loads(urllib.request.urlopen(urllib.request.Request(
            "http://169.254.169.254/latest/dynamic/instance-identity/document"
        )).read().decode('utf-8'))['region']
        sqs = boto3.client('sqs', region)
        dynamodb = boto3.resource('dynamodb', region)
        ssm = boto3.client('ssm', region)

        # 取另一个Account的credentials
        logger.info(f'Get ssm_parameter_credentials: {ssm_parameter_credentials}')
        try:
            credentials = json.loads(ssm.get_parameter(
                Name=ssm_parameter_credentials,
                WithDecryption=True
            )['Parameter']['Value'])
        except Exception as e:
            logger.error(f'Fail to get {ssm_parameter_credentials} in SSM Parameter store. '
                         f'Fix and restart Jobsender. {str(e)}')
            sys.exit(0)
        credentials_session = boto3.session.Session(
            aws_access_key_id=credentials["aws_access_key_id"],
            aws_secret_access_key=credentials["aws_secret_access_key"],
            region_name=credentials["region"]
        )
        if JobType.upper() == "PUT":
            s3_src_client = boto3.client('s3', region, config=s3_config)
            s3_des_client = credentials_session.client('s3', config=s3_config)
        elif JobType.upper() == "GET":
            s3_des_client = boto3.client('s3', region, config=s3_config)
            s3_src_client = credentials_session.client('s3', config=s3_config)
        else:
            logger.error('Wrong JobType setting in config.ini file')
            sys.exit(0)
    # 在没有Role的环境运行，例如本地Mac测试
    else:
        instance_id = "local"
        src_session = boto3.session.Session(profile_name='iad')
        des_session = boto3.session.Session(profile_name='zhy')
        sqs = src_session.client('sqs')
        dynamodb = src_session.resource('dynamodb')
        ssm = src_session.client('ssm')
        s3_src_client = src_session.client('s3', config=s3_config)
        s3_des_client = des_session.client('s3', config=s3_config)
        # 在当前屏幕也输出，便于local debug监控。
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
        logger.addHandler(streamHandler)

    table = dynamodb.Table(table_queue_name)
    table.wait_until_exists()
    sqs_queue = wait_sqs_available(
        sqs=sqs,
        sqs_queue_name=sqs_queue_name
    )

    return sqs, sqs_queue, table, s3_src_client, s3_des_client, instance_id, ssm


def wait_sqs_available(*, sqs, sqs_queue_name):
    while True:
        try:
            return sqs.get_queue_url(QueueName=sqs_queue_name)['QueueUrl']
        except Exception as e:
            logger.warning(f'Waiting for SQS availability. {str(e)}')
            time.sleep(10)


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


# Get source S3 bucket file list with versionId
def get_src_file_list(*, s3_client, bucket, S3Prefix, JobsenderCompareVersionId):
    # get s3 file list
    file_list = []
    if JobsenderCompareVersionId:
        paginator = s3_client.get_paginator('list_object_versions')
    else:
        paginator = s3_client.get_paginator('list_objects_v2')  # 速度比 list_object_versions 快很多
    try:
        if S3Prefix == '/':
            S3Prefix = ''
        logger.info(f'Get s3 file list from: {bucket}/{S3Prefix}')
        response_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=S3Prefix
        )
        for page in response_iterator:
            if "Versions" in page:  # JobsenderCompareVersionId==True
                logger.info(f'Got list_object_versions {bucket}/{S3Prefix}: {len(page["Versions"])}')
                for n in page["Versions"]:
                    # 只拿最新版本的列表
                    if n["IsLatest"]:
                        file_list.append({
                            "Key": n["Key"],
                            "Size": n["Size"],
                            "versionId": n["VersionId"] if JobsenderCompareVersionId else 'null'
                        })
            elif "Contents" in page:  # JobsenderCompareVersionId==False
                logger.info(f'Got list_objects_v2 {bucket}/{S3Prefix}: {len(page["Contents"])}')
                for n in page["Contents"]:
                    file_list.append({
                        "Key": n["Key"],
                        "Size": n["Size"],
                        "versionId": 'null'
                    })
    except Exception as err:
        logger.error(f'Fail to get s3 list versions {bucket}/{S3Prefix}: {str(err)}')
    logger.info(f'Bucket list length(get_src_file_list)：{str(len(file_list))}')
    return file_list


# Get Destination S3 bucket file list with versionId from DDB
def get_des_file_list(*, s3_client, bucket, S3Prefix, table, JobsenderCompareVersionId):
    if JobsenderCompareVersionId:
        ver_list = get_versionid_from_ddb(
            des_bucket=bucket,
            table=table
        )
    else:
        ver_list = {}
    # For delete prefix in des_prefix
    if S3Prefix == '' or S3Prefix == '/':
        # 目的bucket没有设置 Prefix
        dp_len = 0
    else:
        # 目的bucket的 "prefix/"长度
        dp_len = len(S3Prefix) + 1

    # get s3 file list
    file_list = []
    paginator = s3_client.get_paginator('list_objects_v2')
    try:
        if S3Prefix == '/':
            S3Prefix = ''
        logger.info(f'Get s3 file list from: {bucket}/{S3Prefix}')
        response_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=S3Prefix
        )
        for page in response_iterator:
            if "Contents" in page:
                logger.info(f'Got list_objects_v2 {bucket}/{S3Prefix}: {len(page["Contents"])}')
                for n in page["Contents"]:
                    # 取这个key的versionId，如果是ver_list是空，则会全部都是None
                    ver = ver_list.get(n["Key"])
                    # 目的桶要去掉prefix
                    file_list.append({
                        "Key": n["Key"][dp_len:],
                        "Size": n["Size"],
                        "versionId": ver if (ver is not None and JobsenderCompareVersionId) else 'null'
                    })
    except Exception as err:
        logger.error(f'Fail to get s3 list_objects_v2 {bucket}/{S3Prefix}: {str(err)}')
    logger.info(f'Bucket list length(get_des_file_list)：{str(len(file_list))}')
    return file_list


# Get s3 object versionId record in DDB
def get_versionid_from_ddb(*, des_bucket, table):
    logger.info(f'Get des_bucket versionId list from DDB')
    ver_list = {}
    try:
        r = table.query(
            IndexName='desBucket-index',
            KeyConditionExpression='desBucket=:b',
            ExpressionAttributeValues={":b": des_bucket}
        )
        if 'Items' in r:
            for i in r['Items']:
                ver_list[i['desKey']] = i['versionId']
        logger.info(f'Got versionId list {des_bucket}: {len(ver_list)}')
    except Exception as e:
        logger.error(f'Fail to query DDB for versionId {des_bucket}- {str(e)}')
    return ver_list


# Jobsender compare source and destination bucket list
def delta_job_list(*, src_file_list, des_file_list, src_bucket, src_prefix, des_bucket, des_prefix, ignore_list,
                   JobsenderCompareVersionId):
    # Delta list，只对比key&size，version不做对比，只用来发Job
    logger.info(f'Compare source s3://{src_bucket}/{src_prefix} and destination s3://{des_bucket}/{des_prefix}')
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

        # 比对源文件是否在目标中，会连带versionId一起比较
        if src in des_file_list:
            continue  # 在List中，下一个源文件
        # 不在List中，把源文件加入job list
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
                    "versionId": src['versionId']
                }
            )
    spent_time = int(time.time()) - start_time
    if JobsenderCompareVersionId:
        logger.info(f'Finish compare key/size/versionId in {spent_time} Seconds (JobsenderCompareVersionId is enable)')
    else:
        logger.info(f'Finish compare key/size in {spent_time} Seconds (JobsenderCompareVersionId is disable)')
    logger.info(f'Delta Job List: {len(job_list)} - Ignore List: {len(ignore_records)}')
    return job_list, ignore_records


def job_upload_sqs_ddb(*, sqs, sqs_queue, job_list):
    sqs_batch = 0
    sqs_message = []
    logger.info(f'Start uploading jobs to queue: {sqs_queue}')
    # create ddb writer
    # with table.batch_writer() as ddb_batch:
    for job in job_list:
        # construct sqs messages
        sqs_message.append({
            "Id": str(sqs_batch),
            "MessageBody": json.dumps(job),
        })
        sqs_batch += 1

        # write to sqs in batch 10 or is last one
        if sqs_batch == 10 or job == job_list[-1]:
            try:
                sqs.send_message_batch(QueueUrl=sqs_queue, Entries=sqs_message)
            except Exception as e:
                logger.error(f'Fail to send sqs message: {str(sqs_message)}, {str(e)}')
            sqs_batch = 0
            sqs_message = []

    logger.info(f'Complete upload job to queue: {sqs_queue}')
    return


# Split one file size into list of start byte position list
def split(Size, ChunkSize):
    partnumber = 1
    indexList = [0]
    if int(Size / ChunkSize) + 1 > 10000:
        ChunkSize = int(Size / 10000) + 1024  # 对于大于10000分片的大文件，自动调整Chunksize
        logger.info(f'Size excess 10000 parts limit. Auto change ChunkSize to {ChunkSize}')
    while ChunkSize * partnumber < Size:  # 如果刚好是"="，则无需再分下一part，所以这里不能用"<="
        indexList.append(ChunkSize * partnumber)
        partnumber += 1
    return indexList, ChunkSize


# Get S3 versionID
def head_s3_version(*, s3_src_client, Src_bucket, Src_key):
    logger.info(f'Try to update VersionId: {Src_bucket}/{Src_key}')
    try:
        head = s3_src_client.head_object(
            Bucket=Src_bucket,
            Key=Src_key
        )
        versionId = head['VersionId']
        Size = head['ContentLength']
        logger.info(f'Got VersionId: {versionId} - {Src_bucket}/{Src_key} - Size:{Size}')
    except Exception as e:
        logger.error(f'Fail to head s3 - {Src_bucket}/{Src_key}, {str(e)}')
        return "ERR", 0
    return versionId, Size


# Get unfinished multipart upload id from s3
def get_uploaded_list(*, s3_client, Des_bucket, Des_key):
    multipart_uploaded_list = []
    paginator = s3_client.get_paginator('list_multipart_uploads')
    try:
        logger.info(f'Getting unfinished upload id list - {Des_bucket}/{Des_key}...')
        response_iterator = paginator.paginate(
                    Bucket=Des_bucket,
                    Prefix=Des_key
                )
        for page in response_iterator:
            if "Uploads" in page:
                for i in page["Uploads"]:
                    if i["Key"] == Des_key or Des_key == '':
                        multipart_uploaded_list.append({
                            "Key": i["Key"],
                            "Initiated": i["Initiated"],
                            "UploadId": i["UploadId"]
                        })
                        logger.info(f'Unfinished upload, Key: {i["Key"]}, Time: {i["Initiated"]}')
    except Exception as e:
        logger.error(f'Fail to list multipart upload - {Des_bucket}/{Des_key} - {str(e)}')
    return multipart_uploaded_list


# Check file on the list from get_uploaded_list and get created multipart id
def check_file_exist(*, prefix_and_key, UploadIdList):
    # 查Key是否有未完成的UploadID
    keyIDList = []
    for u in UploadIdList:
        if u["Key"] == prefix_and_key:
            keyIDList.append(u)
    # 如果找不到上传过的Upload，则从头开始传
    if not keyIDList:
        return 'UPLOAD'
    # 对同一个Key（文件）的不同Upload找出时间最晚的值
    UploadID_latest = keyIDList[0]
    for u in keyIDList:
        if u["Initiated"] > UploadID_latest["Initiated"]:
            UploadID_latest = u
    # pick last one upload id with latest Initiated time
    logger.info(f"Pick UploadId Initiated Time: {UploadID_latest['Initiated']}")
    return UploadID_latest["UploadId"]


# Check uploaded part number list on Des_bucket
def checkPartnumberList(*, Des_bucket, Des_key, uploadId, s3_des_client):
    partnumberList = []
    logger.info(f'Get partnumber list - {Des_bucket}/{Des_key}')
    paginator = s3_des_client.get_paginator('list_parts')
    try:
        response_iterator = paginator.paginate(
            Bucket=Des_bucket,
            Key=Des_key,
            UploadId=uploadId
        )
        for page in response_iterator:
            if "Parts" in page:
                logger.info(f'Got list_parts: {len(page["Parts"])} - {Des_bucket}/{Des_key}')
                for p in page["Parts"]:
                    partnumberList.append(p["PartNumber"])
    except Exception as e:
        logger.error(f'Fail to list parts in checkPartnumberList - {Des_bucket}/{Des_key} - {str(e)}')
        return []

    if partnumberList:  # 如果空则表示没有查到已上传的Part
        logger.info(f"Found uploaded partnumber {len(partnumberList)} - {json.dumps(partnumberList)}"
                    f" - {Des_bucket}/{Des_key}")
    else:
        logger.info(f'Part number list is empty - {Des_bucket}/{Des_key}')
    return partnumberList


# Process one job
def job_processor(*, uploadId, indexList, partnumberList, job, s3_src_client, s3_des_client,
                  MaxThread, ChunkSize, MaxRetry, JobTimeout, ifVerifyMD5Twice, GetObjectWithVersionId):
    # 线程生成器，配合thread pool给出每个线程的对应关系，便于设置超时控制
    def thread_gen(woker_thread, pool,
                   stop_signal, partnumber, total, md5list, partnumberList, complete_list):
        for partStartIndex in indexList:
            # start to upload part
            if partnumber not in partnumberList:
                dryrun = False  # dryrun 是为了沿用现有的流程做出完成列表，方便后面计算 MD5
            else:
                dryrun = True
            th = pool.submit(woker_thread,
                             stop_signal=stop_signal,
                             partnumber=partnumber,
                             partStartIndex=partStartIndex,
                             total=total,
                             md5list=md5list,
                             dryrun=dryrun,
                             complete_list=complete_list
                             )
            partnumber += 1
            yield th

    # download part from src. s3 and upload to dest. s3
    def woker_thread(*, stop_signal, partnumber, partStartIndex, total, md5list, dryrun, complete_list):
        if stop_signal.is_set():
            return "TIMEOUT"
        Src_bucket = job['Src_bucket']
        Src_key = job['Src_key']
        Des_bucket = job['Des_bucket']
        Des_key = job['Des_key']
        versionId = job['versionId']
        getBody, chunkdata_md5 = b'', b''  # init

        # 下载文件
        if ifVerifyMD5Twice or not dryrun:  # 如果 ifVerifyMD5Twice 则无论是否已有上传过都重新下载，作为校验整个文件用
            if GetObjectWithVersionId:
                logger.info(f"--->Downloading {ChunkSize} Bytes {Src_bucket}/{Src_key} - {partnumber}/{total}"
                            f" - versionId: {versionId}")
            else:
                logger.info(f"--->Downloading {ChunkSize} Bytes {Src_bucket}/{Src_key} - {partnumber}/{total}")
            retryTime = 0

            # 正常工作情况下出现 stop_signal 需要退出 Thread
            while retryTime <= MaxRetry and not stop_signal.is_set():
                retryTime += 1
                try:
                    if GetObjectWithVersionId:  # 按VersionId获取Object
                        response_get_object = s3_src_client.get_object(
                            Bucket=Src_bucket,
                            Key=Src_key,
                            VersionId=versionId,
                            Range="bytes=" + str(partStartIndex) + "-" + str(partStartIndex + ChunkSize - 1)
                        )
                    else:  # 不带VersionId，即获取最新对象
                        response_get_object = s3_src_client.get_object(
                            Bucket=Src_bucket,
                            Key=Src_key,
                            Range="bytes=" + str(partStartIndex) + "-" + str(partStartIndex + ChunkSize - 1)
                        )
                    getBody = response_get_object["Body"].read()
                    chunkdata_md5 = hashlib.md5(getBody)
                    md5list[partnumber - 1] = chunkdata_md5
                    break  # 完成下载，不用重试
                except ClientError as err:
                    if err.response['Error']['Code'] in ['NoSuchKey', 'AccessDenied']:
                        # 没这个ID，文件已经删除，或者无权限访问
                        logger.error(f"ClientError: Fail to access {Src_bucket}/{Src_key} - ERR: {str(err)}.")
                        stop_signal.set()
                        return "QUIT"
                    logger.warning(f"ClientError: Fail to download {Src_bucket}/{Src_key} - ERR: {str(err)}. "
                                   f"Retry part: {partnumber} - Attempts: {retryTime}")
                    if retryTime >= MaxRetry:  # 超过次数退出
                        logger.error(f"ClientError: Quit for Max Download retries: {retryTime} - "
                                     f"{Src_bucket}/{Src_key}")
                        stop_signal.set()
                        return "TIMEOUT"  # 退出Thread
                    else:
                        time.sleep(5 * retryTime)
                        continue
                        # 递增延迟，返回重试
                except Exception as e:
                    logger.warning(f"Fail to download {Src_bucket}/{Src_key} - ERR: {str(e)}. "
                                   f"Retry part: {partnumber} - Attempts: {retryTime}")
                    if retryTime >= MaxRetry:  # 超过次数退出
                        logger.error(f"Quit for Max Download retries: {retryTime} - {Src_bucket}/{Src_key}")
                        stop_signal.set()
                        return "TIMEOUT"  # 退出Thread
                    else:
                        time.sleep(5 * retryTime)
                        continue
        # 上传文件
        if not dryrun:  # 这里就不用考虑 ifVerifyMD5Twice 了，
            retryTime = 0
            while retryTime <= MaxRetry and not stop_signal.is_set():
                retryTime += 1
                try:
                    logger.info(f'--->Uploading {len(getBody)} Bytes {Des_bucket}/{Des_key} - {partnumber}/{total}')
                    s3_des_client.upload_part(
                        Body=getBody,
                        Bucket=Des_bucket,
                        Key=Des_key,
                        PartNumber=partnumber,
                        UploadId=uploadId,
                        ContentMD5=base64.b64encode(chunkdata_md5.digest()).decode('utf-8')
                    )
                    # 请求已经带上md5，如果s3校验是错的就Exception
                    break
                except ClientError as err:
                    if err.response['Error']['Code'] == 'NoSuchUpload':
                        # 没这个ID，则是别人已经完成这个Job了。
                        logger.warning(f'ClientError: Fail to upload part - might be duplicated job:'
                                       f' {Des_bucket}/{Des_key}, {str(err)}')
                        stop_signal.set()
                        return "QUIT"
                    logger.warning(f"ClientError: Fail to upload part - {Des_bucket}/{Des_key} -  {str(err)}, "
                                   f"retry part: {partnumber} Attempts: {retryTime}")
                    if retryTime >= MaxRetry:
                        logger.error(f"ClientError: Quit for Max Upload retries: {retryTime} - {Des_bucket}/{Des_key}")
                        # 改为跳下一个文件
                        stop_signal.set()
                        return "TIMEOUT"
                    else:
                        time.sleep(5 * retryTime)  # 递增延迟重试
                        continue
                except Exception as e:
                    logger.warning(f"Fail to upload part - {Des_bucket}/{Des_key} -  {str(e)}, "
                                   f"retry part: {partnumber} Attempts: {retryTime}")
                    if retryTime >= MaxRetry:
                        logger.error(f"Quit for Max Upload retries: {retryTime} - {Des_bucket}/{Des_key}")
                        # 改为跳下一个文件
                        stop_signal.set()
                        return "TIMEOUT"
                    else:
                        time.sleep(5 * retryTime)
                        continue

        if not stop_signal.is_set():
            complete_list.append(partnumber)
            if not dryrun:
                logger.info(
                    f'--->Complete {len(getBody)} Bytes {Src_bucket}/{Src_key}'
                    f' - {partnumber}/{total} {len(complete_list) / total:.2%}')
        else:
            return "TIMEOUT"
        return "COMPLETE"

    # woker_thread END

    # job_processor Main
    partnumber = 1  # 当前循环要上传的Partnumber
    total = len(indexList)
    md5list = [hashlib.md5(b'')] * total
    complete_list = []

    # 线程池
    try:
        stop_signal = threading.Event()  # 用于JobTimeout终止当前文件的所有线程
        with concurrent.futures.ThreadPoolExecutor(max_workers=MaxThread) as pool:
            # 这里要用迭代器拿到threads对象
            threads = list(thread_gen(woker_thread, pool, stop_signal,
                                      partnumber, total, md5list, partnumberList, complete_list))

            result = concurrent.futures.wait(threads, timeout=JobTimeout, return_when="ALL_COMPLETED")

            # 异常退出
            if "QUIT" in [t.result() for t in result[0]]:  # result[0] 是函数done
                logger.warning(f'QUIT. Canceling {len(result[1])} waiting threads in pool ...')
                stop_signal.set()
                for t in result[1]:
                    t.cancel()
                logger.warning(f'QUIT Job: {job["Src_bucket"]}/{job["Src_key"]}')
                return "QUIT"
            # 超时
            if len(result[1]) > 0:  # # result[0] 是函数not_done, 即timeout有未完成的
                logger.warning(f'TIMEOUT. Canceling {len(result[1])} waiting threads in pool ...')
                stop_signal.set()
                for t in result[1]:
                    t.cancel()
                logger.warning(f'TIMEOUT {JobTimeout}S Job: {job["Src_bucket"]}/{job["Src_key"]}')
                return "TIMEOUT"

        # 线程池End
        logger.info(f'All parts uploaded: {job["Src_bucket"]}/{job["Src_key"]} - Size:{job["Size"]}')

        # 计算所有分片列表的总etag: cal_etag
        digests = b"".join(m.digest() for m in md5list)
        md5full = hashlib.md5(digests)
        cal_etag = '"%s-%s"' % (md5full.hexdigest(), len(md5list))
    except Exception as e:
        logger.error(f'Exception in job_processor: {str(e)}')
        return "ERR"
    return cal_etag


# Complete multipart upload
# 通过查询回来的所有Part列表uploadedListParts来构建completeStructJSON
def completeUpload(*, uploadId, Des_bucket, Des_key, len_indexList, s3_des_client):
    # 查询S3的所有Part列表uploadedListParts构建completeStructJSON
    uploadedListPartsClean = []
    logger.info(f'Get partnumber list - {Des_bucket}/{Des_key}')
    paginator = s3_des_client.get_paginator('list_parts')
    try:
        response_iterator = paginator.paginate(
            Bucket=Des_bucket,
            Key=Des_key,
            UploadId=uploadId
        )
        # 把 ETag 加入到 Part List
        for page in response_iterator:
            if "Parts" in page:
                logger.info(f'Got list_parts: {len(page["Parts"])} - {Des_bucket}/{Des_key}')
                for p in page["Parts"]:
                    uploadedListPartsClean.append({
                        "ETag": p["ETag"],
                        "PartNumber": p["PartNumber"]
                    })
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchUpload':
            # 没这个ID，则是别人已经完成这个Job了。
            logger.warning(f'ClientError: Fail to list parts while completeUpload - might be duplicated job:'
                           f' {Des_bucket}/{Des_key}, {str(err)}')
            return "QUIT"
        logger.error(f'ClientError: Fail to list parts while completeUpload - {Des_bucket}/{Des_key} - {str(e)}')
        return "ERR"
    except Exception as e:
        logger.error(f'Fail to list parts while completeUpload - {Des_bucket}/{Des_key} - {str(e)}')
        return "ERR"
    if len(uploadedListPartsClean) != len_indexList:
        logger.error(f'Uploaded parts size not match - {Des_bucket}/{Des_key}')
        return "ERR"

    completeStructJSON = {"Parts": uploadedListPartsClean}

    # S3合并multipart upload任务
    try:
        logger.info(f'Try to merge multipart upload {Des_bucket}/{Des_key}')
        response_complete = s3_des_client.complete_multipart_upload(
            Bucket=Des_bucket,
            Key=Des_key,
            UploadId=uploadId,
            MultipartUpload=completeStructJSON
        )
        result = response_complete['ETag']
        logger.info(f'Merged: {Des_bucket}/{Des_key}')
    except Exception as e:
        logger.error(f'Fail to complete multipart upload {Des_bucket}/{Des_key}, {str(e)}')
        return "ERR"

    logger.info(f'Complete merge file {Des_bucket}/{Des_key}')
    return result


# Continuely get job message to invoke one processor per job
def job_looper(*, sqs, sqs_queue, table, s3_src_client, s3_des_client, instance_id,
               StorageClass, ChunkSize, MaxRetry, MaxThread, ResumableThreshold,
               JobTimeout, ifVerifyMD5Twice, CleanUnfinishedUpload,
               Des_bucket_default, Des_prefix_default, UpdateVersionId, GetObjectWithVersionId):
    while True:
        # Get Job from sqs
        try:
            logger.info('Get Job from sqs queue...')
            sqs_job_get = sqs.receive_message(QueueUrl=sqs_queue)

            # Empty queue message available
            if 'Messages' not in sqs_job_get:  # No message on sqs queue
                logger.info('No message in queue available, wait...')
                time.sleep(60)

            # 拿到 Job message
            else:
                # TODO: 尚未完整处理 SQS 存在多条消息的情况的意外中断处理，建议只用于针对一次取一个SQS消息
                for sqs_job in sqs_job_get["Messages"]:
                    job = json.loads(sqs_job["Body"])
                    job_receipt = sqs_job["ReceiptHandle"]  # 用于后面删除message

                    # 判断是S3来的消息，而不是jodsender来的就转换一下
                    if 'Records' in job:  # S3来的消息带着'Records'
                        for One_record in job['Records']:
                            if 's3' in One_record:
                                Src_bucket = One_record['s3']['bucket']['name']
                                Src_key = One_record['s3']['object']['key']
                                Src_key = urllib.parse.unquote_plus(Src_key)
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
                    if 'Des_bucket' not in job and 'Event' not in job:
                        logger.warning(f'Wrong sqs job: {json.dumps(job, default=str)}')
                        logger.warning('Try to handle next message')
                        time.sleep(1)
                        continue
                    if 'versionId' not in job:
                        job['versionId'] = 'null'

                    # 主流程
                    if 'Event' not in job:
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
                    else:
                        if job['Event'] == 's3:TestEvent':
                            logger.info('Skip s3:TestEvent')
                            upload_etag_full = "s3:TestEvent"
                        else:
                            upload_etag_full = "OtherEvent"

                    # Del Job on sqs
                    logger.info(f'upload_etag_full={upload_etag_full}, job={str(job)}')
                    if upload_etag_full != "TIMEOUT":
                        # 如果是超时的就不删SQS消息，是正常结束或QUIT就删
                        # QUIT 是 NoSuchUpload, NoSuchKey, AccessDenied，可以认为没必要再让下一个worker再试了
                        # 直接删除SQS，并且DDB并不会记录结束状态

                        try:
                            logger.info(f'Try to finsh job message on sqs. {str(job)}')
                            sqs.delete_message(
                                QueueUrl=sqs_queue,
                                ReceiptHandle=job_receipt
                            )
                        except Exception as e:
                            logger.error(f'Fail to delete sqs message: {str(sqs_job)}, {str(e)}')

        except Exception as e:
            logger.error(f'Fail. Wait for 5 seconds. ERR: {str(e)}')
            time.sleep(5)
        # Finish Job, go back to get next job in queue


# 清理S3上现有未完成的Multipart Upload ID（当前Job）
def clean_multipart_upload(*, s3_client, multipart_uploaded_list, Des_bucket):
    for clean_i in multipart_uploaded_list:
        try:
            s3_client.abort_multipart_upload(
                Bucket=Des_bucket,
                Key=clean_i["Key"],
                UploadId=clean_i["UploadId"]
            )
            logger.info(f'CLEAN FINISHED - {str(multipart_uploaded_list)}')
        except Exception as e:
            logger.error(f'Fail to clean - {str(multipart_uploaded_list)} - {str(e)}')


# Steps func for multipart upload for one job
def step_function(*, job, table, s3_src_client, s3_des_client, instance_id,
                  StorageClass, ChunkSize, MaxRetry, MaxThread,
                  JobTimeout, ifVerifyMD5Twice, CleanUnfinishedUpload, UpdateVersionId, GetObjectWithVersionId):
    # 正常开始处理
    Src_bucket = job['Src_bucket']
    Src_key = job['Src_key']
    Size = job['Size']
    Des_bucket = job['Des_bucket']
    Des_key = job['Des_key']
    versionId = job['versionId']
    upload_etag_full = ""
    logger.info(f'Start multipart: {Src_bucket}/{Src_key}, Size: {Size}, versionId: {versionId}')

    # Get dest s3 unfinish multipart upload of this file
    multipart_uploaded_list = get_uploaded_list(
        s3_client=s3_des_client,
        Des_bucket=Des_bucket,
        Des_key=Des_key
    )

    # 设置了CleanUnfinishedUpload，就清理S3上现有未完成的Multipart Upload ID（当前Job），不做断点续传
    if multipart_uploaded_list and CleanUnfinishedUpload:
        logger.warning(f'You set CleanUnfinishedUpload. Now clean: {str(multipart_uploaded_list)}')
        clean_multipart_upload(
            s3_client=s3_des_client,
            multipart_uploaded_list=multipart_uploaded_list,
            Des_bucket=Des_bucket
        )
        multipart_uploaded_list = []

    # 开始 Job 步骤
    # 循环重试3次（如果MD5计算的ETag，或VersionID不一致）
    for md5_retry in range(Max_md5_retry + 1):
        # Job 准备
        # 检查文件没Multipart UploadID要新建, 有则 return UploadID
        response_check_upload = check_file_exist(
            prefix_and_key=Des_key,
            UploadIdList=multipart_uploaded_list
        )
        if response_check_upload == 'UPLOAD':
            try:
                logger.info(f'Create multipart upload - {Des_bucket}/{Des_key}')
                response_new_upload = s3_des_client.create_multipart_upload(
                    Bucket=Des_bucket,
                    Key=Des_key,
                    StorageClass=StorageClass
                )
                # If update versionID enabled, update s3 versionID
                # 但可能会出现中断重传的时候，拿到了另一个新version，从而导致文件半老半新，所以需要在最后完成时候校验一次versionId
                if UpdateVersionId:
                    versionId, Size = head_s3_version(
                        s3_src_client=s3_src_client,
                        Src_bucket=Src_bucket,
                        Src_key=Src_key
                    )
                    if versionId == "ERR":
                        break
                    job['versionId'] = versionId
                    job['Size'] = Size
                reponse_uploadId = response_new_upload["UploadId"]
                partnumberList = []
                # Write log to DDB in first round of job
                # ddb_first_round(table, Src_bucket, Src_key, Size, versionId)
            except Exception as e:
                logger.error(f'Fail to create new multipart upload - {Des_bucket}/{Des_key} - {str(e)}')
                upload_etag_full = "ERR"
                break
        else:
            reponse_uploadId = response_check_upload
            logger.info(f'Resume upload id: {Des_bucket}/{Des_key}')
            # 获取已上传partnumberList
            partnumberList = checkPartnumberList(
                Des_bucket=Des_bucket,
                Des_key=Des_key,
                uploadId=reponse_uploadId,
                s3_des_client=s3_des_client
            )
            # Get versionId/Size from DDB，如果传输前或中断后文件被替换，Size可能改变，所以重新取启动时候的versionId和当时的Size
            if UpdateVersionId:
                versionId, Size = ddb_get(
                    table=table,
                    Src_bucket=Src_bucket,
                    Src_key=Src_key
                )
                if versionId == "ERR":
                    break
                job['versionId'] = versionId
                job['Size'] = Size
        # 获取文件拆分片索引列表，例如[0, 10, 20]
        indexList, ChunkSize_auto = split(
            Size,
            ChunkSize
        )  # 对于大于10000分片的大文件，自动调整为Chunksize_auto

        # Write log to DDB in first round of job
        percent = int(len(partnumberList) / len(indexList) * 100)
        # ddb_this_round(table, percent, Src_bucket, Src_key, instance_id)
        ddb_start(table=table,
                  percent=percent,
                  job=job,
                  instance_id=instance_id,
                  new_upload=(response_check_upload == 'UPLOAD'))

        # Job Thread: uploadPart, 超时或Key不对返回 TIMEOUT/QUIT
        upload_etag_full = job_processor(
            uploadId=reponse_uploadId,
            indexList=indexList,
            partnumberList=partnumberList,
            job=job,
            s3_src_client=s3_src_client,
            s3_des_client=s3_des_client,
            MaxThread=MaxThread,
            ChunkSize=ChunkSize_auto,  # 对单个文件使用自动调整的 Chunksize_auto
            MaxRetry=MaxRetry,
            JobTimeout=JobTimeout,
            ifVerifyMD5Twice=ifVerifyMD5Twice,
            GetObjectWithVersionId=GetObjectWithVersionId
        )
        if upload_etag_full == "TIMEOUT" or upload_etag_full == "QUIT":
            logger.warning(f'Quit job upload_etag_full == {upload_etag_full} - {str(job)}')
            break  # 退出处理该Job
        elif upload_etag_full == "ERR":
            # 清掉已上传id，以便重新上传
            logger.error(f'upload_etag_full ERR - {str(job)}')
            clean_multipart_upload(
                s3_client=s3_des_client,
                multipart_uploaded_list=multipart_uploaded_list,
                Des_bucket=Des_bucket
            )
            multipart_uploaded_list = []
            if md5_retry >= Max_md5_retry:
                logger.error(f'Quit job upload_etag_full ERR - {upload_etag_full} - {str(job)}')
                return 'ERR'
            continue
            # 循环重试Job

        # 合并S3上的文件
        complete_etag = completeUpload(
            uploadId=reponse_uploadId,
            Des_bucket=Des_bucket,
            Des_key=Des_key,
            len_indexList=len(indexList),
            s3_des_client=s3_des_client
        )
        if complete_etag == "ERR":
            # 清掉已上传id，以便重新上传
            logger.error(f'complete_etag ERR - {str(job)}')
            clean_multipart_upload(
                s3_client=s3_des_client,
                multipart_uploaded_list=multipart_uploaded_list,
                Des_bucket=Des_bucket
            )
            multipart_uploaded_list = []
            if md5_retry >= Max_md5_retry:
                logger.error(f'Quit job complete_etag - {upload_etag_full} - {str(job)}')
                return 'ERR'
            continue
            # 循环重试Job

        # 检查文件MD5
        if ifVerifyMD5Twice and complete_etag != "QUIT":  # QUIT是因为重复ID，所以就不检查了
            if complete_etag == upload_etag_full:
                logger.info(f'MD5 ETag Matched - {Des_bucket}/{Des_key} - {complete_etag}')
                break  # 结束本文件，下一个sqs job
            else:  # ETag 不匹配，删除目的S3的文件，重试
                logger.error(f'MD5 ETag NOT MATCHED {Des_bucket}/{Des_key}( Destination / Origin ): '
                             f'{complete_etag} - {upload_etag_full}')
                del_des_s3_object(
                    s3_des_client=s3_des_client,
                    Des_bucket=Des_bucket,
                    Des_key=Des_key
                )
                # 清掉已上传id，以便重新上传
                logger.error(f'complete_etag != upload_etag_full ERR - {str(job)}')
                clean_multipart_upload(
                    s3_client=s3_des_client,
                    multipart_uploaded_list=multipart_uploaded_list,
                    Des_bucket=Des_bucket
                )
                multipart_uploaded_list = []
                if md5_retry >= Max_md5_retry:
                    logger.error(f'Quit job ifVerifyMD5Twice - {upload_etag_full} - {str(job)}')
                    return 'ERR'
                continue
                # 重新执行Job

        # DynamoDB log: ADD status: DONE/ERR(upload_etag_full)
        ddb_complete(
            upload_etag_full=upload_etag_full,
            table=table,
            Src_bucket=Src_bucket,
            Src_key=Src_key
        )

        # 正常结束 md5_retry 循环
        break
    # END md5_retry 超过次数

    # complete one job
    return upload_etag_full


# delete des s3 object
def del_des_s3_object(*, s3_des_client, Des_bucket, Des_key):
    logger.info(f'Delete {Des_bucket}/{Des_key}')
    try:
        s3_des_client.delete_object(
            Bucket=Des_bucket,
            Key=Des_key
        )
    except Exception as e:
        logger.error(f'Fail to delete S3 object - {Des_bucket}/{Des_key} - {str(e)}')


# Get iteam versionId and Size from DDB
def ddb_get(*, table, Src_bucket, Src_key):
    logger.info(f'Get versionId and Size from DDB - {Src_bucket}/{Src_key}')
    table_key = str(PurePosixPath(Src_bucket) / Src_key)
    if Src_key[-1] == '/':  # 针对空目录对象
        table_key += '/'
    try:
        response = table.get_item(
            Key={"Key": table_key},
            AttributesToGet=["versionId", "Size"]
        )
        versionId = response['Item']['versionId']
        Size = response['Item']['Size']
        logger.info(f'Got VersionId: {versionId} - {Src_bucket}/{Src_key} - Size:{Size}')
    except Exception as e:
        logger.error(f'Fail to Get versionId and Size from DDB - {Src_bucket}/{Src_key} - {str(e)}')
        return 'ERR', 0
    return versionId, Size


# # Write log to DDB in first round of job
def ddb_start(*, table, percent, job, instance_id, new_upload):
    Src_bucket = job['Src_bucket']
    Src_key = job['Src_key']
    Size = job['Size']
    Des_bucket = job['Des_bucket']
    Des_key = job['Des_key']
    versionId = job['versionId']
    logger.info(f'Write log to DDB start job - {Src_bucket}/{Src_key}')
    cur_time = time.time()
    table_key = str(PurePosixPath(Src_bucket) / Src_key)
    if Src_key[-1] == '/':  # 针对空目录对象
        table_key += '/'
    UpdateExpression = "ADD instanceID :id, tryTimes :t, startTime_f :s_format " \
                       "SET lastTimeProgress=:p, Size=:size, desBucket=:b, desKey=:k"
    ExpressionAttributeValues = {
        ":t": 1,
        ":id": {instance_id},
        ":s_format": {time.asctime(time.localtime(cur_time))},
        ":size": Size,
        ":p": percent,
        ":b": Des_bucket,
        ":k": Des_key
    }
    if new_upload:
        logger.info(f'Update DDB <firstTime> - {Src_bucket}/{Src_key}')
        ExpressionAttributeValues[":s"] = int(cur_time)
        ExpressionAttributeValues[":v"] = versionId
        UpdateExpression += ", firstTime =:s, versionId =:v"
    try:
        table.update_item(
            Key={"Key": table_key},
            UpdateExpression=UpdateExpression,
            ExpressionAttributeValues=ExpressionAttributeValues
        )
    except Exception as e:
        # 日志写不了
        logger.error(f'Fail to put log to DDB at start job - {Src_bucket}/{Src_key} - {str(e)}')
        return


# DynamoDB log: ADD status: DONE/ERR(upload_etag_full)
def ddb_complete(*, upload_etag_full, table, Src_bucket, Src_key):
    if upload_etag_full not in ["TIMEOUT", "ERR", "QUIT"]:
        status = "DONE"
    else:
        status = upload_etag_full
    cur_time = time.time()
    table_key = str(PurePosixPath(Src_bucket) / Src_key)
    if Src_key[-1] == '/':  # 针对空目录对象
        table_key += '/'

    UpdateExpression = "ADD jobStatus :done SET totalSpentTime=:s-firstTime, endTime=:s, endTime_f=:e"
    ExpressionAttributeValues = {
        ":done": {status},
        ":s": int(cur_time),
        ":e": time.asctime(time.localtime(cur_time))
    }

    # 正常写DDB，如果是异常的就不加这个lastTimeProgress=100
    if status == "DONE":
        UpdateExpression += ", lastTimeProgress=:p"
        ExpressionAttributeValues[":p"] = 100

    # update DDB
    logger.info(f'Write job complete status to DDB: {status} - {Src_bucket}/{Src_key}')
    try:
        table.update_item(
            Key={"Key": table_key},
            UpdateExpression=UpdateExpression,
            ExpressionAttributeValues=ExpressionAttributeValues
        )
    except Exception as e:
        logger.error(f'Fail to put log to DDB at end - {Src_bucket}/{Src_key} - {str(e)}')
    return


def step_fn_small_file(*, job, table, s3_src_client, s3_des_client, instance_id, StorageClass, MaxRetry,
                       UpdateVersionId, GetObjectWithVersionId):
    # 开始处理小文件
    Src_bucket = job['Src_bucket']
    Src_key = job['Src_key']
    Size = job['Size']
    Des_bucket = job['Des_bucket']
    Des_key = job['Des_key']
    versionId = job['versionId']
    # If update versionID enabled, update s3 versionID
    if UpdateVersionId:
        versionId = head_s3_version(
                        s3_src_client=s3_src_client,
                        Src_bucket=Src_bucket,
                        Src_key=Src_key
                    )
        job['versionId'] = versionId

    logger.info(f'Start small: {Src_bucket}/{Src_key}, Size: {Size}, versionId: {versionId}')
    # Write DDB log for first round
    ddb_start(table=table,
              percent=0,
              job=job,
              instance_id=instance_id,
              new_upload=True)

    upload_etag_full = []
    for retryTime in range(MaxRetry + 1):
        try:
            # Get object
            logger.info(f'--->Downloading {Size} Bytes {Src_bucket}/{Src_key} - Small file 1/1')
            if GetObjectWithVersionId:
                response_get_object = s3_src_client.get_object(
                    Bucket=Src_bucket,
                    Key=Src_key,
                    VersionId=versionId
                )
            else:
                response_get_object = s3_src_client.get_object(
                    Bucket=Src_bucket,
                    Key=Src_key
                )
            getBody = response_get_object["Body"].read()
            chunkdata_md5 = hashlib.md5(getBody)
            ContentMD5 = base64.b64encode(chunkdata_md5.digest()).decode('utf-8')

            # Put object
            logger.info(f'--->Uploading {Size} Bytes {Des_bucket}/{Des_key} - Small file 1/1')
            response_put_object = s3_des_client.put_object(
                Body=getBody,
                Bucket=Des_bucket,
                Key=Des_key,
                ContentMD5=ContentMD5,
                StorageClass=StorageClass
            )
            # 请求已经带上md5，如果s3校验是错的就Exception
            upload_etag_full = response_put_object['ETag']
            # 结束 Upload/download
            break
        except ClientError as e:
            if e.response['Error']['Code'] in ['NoSuchKey', 'AccessDenied']:
                logger.error(f"Fail to access {Src_bucket}/{Src_key} - ERR: {str(e)}.")
                return "QUIT"
            logger.warning(f'Download/Upload small file Fail: {Src_bucket}/{Src_key}, '
                           f'{str(e)}, Attempts: {retryTime}')
            if retryTime >= MaxRetry:
                logger.error(f'Fail MaxRetry Download/Upload small file: {Des_bucket}/{Des_key}')
                return "TIMEOUT"
            else:
                time.sleep(5 * retryTime)
        except Exception as e:
            logger.error(f'Fail in step_fn_small - {Des_bucket}/{Des_key} - {str(e)}')
            return "TIMEOUT"

    # Write DDB log for complete
    ddb_complete(
        upload_etag_full=upload_etag_full,
        table=table,
        Src_bucket=Src_bucket,
        Src_key=Src_key
    )
    # complete one job
    return upload_etag_full
