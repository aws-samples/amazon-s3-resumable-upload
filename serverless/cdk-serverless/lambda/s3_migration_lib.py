# PROJECT LONGBOW - LIB FOR TRANSMISSION BETWEEN AMAZON S3

import time
import logging
import json
import os
from pathlib import PurePosixPath
import hashlib
import concurrent.futures
import threading
import base64
import sys

import urllib.request
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger()


# Configure logging
def set_log(LoggingLevel, this_file_name):
    _logger = logging.getLogger()
    _logger.setLevel(logging.WARNING)
    if LoggingLevel == 'INFO':
        _logger.setLevel(logging.INFO)
    elif LoggingLevel == 'DEBUG':
        _logger.setLevel(logging.DEBUG)
    # File logging
    file_path = os.path.split(os.path.abspath(__file__))[0]
    log_path = file_path + '/s3_migration_log'
    if not os.path.exists(log_path):
        os.system(f"mkdir {log_path}")
        print(f'Created folder {log_path}')
    else:
        print(f'Folder exist {log_path}')
    # this_file_name = os.path.splitext(os.path.basename(__file__))[0]
    t = time.localtime()
    start_time = f'{t.tm_year}-{t.tm_mon}-{t.tm_mday}-{t.tm_hour}-{t.tm_min}-{t.tm_sec}'
    _log_file_name = f'{log_path}/{this_file_name}-{start_time}.log'
    print('Logging level:', LoggingLevel, 'Log file:', _log_file_name)
    fileHandler = logging.FileHandler(filename=_log_file_name)
    fileHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
    _logger.addHandler(fileHandler)
    # Screen stream logging INFO 模式下在当前屏幕也输出，便于debug监控。
    # if LoggingLevel == 'INFO':
    #     streamHandler = logging.StreamHandler()
    #     streamHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
    #     _logger.addHandler(streamHandler)
    return _logger, _log_file_name


# Set environment
def set_env(JobType, LocalProfileMode, table_queue_name, ssm_parameter_credentials):
    s3_config = Config(max_pool_connections=200)  # boto default 10

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
            logger.error(f'Fail to get {ssm_parameter_credentials} in SSM Parameter store, fix and restart Jobsender')
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

    table = dynamodb.Table(table_queue_name)
    table.wait_until_exists()
    sqs_queue = wait_sqs_available(sqs, table_queue_name)

    return sqs, sqs_queue, table, s3_src_client, s3_des_client, instance_id, ssm


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
    logging.info(f'ApproximateNumberOfMessagesNotVisible: {NotVisible}, ApproximateNumberOfMessages: {Visible}')
    if NotVisible == '0' and (Visible == '0' or Visible == '1'):
        # In init state, the new created bucket trigger SQS will send one test message to SQS.
        # So here to ignore the case Visible == '1'
        return True  # sqs is empty
    return False  # sqs is not empty


def get_s3_file_list(s3_client, bucket, S3Prefix):
    logger.info(f'Get s3 file list from bucket: {bucket}')

    # get s3 file list with loop retry every 5 sec
    while True:
        des_file_list = []
        try:
            response_fileList = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=S3Prefix,
                MaxKeys=1000
            )
            break
        except Exception as err:
            logger.warning(f'Fail to get s3 list objests: {str(err)}')
            time.sleep(5)
            logger.warning(f'Retry get S3 list bucket: {bucket}')

    if response_fileList["KeyCount"] != 0:
        for n in response_fileList["Contents"]:
            # if n["Size"] != 0:  # 子目录或 0 size 文件，不处理
            des_file_list.append({
                "Key": n["Key"],
                "Size": n["Size"]
            })
            # else:
            #     logger.warning(f'Zero size file, skip: {bucket}/{n["Key"]}')

        # IsTruncated, keep getting next lists
        while response_fileList["IsTruncated"]:
            # Get next part of s3 list
            while True:
                try:
                    response_fileList = s3_client.list_objects_v2(
                        Bucket=bucket,
                        Prefix=S3Prefix,
                        MaxKeys=1000,
                        ContinuationToken=response_fileList["NextContinuationToken"]
                    )
                    break
                except Exception as err:
                    logger.warning(str(err))
                    time.sleep(5)
                    logger.warning(f'Retry get S3 list bucket: {bucket}')

            for n in response_fileList["Contents"]:
                # if n["Size"] != 0:  # 子目录或 0 size 文件，不处理
                des_file_list.append({
                    "Key": n["Key"],
                    "Size": n["Size"]
                })
                # else:
                #     logger.warning(f'Zero size file, skip: {bucket}/{n["Key"]}')
        logger.info(f'Bucket list length：{str(len(des_file_list))}')
    else:
        logger.info(f'File list is empty in: {bucket}')

    return des_file_list


def delta_job_list(src_file_list, des_file_list, bucket_para, ignore_list):
    src_bucket = bucket_para['src_bucket']
    src_prefix = str(PurePosixPath(bucket_para['src_prefix']))
    des_bucket = bucket_para['des_bucket']
    des_prefix = str(PurePosixPath(bucket_para['des_prefix']))
    if des_prefix == '.':
        dp_len = 0
    else:
        dp_len = len(des_prefix) + 1  # 目的bucket的 "prefix/"长度
    # Delta list
    logger.info(f'Compare source s3://{src_bucket}/{src_prefix} and '
                f'destination s3://{des_bucket}/{des_prefix}')
    start_time = int(time.time())
    job_list = []
    ignore_records = []
    for src in src_file_list:

        # 排除掉 ignore_list 里面列的 bucket/key
        src_bucket_key = src_bucket+'/'+src['Key']
        ignore_match = False
        # 每个 ignore key 匹配一次
        for ignore_key in ignore_list:
            # 前缀 Wildcard 匹配
            if ignore_key[-1] == '*':
                if src_bucket_key.startswith(ignore_key[:-1]):  # 匹配上
                    ignore_match = True
                    break
                # 匹配不上，循环下一个 ignore_key
            # 后缀 Wildcard 匹配
            elif ignore_key[0] == '*':
                if src_bucket_key.endswith(ignore_key[1:]):  # 匹配上
                    ignore_match = True
                    break
                # 匹配不上，循环下一个 ignore_key
            # 精确匹配
            else:
                if ignore_key == src_bucket_key:  # 匹配上
                    ignore_match = True
                    break
                # 匹配不上，循环下一个 ignore_key
        if ignore_match:
            ignore_records.append(src_bucket_key)
            continue  # 跳过当前 src

        # 比对源文件是否在目标中
        in_list = False
        for des in des_file_list:
            # 去掉目的bucket的prefix做Key对比，且Size一致，则判为存在，不加入上传列表
            if des['Key'][dp_len:] == src['Key'] and des['Size'] == src['Size']:
                in_list = True
                break
        # 单个源文件比对结束
        if in_list:
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
    logger.info(f'Generate delta file list LENGTH: {len(job_list)} - SPENT TIME: {spent_time}S')
    return job_list, ignore_records


def wait_sqs_available(sqs, table_queue_name):
    while True:
        try:
            return sqs.get_queue_url(QueueName=table_queue_name)['QueueUrl']
        except Exception as e:
            logger.warning(f'Waiting for SQS availability. {str(e)}')
            time.sleep(10)


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


# Get unfinished multipart upload id from s3
def get_uploaded_list(s3_client, Des_bucket, Des_key, MaxRetry):
    NextKeyMarker = ''
    IsTruncated = True
    multipart_uploaded_list = []
    while IsTruncated:
        IsTruncated = False
        for retry in range(MaxRetry + 1):
            try:
                logger.info(f'Getting unfinished upload id list {retry} retry {Des_bucket}/{Des_key}...')
                list_multipart_uploads = s3_client.list_multipart_uploads(
                    Bucket=Des_bucket,
                    Prefix=Des_key,
                    MaxUploads=1000,
                    KeyMarker=NextKeyMarker
                )
                IsTruncated = list_multipart_uploads["IsTruncated"]
                NextKeyMarker = list_multipart_uploads["NextKeyMarker"]
                if "Uploads" in list_multipart_uploads:
                    for i in list_multipart_uploads["Uploads"]:
                        if i["Key"] == Des_key:
                            multipart_uploaded_list.append({
                                "Key": i["Key"],
                                "Initiated": i["Initiated"],
                                "UploadId": i["UploadId"]
                            })
                            logger.info(f'Unfinished upload, Key: {i["Key"]}, Time: {i["Initiated"]}')
                break  # 退出重试循环
            except Exception as e:
                logger.warning(f'Fail to list multipart upload {str(e)}')
                if retry >= MaxRetry:
                    logger.error(f'Fail MaxRetry list multipart upload {str(e)}')
                    return []
                else:
                    time.sleep(5 * retry)

    return multipart_uploaded_list


# Check file on the list from get_uploaded_list and get created multipart id
def check_file_exist(prefix_and_key, UploadIdList):
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
def checkPartnumberList(Des_bucket, Des_key, uploadId, s3_des_client, MaxRetry=10):
    partnumberList = []
    PartNumberMarker = 0
    IsTruncated = True
    while IsTruncated:
        IsTruncated = False
        for retry in range(MaxRetry + 1):
            try:
                logger.info(f'Get partnumber list {retry} retry, PartNumberMarker: {PartNumberMarker}...')
                response_uploadedList = s3_des_client.list_parts(
                    Bucket=Des_bucket,
                    Key=Des_key,
                    UploadId=uploadId,
                    MaxParts=1000,
                    PartNumberMarker=PartNumberMarker
                )
                PartNumberMarker = response_uploadedList['NextPartNumberMarker']
                IsTruncated = response_uploadedList['IsTruncated']
                if 'Parts' in response_uploadedList:
                    logger.info(f'Response part number list len: {len(response_uploadedList["Parts"])}')
                    for partnumberObject in response_uploadedList["Parts"]:
                        partnumberList.append(partnumberObject["PartNumber"])
                break
            except Exception as e:
                logger.warning(f'Fail to list parts in checkPartnumberList. {str(e)}')
                if retry >= MaxRetry:
                    logger.error(f'Fail MaxRetry list parts in checkPartnumberList. {str(e)}')
                    return []
                else:
                    time.sleep(5 * retry)
        # 循环完成获取list

    if partnumberList:  # 如果空则表示没有查到已上传的Part
        logger.info(f"Found uploaded partnumber: {len(partnumberList)} - {json.dumps(partnumberList)}")
    else:
        logger.info(f'Part number list is empty')
    return partnumberList


# Process one job
def job_processor(uploadId, indexList, partnumberList, job, s3_src_client, s3_des_client,
                  MaxThread, ChunkSize, MaxRetry, JobTimeout, ifVerifyMD5Twice):
    # 线程生成器，配合thread pool给出每个线程的对应关系，便于设置超时控制
    def thread_gen(woker_thread, pool,
                   stop_signal, partnumber, total, md5list, partnumberList, complete_list):
        for partStartIndex in indexList:
            # start to upload part
            if partnumber not in partnumberList:
                dryrun = False  # dryrun 是为了沿用现有的流程做出完成列表，方便后面计算 MD5
            else:
                dryrun = True
            th = pool.submit(woker_thread, stop_signal, partnumber, partStartIndex,
                             total, md5list, dryrun, complete_list)
            partnumber += 1
            yield th

    # download part from src. s3 and upload to dest. s3
    def woker_thread(stop_signal, partnumber, partStartIndex, total, md5list, dryrun, complete_list):
        if stop_signal.is_set():
            return "TIMEOUT"
        Src_bucket = job['Src_bucket']
        Src_key = job['Src_key']
        Des_bucket = job['Des_bucket']
        Des_key = job['Des_key']

        # 下载文件
        if ifVerifyMD5Twice or not dryrun:  # 如果 ifVerifyMD5Twice 则无论是否已有上传过都重新下载，作为校验整个文件用

            if not dryrun:
                logger.info(f"--->Downloading {ChunkSize} Bytes {Src_bucket}/{Src_key} - {partnumber}/{total}")
            else:
                logger.info(
                    f"--->Downloading {ChunkSize} Bytes for verify MD5 {Src_bucket}/{Src_key} - {partnumber}/{total}")
            retryTime = 0

            # 正常工作情况下出现 stop_signal 需要退出 Thread
            while retryTime <= MaxRetry and not stop_signal.is_set():
                retryTime += 1
                try:
                    response_get_object = s3_src_client.get_object(
                        Bucket=Src_bucket,
                        Key=Src_key,
                        Range="bytes=" + str(partStartIndex) + "-" + str(partStartIndex + ChunkSize - 1)
                    )
                    getBody = response_get_object["Body"].read()
                    chunkdata_md5 = hashlib.md5(getBody)
                    md5list[partnumber - 1] = chunkdata_md5
                    break  # 完成下载，不用重试
                except Exception as err:
                    logger.warning(f"DownloadThreadFunc - {Src_bucket}/{Src_key} - Exception log: {str(err)}. "
                                   f"Download part fail, retry part: {partnumber} Attempts: {retryTime}")
                    if retryTime > MaxRetry:
                        logger.error(f"Quit for Max Download retries: {retryTime} - {Src_bucket}/{Src_key}")
                        # 超过次数退出，改为跳下一个文件
                        stop_signal.set()
                        return "MaxRetry"  # 退出Thread
                    else:
                        time.sleep(5 * retryTime)
                        # 递增延迟，返回重试
        # 上传文件
        if not dryrun:  # 这里就不用考虑 ifVerifyMD5Twice 了，

            retryTime = 0
            while retryTime <= MaxRetry and not stop_signal.is_set():
                retryTime += 1
                try:
                    logger.info(f'--->Uploading {ChunkSize} Bytes {Des_bucket}/{Des_key} - {partnumber}/{total}')
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
                        return "ERR"
                    logger.warning(f"ClientError: Fail to upload part - {Des_bucket}/{Des_key} -  {str(err)}, "
                                   f"retry part: {partnumber} Attempts: {retryTime}")
                    if retryTime > MaxRetry:
                        logger.error(f"ClientError: Quit for Max Upload retries: {retryTime} - {Des_bucket}/{Des_key}")
                        # 改为跳下一个文件
                        stop_signal.set()
                        return "MaxRetry"
                    else:
                        time.sleep(5 * retryTime)  # 递增延迟重试
                except Exception as err:
                    logger.warning(f"Exception: Fail to upload part - {Des_bucket}/{Des_key} -  {str(err)}, "
                                   f"retry part: {partnumber} Attempts: {retryTime}")
                    if retryTime > MaxRetry:
                        logger.error(f"Exception: Quit for Max Upload retries: {retryTime} - {Des_bucket}/{Des_key}")
                        # 改为跳下一个文件
                        stop_signal.set()
                        return "MaxRetry"
                    else:
                        time.sleep(5 * retryTime)  # 递增延迟重试

        if not stop_signal.is_set():
            complete_list.append(partnumber)
            if not dryrun:
                logger.info(
                    f'--->Complete {ChunkSize} Bytes {Src_bucket}/{Src_key} - {partnumber}/{total} {len(complete_list) / total:.2%}')
        else:
            return "TIMEOUT"
        return "Complete"

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
            if len(result[1]) > 0:
                logger.warning(f'Canceling {len(result[1])} threads...')
                stop_signal.set()
                for t in result[1]:
                    t.cancel()

        if stop_signal.is_set():
            logger.warning(f'TIMEOUT {JobTimeout}S or MaxRetry, Job: {job["Src_bucket"]}/{job["Src_key"]}')
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
def completeUpload(uploadId, Des_bucket, Des_key, len_indexList, s3_des_client, MaxRetry):
    # 查询S3的所有Part列表uploadedListParts构建completeStructJSON
    # 发现跟checkPartnumberList有点像，但计算Etag不同，隔太久了，懒得合并了 :)
    uploadedListPartsClean = []
    PartNumberMarker = 0
    IsTruncated = True
    while IsTruncated:
        IsTruncated = False
        for retryTime in range(MaxRetry + 1):
            try:
                logger.info(f'Get complete partnumber list {retryTime} retry, PartNumberMarker: {PartNumberMarker}...')
                response_uploadedList = s3_des_client.list_parts(
                    Bucket=Des_bucket,
                    Key=Des_key,
                    UploadId=uploadId,
                    MaxParts=1000,
                    PartNumberMarker=PartNumberMarker
                )
                NextPartNumberMarker = response_uploadedList['NextPartNumberMarker']
                IsTruncated = response_uploadedList['IsTruncated']
                # 把 ETag 加入到 Part List
                for partObject in response_uploadedList["Parts"]:
                    ETag = partObject["ETag"]
                    PartNumber = partObject["PartNumber"]
                    addup = {
                        "ETag": ETag,
                        "PartNumber": PartNumber
                    }
                    uploadedListPartsClean.append(addup)
                PartNumberMarker = NextPartNumberMarker
                break
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchUpload':
                    # Fail to list part list，没这个ID，则是别人已经完成这个Job了。
                    logger.warning(f'Fail to list parts while completeUpload, might be duplicated job:'
                                   f' {Des_bucket}/{Des_key}, {str(e)}')
                    return "ERR"
                logger.warning(f'Fail to list parts while completeUpload {Des_bucket}/{Des_key}, {str(e)}')
                if retryTime >= MaxRetry:
                    logger.error(f'Fail MaxRetry list parts while completeUpload {Des_bucket}/{Des_key}')
                    return "ERR"
                else:
                    time.sleep(5 * retryTime)
            except Exception as e:
                logger.warning(f'Fail to list parts while completeUpload {Des_bucket}/{Des_key}, {str(e)}')
                if retryTime >= MaxRetry:
                    logger.error(f'Fail MaxRetry list parts while completeUpload {Des_bucket}/{Des_key}')
                    return "ERR"
                else:
                    time.sleep(5 * retryTime)
        # 循环获取直到拿完全部parts

    if len(uploadedListPartsClean) != len_indexList:
        logger.warning(f'Uploaded parts size not match - {Des_bucket}/{Des_key}')
        return "ERR"
    completeStructJSON = {"Parts": uploadedListPartsClean}

    # S3合并multipart upload任务
    for retryTime in range(MaxRetry + 1):
        try:
            logger.info(f'Try to merge multipart upload {Des_bucket}/{Des_key}')
            response_complete = s3_des_client.complete_multipart_upload(
                Bucket=Des_bucket,
                Key=Des_key,
                UploadId=uploadId,
                MultipartUpload=completeStructJSON
            )
            result = response_complete['ETag']
            break
        except Exception as e:
            logger.warning(f'Fail to complete multipart upload {Des_bucket}/{Des_key}, {str(e)}')
            if retryTime >= MaxRetry:
                logger.error(f'Fail MaxRetry complete multipart upload {Des_bucket}/{Des_key}')
                return "ERR"
            else:
                time.sleep(5 * retryTime)
    logger.info(f'Complete merge file {Des_bucket}/{Des_key}')
    return result


# Continuely get job message to invoke one processor per job
def job_looper(sqs, sqs_queue, table, s3_src_client, s3_des_client, instance_id,
               StorageClass, ChunkSize, MaxRetry, MaxThread, ResumableThreshold,
               JobTimeout, ifVerifyMD5Twice, CleanUnfinishedUpload,
               Des_bucket_default, Des_prefix_default):
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
                # TODO: 尚未完整处理 SQS 存在多条消息的情况，实际只针对一次取一个SQS消息
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
                                Des_bucket, Des_prefix = Des_bucket_default, Des_prefix_default
                                Des_key = str(PurePosixPath(Des_prefix) / Src_key)
                                if Src_key[-1] == '/':  # 针对空目录对象
                                    table_key += '/'
                                job = {
                                    'Src_bucket': Src_bucket,
                                    'Src_key': Src_key,
                                    'Size': Size,
                                    'Des_bucket': Des_bucket,
                                    'Des_key': Des_key
                                }
                    if 'Des_bucket' not in job and 'Event' not in job:
                        logger.warning(f'Wrong sqs job: {json.dumps(job, default=str)}')
                        logger.warning('Try to handle next message')
                        time.sleep(1)
                        continue
                    ######## 主流程
                    if 'Event' not in job:
                        if job['Size'] > ResumableThreshold:
                            upload_etag_full = step_function(job, table, s3_src_client, s3_des_client, instance_id,
                                                             StorageClass, ChunkSize, MaxRetry, MaxThread,
                                                             JobTimeout, ifVerifyMD5Twice, CleanUnfinishedUpload)
                        else:
                            upload_etag_full = step_fn_small_file(job, table, s3_src_client, s3_des_client, instance_id,
                                                                  StorageClass, MaxRetry)
                    else:
                        if job['Event'] == 's3:TestEvent':
                            logger.info('Skip s3:TestEvent')
                            upload_etag_full = "s3:TestEvent"
                        else:
                            upload_etag_full = "OtherEvent"
                    ########
                    # Del Job on sqs
                    if upload_etag_full != "TIMEOUT" and upload_etag_full != "ERR":
                        # 如果是超时或ERR的就不删SQS消息，是正常结束就删
                        # 大文件会在退出线程时设 MaxRetry 为 TIMEOUT，小文件则会返回 MaxRetry
                        # 小文件出现该问题可以认为没必要再让下一个worker再试了，不是因为文件下载太大导致，而是权限设置导致
                        # 直接删除SQS，并且DDB并不会记录结束状态
                        # 如果希望小文件也继续让SQS消息恢复，并让下一个worker再试，则在上面判断加upload_etag_full != "MaxRetry"
                        for retry in range(MaxRetry + 1):
                            try:
                                logger.info(f'Try to finsh job message on sqs.')
                                sqs.delete_message(
                                    QueueUrl=sqs_queue,
                                    ReceiptHandle=job_receipt
                                )
                                break
                            except Exception as e:
                                logger.warning(f'Fail to delete sqs message: {str(sqs_job)}, {str(e)}')
                                if retry >= MaxRetry:
                                    logger.error(f'Fail MaxRetry delete sqs message: {str(sqs_job)}, {str(e)}')
                                else:
                                    time.sleep(5 * retry)

        except Exception as e:
            logger.error(f'Fail. Wait for 5 seconds. ERR: {str(e)}')
            time.sleep(5)
        # Finish Job, go back to get next job in queue


def step_function(job, table, s3_src_client, s3_des_client, instance_id,
                  StorageClass, ChunkSize, MaxRetry, MaxThread,
                  JobTimeout, ifVerifyMD5Twice, CleanUnfinishedUpload):
    # 正常开始处理
    Src_bucket = job['Src_bucket']
    Src_key = job['Src_key']
    Size = job['Size']
    Des_bucket = job['Des_bucket']
    Des_key = job['Des_key']
    logger.info(f'Start: {Src_bucket}/{Src_key}, Size: {Size}')

    # Get dest s3 unfinish multipart upload of this file
    multipart_uploaded_list = get_uploaded_list(s3_des_client, Des_bucket, Des_key, MaxRetry)

    # Debug用，清理S3上现有未完成的Multipart Upload ID（不只是当前Job，而对应目标Bucket上所有的）
    if multipart_uploaded_list and CleanUnfinishedUpload:
        logger.warning(f'You set CleanUnfinishedUpload. There are {len(multipart_uploaded_list)}.'
                       f' Now clean them and restart!')
        multipart_uploaded_list = get_uploaded_list(s3_des_client, Des_bucket, "", MaxRetry)
        for clean_i in multipart_uploaded_list:
            try:
                s3_des_client.abort_multipart_upload(
                    Bucket=Des_bucket,
                    Key=clean_i["Key"],
                    UploadId=clean_i["UploadId"]
                )
            except Exception as e:
                logger.error(f'Fail to clean {str(e)}')
        multipart_uploaded_list = []
        logger.info('CLEAN FINISHED')

    # 开始 Job 步骤
    # 循环重试3次（如果MD5计算的ETag不一致）
    for md5_retry in range(3):
        # Job 准备
        # 检查文件没Multipart UploadID要新建, 有则 return UploadID
        response_check_upload = check_file_exist(
            Des_key,
            multipart_uploaded_list
        )
        if response_check_upload == 'UPLOAD':
            try:
                logger.info(f'Create multipart upload: {Des_bucket}/{Des_key}')
                response_new_upload = s3_des_client.create_multipart_upload(
                    Bucket=Des_bucket,
                    Key=Des_key,
                    StorageClass=StorageClass
                )
                # Write log to DDB in first round of job
                ddb_first_round(table, Src_bucket, Src_key, Size, MaxRetry)
            except Exception as e:
                logger.warning(f'Fail to create new multipart upload. {str(e)}')
                if md5_retry >= 2:
                    upload_etag_full = "ERR"
                    break
                else:
                    time.sleep(5 * md5_retry)
                    continue
            # logger.info("UploadId: "+response_new_upload["UploadId"])
            reponse_uploadId = response_new_upload["UploadId"]
            partnumberList = []
        else:
            reponse_uploadId = response_check_upload
            logger.info(f'Resume upload id: {Des_bucket}/{Des_key}')
            # 获取已上传partnumberList
            partnumberList = checkPartnumberList(
                Des_bucket,
                Des_key,
                reponse_uploadId,
                s3_des_client,
                MaxRetry
            )

        # 获取文件拆分片索引列表，例如[0, 10, 20]
        indexList, ChunkSize_auto = split(
            Size,
            ChunkSize
        )  # 对于大于10000分片的大文件，自动调整为Chunksize_auto

        # Write log to DDB in first round of job
        percent = int(len(partnumberList) / len(indexList) * 100)
        ddb_this_round(table, percent, Src_bucket, Src_key, instance_id, MaxRetry)

        # Job Thread: uploadPart, 加入超时机制之后返回 "TIMEOUT"
        upload_etag_full = job_processor(
            reponse_uploadId,
            indexList,
            partnumberList,
            job,
            s3_src_client,
            s3_des_client,
            MaxThread,
            ChunkSize_auto,  # 对单个文件使用自动调整的 Chunksize_auto
            MaxRetry,
            JobTimeout,
            ifVerifyMD5Twice
        )
        if upload_etag_full == "TIMEOUT":
            break  # 超时退出处理该Job，因为sqs超时会被其他worker拿到
        elif upload_etag_full == "ERR":
            multipart_uploaded_list = []  # 清掉已上传id列表，以便重新上传
            continue  # 循环重试

        # 合并S3上的文件
        complete_etag = completeUpload(reponse_uploadId, Des_bucket, Des_key,
                                       len(indexList), s3_des_client, MaxRetry)
        logger.info(f'Merged: {Des_bucket}/{Des_key}')
        if complete_etag == "ERR":
            multipart_uploaded_list = []  # 清掉已上传id列表，以便重新上传
            continue  # 循环重试

        # 检查文件MD5
        if ifVerifyMD5Twice:
            if complete_etag == upload_etag_full:
                logger.info(f'MD5 ETag Matched - {Des_bucket}/{Des_key} - {complete_etag}')
                break  # 结束本文件，下一个sqs job
            else:  # ETag 不匹配，删除目的S3的文件，重试
                logger.warning(f'MD5 ETag NOT MATCHED {Des_bucket}/{Des_key}( Destination / Origin ): '
                               f'{complete_etag} - {upload_etag_full}')
                try:
                    s3_des_client.delete_object(
                        Bucket=Des_bucket,
                        Key=Des_key
                    )
                except Exception as e:
                    logger.warning(f'Fail to delete on S3. {str(e)}')
                multipart_uploaded_list = []
                if md5_retry >= 2:
                    logger.error(f'MD5 ETag NOT MATCHED Exceed Max Retries - {Des_bucket}/{Des_key}')
                    upload_etag_full = "ERR"
                else:
                    logger.warning(f'Retry {Des_bucket}/{Des_key}')
                    continue
        # 结束 Job
        break
    # DynamoDB log: ADD status: DONE/ERR(upload_etag_full)
    ddb_complete(upload_etag_full, table, Src_bucket, Src_key, MaxRetry)
    # complete one job
    return upload_etag_full


# Write log to DDB in first round of job
def ddb_first_round(table, Src_bucket, Src_key, Size, MaxRetry):
    for retry in range(MaxRetry + 1):
        try:
            logger.info(f'Write log to DDB in first round of job: {Src_bucket}/{Src_key}')
            cur_time = time.time()
            table_key = str(PurePosixPath(Src_bucket) / Src_key)
            if Src_key[-1] == '/':  # 针对空目录对象
                table_key += '/'
            table.update_item(
                Key={
                    "Key": table_key
                },
                UpdateExpression="SET firstTime=:s, firstTime_f=:s_format, Size=:size",
                ExpressionAttributeValues={
                    ":s": int(cur_time),
                    ":s_format": time.asctime(time.localtime(cur_time)),
                    ":size": Size
                }
            )
            break
        except Exception as e:
            # 日志写不了
            logger.warning(f'Fail to put log to DDB at starting this round: {Src_bucket}/{Src_key}, {str(e)}')
            if retry >= MaxRetry:
                logger.error(f'Fail MaxRetry put log to DDB at start {Src_bucket}/{Src_key}')
            else:
                time.sleep(5 * retry)


# Write log to DDB in first round of job
def ddb_start_small(table, Src_bucket, Src_key, Size, MaxRetry, instance_id):
    for retry in range(MaxRetry + 1):
        try:
            logger.info(f'Write log to DDB start small file job: {Src_bucket}/{Src_key}')
            cur_time = time.time()
            table_key = str(PurePosixPath(Src_bucket) / Src_key)
            if Src_key[-1] == '/':  # 针对空目录对象
                table_key += '/'
            table.update_item(
                Key={
                    "Key": table_key
                },
                UpdateExpression="ADD instanceID :id, tryTimes :t "
                                 "SET firstTime=:s, firstTime_f=:s_format, Size=:size",
                ExpressionAttributeValues={
                    ":t": 1,
                    ":id": {instance_id},
                    ":s": int(cur_time),
                    ":s_format": time.asctime(time.localtime(cur_time)),
                    ":size": Size
                }
            )
            break
        except Exception as e:
            # 日志写不了
            logger.warning(f'Fail to put log to DDB at starting small file job: {Src_bucket}/{Src_key}, {str(e)}')
            if retry >= MaxRetry:
                logger.error(f'Fail MaxRetry put log to DDB at start small file: {Src_bucket}/{Src_key}')
            else:
                time.sleep(5 * retry)


# DynamoDB log: ADD retry time, instance-id list, SET startTime of this round
def ddb_this_round(table, percent, Src_bucket, Src_key, instance_id, MaxRetry):
    for retry in range(MaxRetry + 1):
        try:
            logger.info(f'Write log to DDB via start this round of job: {Src_bucket}/{Src_key}')
            cur_time = time.time()
            table_key = str(PurePosixPath(Src_bucket) / Src_key)
            if Src_key[-1] == '/':  # 针对空目录对象
                table_key += '/'
            table.update_item(
                Key={
                    "Key": table_key
                },
                UpdateExpression="ADD instanceID :id, tryTimes :t "
                                 "SET thisRoundStart=:s, thisRoundStart_f=:s_format, lastTimeProgress=:p",
                ExpressionAttributeValues={
                    ":t": 1,
                    ":id": {instance_id},
                    ":s": int(cur_time),
                    ":s_format": time.asctime(time.localtime(cur_time)),
                    ":p": percent
                }
            )
            break
        except Exception as e:
            # 日志写不了
            logger.warning(f'Fail to put log to DDB at starting this round: {Src_bucket}/{Src_key}, {str(e)}')
            if retry >= MaxRetry:
                logger.error(f'Fail MaxRetry put log to DDB at start {Src_bucket}/{Src_key}')
            else:
                time.sleep(5 * retry)


# DynamoDB log: ADD status: DONE/ERR(upload_etag_full)
def ddb_complete(upload_etag_full, table, Src_bucket, Src_key, MaxRetry):
    status = "DONE"
    if upload_etag_full == "TIMEOUT":
        status = "TIMEOUT_or_MaxRetry"
    elif upload_etag_full == "ERR":
        status = "ERR"
    logger.info(f'Write job complete status to DDB: {status}')
    cur_time = time.time()
    table_key = str(PurePosixPath(Src_bucket) / Src_key)
    if Src_key[-1] == '/':  # 针对空目录对象
        table_key += '/'
    if status == "DONE":  # 正常写DDB
        UpdateExpression = "SET totalSpentTime=:s-firstTime, lastTimeProgress=:p, endTime=:s, endTime_f=:e" \
                           " ADD jobStatus :done"
        ExpressionAttributeValues = {
            ":done": {status},
            ":s": int(cur_time),
            ":p": 100,
            ":e": time.asctime(time.localtime(cur_time))
        }
    else:  # 状态异常，写DDB不能覆盖 lastTimeProgress
        UpdateExpression = "SET totalSpentTime=:s-firstTime, endTime=:s, endTime_f=:e" \
                           " ADD jobStatus :done"
        ExpressionAttributeValues = {
            ":done": {status},
            ":s": int(cur_time),
            ":e": time.asctime(time.localtime(cur_time))
        }
    for retry in range(MaxRetry + 1):
        try:

            table.update_item(
                Key={"Key": table_key},
                UpdateExpression=UpdateExpression,
                ExpressionAttributeValues=ExpressionAttributeValues
            )
            break
        except Exception as e:
            logger.warning(f'Fail to put log to DDB at end:{Src_bucket}/{Src_key} {str(e)}')
            if retry >= MaxRetry:
                logger.error(f'Fail MaxRetry to put log to DDB at end of job:{Src_bucket}/{Src_key} {str(e)}')
            else:
                time.sleep(5 * retry)


def step_fn_small_file(job, table, s3_src_client, s3_des_client, instance_id,
                       StorageClass, MaxRetry):
    # 开始处理小文件
    Src_bucket = job['Src_bucket']
    Src_key = job['Src_key']
    Size = job['Size']
    Des_bucket = job['Des_bucket']
    Des_key = job['Des_key']
    logger.info(f'Start small file procedure: {Src_bucket}/{Src_key}, Size: {Size}')

    # Write DDB log for first round
    ddb_start_small(table, Src_bucket, Src_key, Size, MaxRetry, instance_id)

    for retryTime in range(MaxRetry + 1):
        try:
            # Get object
            logger.info(f'--->Downloading {Size} Bytes {Src_bucket}/{Src_key} - Small file 1/1')
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
        except Exception as e:
            logger.warning(f'Download/Upload small file Fail: {Src_bucket}/{Src_key}, '
                           f'{str(e)}, Attempts: {retryTime}')
            if retryTime >= MaxRetry:
                logger.error(f'Fail MaxRetry Download/Upload small file: {Des_bucket}/{Des_key}')
                return "MaxRetry"
            else:
                time.sleep(5 * retryTime)

    # Write DDB log for complete
    ddb_complete(upload_etag_full, table, Src_bucket, Src_key, MaxRetry)
    # complete one job
    return upload_etag_full
