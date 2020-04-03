# -*- coding: utf-8 -*-
# PROJECT LONGBOW - AMAZON S3 UPLOAD TOOL WITH BREAK-POINT RESUMING
import os
import sys
import json
import base64
from boto3.session import Session
from botocore.client import Config
from concurrent import futures
from configparser import ConfigParser, RawConfigParser, NoOptionError
import time
import hashlib
import logging
from pathlib import PurePosixPath, Path
import platform
import codecs

global JobType, SrcFileIndex, DesProfileName, DesBucket, S3Prefix, MaxRetry, MaxThread, \
    MaxParallelFile, StorageClass, ifVerifyMD5, DontAskMeToClean, LoggingLevel, \
    SrcDir, SrcBucket, SrcProfileName, ali_SrcBucket, ali_access_key_id, ali_access_key_secret, ali_endpoint


# Read config.ini with GUI
def set_config():
    sys_para = sys.argv
    file_path = os.path.split(sys_para[0])[0]
    gui = False
    if platform.uname()[0] == 'Windows':  # Win默认打开
        gui = True
    if platform.uname()[0] == 'Linux':  # Linux 默认关闭
        gui = False
    if '--gui' in sys.argv:  # 指定 gui 模式
        gui = True
    if '--nogui' in sys.argv:  # 带 nogui 就覆盖前面Win打开要求
        gui = False

    JobType_list = ['LOCAL_TO_S3', 'S3_TO_S3', 'ALIOSS_TO_S3']
    StorageClass_list = ['STANDARD', 'REDUCED_REDUNDANCY', 'STANDARD_IA', 'ONEZONE_IA', 'INTELLIGENT_TIERING',
                         'GLACIER', 'DEEP_ARCHIVE']
    config_file = os.path.join(file_path, 's3_upload_config.ini')

    # If no config file, read the default config
    if not os.path.exists(config_file):
        config_file += '.default'
        print("No customized config, use the default config")
    cfg = ConfigParser()
    print(f'Reading config file: {config_file}')

    # Get local config value
    try:
        global JobType, SrcFileIndex, DesProfileName, DesBucket, S3Prefix, MaxRetry, MaxThread, \
            MaxParallelFile, StorageClass, ifVerifyMD5, DontAskMeToClean, LoggingLevel, \
            SrcDir, SrcBucket, SrcProfileName, ali_SrcBucket, ali_access_key_id, ali_access_key_secret, ali_endpoint
        cfg.read(config_file, encoding='utf-8-sig')
        JobType = cfg.get('Basic', 'JobType')
        SrcFileIndex = cfg.get('Basic', 'SrcFileIndex')
        DesProfileName = cfg.get('Basic', 'DesProfileName')
        DesBucket = cfg.get('Basic', 'DesBucket')
        S3Prefix = cfg.get('Basic', 'S3Prefix')
        Megabytes = 1024 * 1024
        ChunkSize = cfg.getint('Advanced', 'ChunkSize') * Megabytes
        MaxRetry = cfg.getint('Advanced', 'MaxRetry')
        MaxThread = cfg.getint('Advanced', 'MaxThread')
        MaxParallelFile = cfg.getint('Advanced', 'MaxParallelFile')
        StorageClass = cfg.get('Advanced', 'StorageClass')
        ifVerifyMD5 = cfg.getboolean('Advanced', 'ifVerifyMD5')
        DontAskMeToClean = cfg.getboolean('Advanced', 'DontAskMeToClean')
        LoggingLevel = cfg.get('Advanced', 'LoggingLevel')
        try:
            SrcDir = cfg.get('LOCAL_TO_S3', 'SrcDir')
        except NoOptionError:
            SrcDir = ''
        try:
            SrcBucket = cfg.get('S3_TO_S3', 'SrcBucket')
            SrcProfileName = cfg.get('S3_TO_S3', 'SrcProfileName')
        except NoOptionError:
            SrcBucket = ''
            SrcProfileName = ''
        try:
            ali_SrcBucket = cfg.get('ALIOSS_TO_S3', 'ali_SrcBucket')
            ali_access_key_id = cfg.get('ALIOSS_TO_S3', 'ali_access_key_id')
            ali_access_key_secret = cfg.get('ALIOSS_TO_S3', 'ali_access_key_secret')
            ali_endpoint = cfg.get('ALIOSS_TO_S3', 'ali_endpoint')
        except NoOptionError:
            ali_SrcBucket = ""
            ali_access_key_id = ""
            ali_access_key_secret = ""
            ali_endpoint = ""
    except Exception as e:
        print("ERR loading s3_upload_config.ini", str(e))
        input('PRESS ENTER TO QUIT')
        sys.exit(0)

    # GUI only well support LOCAL_TO_S3 mode, start with --gui option
    # For other JobTpe, GUI is not a prefer option since it's better run on EC2 Linux
    if gui:
        # For GUI
        from tkinter import Tk, filedialog, END, StringVar, BooleanVar, messagebox
        from tkinter.ttk import Combobox, Label, Button, Entry, Spinbox, Checkbutton
        # get profile name list in ./aws/credentials
        pro_conf = RawConfigParser()
        pro_path = os.path.join(os.path.expanduser("~"), ".aws")
        cre_path = os.path.join(pro_path, "credentials")
        if os.path.exists(cre_path):
            pro_conf.read(cre_path)
            profile_list = pro_conf.sections()
        else:
            print(f"There is no aws_access_key in {cre_path}, please input for Destination S3 Bucket: ")
            aws_access_key_id = input('aws_access_key_id: ')
            aws_secret_access_key = input('aws_secret_access_key: ')
            region = input('region: ')
            pro_conf.add_section('default')
            pro_conf['default']['aws_access_key_id'] = aws_access_key_id
            pro_conf['default']['aws_secret_access_key'] = aws_secret_access_key
            pro_conf['default']['region'] = region
            profile_list = ['default']
            with open(cre_path, 'w') as f:
                print(f"Saving credentials to {cre_path}")
                pro_conf.write(f)

        # Click Select Folder
        def browse_folder():
            local_dir = filedialog.askdirectory(initialdir=os.path.dirname(__file__))
            url_txt.delete(0, END)
            url_txt.insert(0, local_dir)
            file_txt.delete(0, END)
            file_txt.insert(0, "*")
            # Finsih browse folder

        # Click Select File
        def browse_file():
            local_file = filedialog.askopenfilename()
            url_txt.delete(0, END)
            url_txt.insert(0, os.path.split(local_file)[0])
            file_txt.delete(0, END)
            file_txt.insert(0, os.path.split(local_file)[1])
            # Finsih browse file

        # Click List Buckets
        def ListBuckets(*args):
            DesProfileName = DesProfileName_txt.get()
            client = Session(profile_name=DesProfileName).client('s3')
            bucket_list = []
            try:
                response = client.list_buckets()
                if 'Buckets' in response:
                    bucket_list = [b['Name'] for b in response['Buckets']]
            except Exception as e:
                messagebox.showerror('Error', f'Failt to List buckets. \n'
                                              f'Please verify your aws_access_key of profile: [{DesProfileName}]\n'
                                              f'{str(e)}')
                bucket_list = ['CAN_NOT_GET_BUCKET_LIST']
            DesBucket_txt['values'] = bucket_list
            DesBucket_txt.current(0)
            # Finish ListBuckets

        # Click List Prefix
        def ListPrefix(*args):
            DesProfileName = DesProfileName_txt.get()
            client = Session(profile_name=DesProfileName).client('s3')
            prefix_list = []
            this_bucket = DesBucket_txt.get()
            max_get = 100
            try:
                response = client.list_objects_v2(
                    Bucket=this_bucket,
                    Delimiter='/'
                )  # Only get the max 1000 prefix for simply list
                if 'CommonPrefixes' in response:
                    prefix_list = [c['Prefix'] for c in response['CommonPrefixes']]
                if not prefix_list:
                    messagebox.showinfo('Message', f'There is no "/" Prefix in: {this_bucket}')
                if response['IsTruncated']:
                    messagebox.showinfo('Message', f'More than {max_get} Prefix, cannot fully list here.')
            except Exception as e:
                messagebox.showinfo('Error', f'Cannot get prefix list from bucket: {this_bucket}, {str(e)}')
            S3Prefix_txt['values'] = prefix_list
            S3Prefix_txt.current(0)
            # Finish list prefix

        # Change JobType
        def job_change(*args):
            if JobType_mode.get() != 'LOCAL_TO_S3':
                messagebox.showinfo('Notice', 'S3_TO_S3 or OSS_TO_S3. \n'
                                              'Please config the rest hidden parameter in s3_upload_config.ini')
            # Finish JobType change message

        # Click START button
        def close():
            window.withdraw()
            ok = messagebox.askokcancel('Start uploading job',
                                        f'Upload from Local to \ns3://{DesBucket_txt.get()}/{S3Prefix_txt.get()}\n'
                                        f'Click OK to START')
            if not ok:
                window.deiconify()
                return
            window.quit()
            return
            # Finish close()

        # Start GUI
        window = Tk()
        window.title("LONGBOW - AMAZON S3 UPLOAD TOOL WITH BREAK-POINT RESUMING")
        window.geometry('705x350')
        window.configure(background='#ECECEC')
        window.protocol("WM_DELETE_WINDOW", sys.exit)

        Label(window, text='Job Type').grid(column=0, row=0, sticky='w', padx=2, pady=2)
        JobType_mode = Combobox(window, width=15, state="readonly")
        JobType_mode['values'] = tuple(JobType_list)
        JobType_mode.grid(column=1, row=0, sticky='w', padx=2, pady=2)
        if JobType in JobType_list:
            position = JobType_list.index(JobType)
            JobType_mode.current(position)
        else:
            JobType_mode.current(0)
        JobType_mode.bind("<<ComboboxSelected>>", job_change)

        Label(window, text="Folder").grid(column=0, row=1, sticky='w', padx=2, pady=2)
        url_txt = Entry(window, width=50)
        url_txt.grid(column=1, row=1, sticky='w', padx=2, pady=2)
        url_btn = Button(window, text="Select Folder", width=10, command=browse_folder)
        url_btn.grid(column=2, row=1, sticky='w', padx=2, pady=2)
        url_txt.insert(0, SrcDir)

        Label(window, text="Filename or *").grid(column=0, row=2, sticky='w', padx=2, pady=2)
        file_txt = Entry(window, width=50)
        file_txt.grid(column=1, row=2, sticky='w', padx=2, pady=2)
        file_btn = Button(window, text="Select File", width=10, command=browse_file)
        file_btn.grid(column=2, row=2, sticky='w', padx=2, pady=2)
        file_txt.insert(0, SrcFileIndex)

        Label(window, text="AWS Profile").grid(column=0, row=3, sticky='w', padx=2, pady=2)
        DesProfileName_txt = Combobox(window, width=15, state="readonly")
        DesProfileName_txt['values'] = tuple(profile_list)
        DesProfileName_txt.grid(column=1, row=3, sticky='w', padx=2, pady=2)
        if DesProfileName in profile_list:
            position = profile_list.index(DesProfileName)
            DesProfileName_txt.current(position)
        else:
            DesProfileName_txt.current(0)
        DesProfileName = DesProfileName_txt.get()
        DesProfileName_txt.bind("<<ComboboxSelected>>", ListBuckets)

        Label(window, text="S3 Bucket").grid(column=0, row=4, sticky='w', padx=2, pady=2)
        DesBucket_txt = Combobox(window, width=48)
        DesBucket_txt.grid(column=1, row=4, sticky='w', padx=2, pady=2)
        DesBucket_txt['values'] = DesBucket
        DesBucket_txt.current(0)
        Button(window, text="List Buckets", width=10, command=ListBuckets) \
            .grid(column=2, row=4, sticky='w', padx=2, pady=2)

        Label(window, text="S3 Prefix").grid(column=0, row=5, sticky='w', padx=2, pady=2)
        S3Prefix_txt = Combobox(window, width=48)
        S3Prefix_txt.grid(column=1, row=5, sticky='w', padx=2, pady=2)
        S3Prefix_txt['values'] = S3Prefix
        if S3Prefix != '':
            S3Prefix_txt.current(0)
        Button(window, text="List Prefix", width=10, command=ListPrefix) \
            .grid(column=2, row=5, sticky='w', padx=2, pady=2)

        Label(window, text="MaxThread/File").grid(column=0, row=6, sticky='w', padx=2, pady=2)
        if MaxThread < 1 or MaxThread > 100:
            MaxThread = 5
        var_t = StringVar()
        var_t.set(str(MaxThread))
        MaxThread_txt = Spinbox(window, from_=1, to=100, width=15, textvariable=var_t)
        MaxThread_txt.grid(column=1, row=6, sticky='w', padx=2, pady=2)

        Label(window, text="MaxParallelFile").grid(column=0, row=7, sticky='w', padx=2, pady=2)
        if MaxParallelFile < 1 or MaxParallelFile > 100:
            MaxParallelFile = 5
        var_f = StringVar()
        var_f.set(str(MaxParallelFile))
        MaxParallelFile_txt = Spinbox(window, from_=1, to=100, width=15, textvariable=var_f)
        MaxParallelFile_txt.grid(column=1, row=7, sticky='w', padx=2, pady=2)

        Label(window, text="S3 StorageClass").grid(column=0, row=8, sticky='w', padx=2, pady=2)
        StorageClass_txt = Combobox(window, width=15, state="readonly")
        StorageClass_txt['values'] = tuple(StorageClass_list)
        StorageClass_txt.grid(column=1, row=8, sticky='w', padx=2, pady=2)
        if StorageClass in StorageClass_list:
            position = StorageClass_list.index(StorageClass)
            StorageClass_txt.current(position)
        else:
            StorageClass_txt.current(0)

        save_config = BooleanVar()
        save_config.set(True)
        save_config_txt = Checkbutton(window, text="Save to s3_upload_config.ini", var=save_config)
        save_config_txt.grid(column=1, row=9, padx=2, pady=2)

        Button(window, text="Start Upload", width=15, command=close).grid(column=1, row=10, padx=5, pady=5)
        window.mainloop()

        JobType = JobType_mode.get()
        SrcDir = url_txt.get()
        SrcFileIndex = file_txt.get()
        DesBucket = DesBucket_txt.get()
        S3Prefix = S3Prefix_txt.get()
        DesProfileName = DesProfileName_txt.get()
        StorageClass = StorageClass_txt.get()
        MaxThread = int(MaxThread_txt.get())
        MaxParallelFile = int(MaxParallelFile_txt.get())

        if save_config:
            cfg['Basic']['JobType'] = JobType
            cfg['Basic']['DesProfileName'] = DesProfileName
            cfg['Basic']['DesBucket'] = DesBucket
            cfg['Basic']['S3Prefix'] = S3Prefix
            cfg['Advanced']['MaxThread'] = str(MaxThread)
            cfg['Advanced']['MaxParallelFile'] = str(MaxParallelFile)
            cfg['Advanced']['StorageClass'] = StorageClass
            cfg['LOCAL_TO_S3']['SrcDir'] = SrcDir
            cfg['Basic']['SrcFileIndex'] = SrcFileIndex
            config_file = os.path.join(file_path, 's3_upload_config.ini')
            with codecs.open(config_file, 'w', 'utf-8') as f:
                cfg.write(f)
                print(f"Save config to {config_file}")
        # GUI window finish

    # 校验
    if JobType not in JobType_list:
        print(f'ERR JobType: {JobType}, check config file: {config_file}')
        input('PRESS ENTER TO QUIT')
        sys.exit(0)
    # Finish set_config()
    return ChunkSize


# Configure logging
def set_log():
    logger = logging.getLogger()
    # File logging
    if not os.path.exists("./log"):
        os.system("mkdir log")
    this_file_name = os.path.splitext(os.path.basename(__file__))[0]
    t = time.localtime()
    file_time = f'{t.tm_year}-{t.tm_mon}-{t.tm_mday}-{t.tm_hour}-{t.tm_min}-{t.tm_sec}'
    log_file_name = './log/' + this_file_name + '-' + file_time + '.log'
    print('Logging to file:', os.path.abspath(log_file_name))
    print('Logging level:', LoggingLevel)
    fileHandler = logging.FileHandler(filename=log_file_name, encoding='utf-8')
    fileHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
    logger.addHandler(fileHandler)
    # Screen stream logging
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
    logger.addHandler(streamHandler)
    # Loggin Level
    logger.setLevel(logging.WARNING)
    if LoggingLevel == 'INFO':
        logger.setLevel(logging.INFO)
    elif LoggingLevel == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    return logger, log_file_name


# Get local file list
def get_local_file_list():
    __src_file_list = []
    try:
        if SrcFileIndex == "*":
            for parent, dirnames, filenames in os.walk(SrcDir):
                for filename in filenames:  # 遍历输出文件信息
                    file_absPath = os.path.join(parent, filename)
                    file_relativePath = file_absPath[len(SrcDir) + 1:]
                    file_size = os.path.getsize(file_absPath)
                    __src_file_list.append({
                        "Key": Path(file_relativePath),
                        "Size": file_size
                    })
        else:
            join_path = os.path.join(SrcDir, SrcFileIndex)
            file_size = os.path.getsize(join_path)
            __src_file_list = [{
                "Key": SrcFileIndex,
                "Size": file_size
            }]
    except Exception as err:
        logger.error('Can not get source files. ERR: ' + str(err))
        input('PRESS ENTER TO QUIT')
        sys.exit(0)
    if not __src_file_list:
        logger.error('Source file empty.')
        input('PRESS ENTER TO QUIT')
        sys.exit(0)
    return __src_file_list


# Get object list on S3
def get_s3_file_list(s3_client, bucket):
    logger.info('Get s3 file list ' + bucket)
    __des_file_list = []
    try:
        response_fileList = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=S3Prefix,
            MaxKeys=1000
        )
        if response_fileList["KeyCount"] != 0:
            for n in response_fileList["Contents"]:
                __des_file_list.append({
                    "Key": n["Key"],
                    "Size": n["Size"]
                })
            while response_fileList["IsTruncated"]:
                response_fileList = s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=S3Prefix,
                    MaxKeys=1000,
                    ContinuationToken=response_fileList["NextContinuationToken"]
                )
                if "Contents" in response_fileList:
                    for n in response_fileList["Contents"]:
                        __des_file_list.append({
                            "Key": n["Key"],
                            "Size": n["Size"]
                        })
        else:
            logger.info('File list is empty in the s3 bucket')
    except Exception as err:
        logger.error(str(err))
        input('PRESS ENTER TO QUIT')
        sys.exit(0)
    return __des_file_list


# Check single file on S3
def head_s3_single_file(s3_client, bucket):
    try:
        response_fileList = s3_client.head_object(
            Bucket=bucket,
            Key=S3Prefix + SrcFileIndex
        )
        file = [{
            "Key": S3Prefix + SrcFileIndex,
            "Size": response_fileList["ContentLength"]
        }]
    except Exception as err:
        logger.error(str(err))
        input('PRESS ENTER TO QUIT')
        sys.exit(0)
    return file


# Check single file on OSS
def head_oss_single_file(__ali_bucket):
    try:
        response_fileList = __ali_bucket.head_object(
            key=S3Prefix + SrcFileIndex
        )
        file = [{
            "Key": S3Prefix + SrcFileIndex,
            "Size": response_fileList.content_length
        }]
    except Exception as err:
        logger.error(str(err))
        input('PRESS ENTER TO QUIT')
        sys.exit(0)
    return file


# Get object list on OSS
def get_ali_oss_file_list(__ali_bucket):
    logger.info('Get oss file list ' + ali_SrcBucket)
    __des_file_list = []
    try:
        response_fileList = __ali_bucket.list_objects(
            prefix=S3Prefix,
            max_keys=1000
        )

        if len(response_fileList.object_list) != 0:
            for n in response_fileList.object_list:
                __des_file_list.append({
                    "Key": n.key,
                    "Size": n.size
                })
            while response_fileList.is_truncated:
                response_fileList = __ali_bucket.list_objects(
                    prefix=S3Prefix,
                    max_keys=1000,
                    marker=response_fileList.next_marker
                )
                for n in response_fileList.object_list:
                    __des_file_list.append({
                        "Key": n.key,
                        "Size": n.size
                    })
        else:
            logger.info('File list is empty in the ali_oss bucket')
    except Exception as err:
        logger.error(str(err))
        input('PRESS ENTER TO QUIT')
        sys.exit(0)
    return __des_file_list


# Get all exist object list on S3
def get_uploaded_list(s3_client):
    logger.info('Get unfinished multipart upload')
    NextKeyMarker = ''
    IsTruncated = True
    __multipart_uploaded_list = []
    while IsTruncated:
        list_multipart_uploads = s3_client.list_multipart_uploads(
            Bucket=DesBucket,
            Prefix=S3Prefix,
            MaxUploads=1000,
            KeyMarker=NextKeyMarker
        )
        IsTruncated = list_multipart_uploads["IsTruncated"]
        NextKeyMarker = list_multipart_uploads["NextKeyMarker"]
        if NextKeyMarker != '':
            for i in list_multipart_uploads["Uploads"]:
                __multipart_uploaded_list.append({
                    "Key": i["Key"],
                    "Initiated": i["Initiated"],
                    "UploadId": i["UploadId"]
                })
                logger.info(f'Unfinished upload, Key: {i["Key"]}, Time: {i["Initiated"]}')
    return __multipart_uploaded_list


# Jump to handle next file
class NextFile(Exception):
    pass


def uploadTheard_small(srcfile, prefix_and_key):
    print(f'\033[0;32;1m--->Uploading\033[0m {srcfile["Key"]} - small file')
    with open(os.path.join(SrcDir, srcfile["Key"]), 'rb') as data:
        for retryTime in range(MaxRetry + 1):
            try:
                chunkdata = data.read()
                chunkdata_md5 = hashlib.md5(chunkdata)
                s3_dest_client.put_object(
                    Body=chunkdata,
                    Bucket=DesBucket,
                    Key=prefix_and_key,
                    ContentMD5=base64.b64encode(chunkdata_md5.digest()).decode('utf-8'),
                    StorageClass=StorageClass
                )
                print(f'\033[0;34;1m    --->Complete\033[0m {srcfile["Key"]} - small file')
                break
            except Exception as e:
                logger.warning(f'Upload small file Fail: {srcfile["Key"]}, '
                               f'{str(e)}, Attempts: {retryTime}')
                if retryTime >= MaxRetry:
                    logger.error(f'Fail MaxRetry Download/Upload small file: {srcfile["Key"]}')
                    return "MaxRetry"
                else:
                    time.sleep(5 * retryTime)
    return


def download_uploadThread_small(srcfileKey):
    for retryTime in range(MaxRetry + 1):
        try:
            # Get object
            print(f"\033[0;33;1m--->Downloading\033[0m {srcfileKey} - small file")
            response_get_object = s3_src_client.get_object(
                Bucket=SrcBucket,
                Key=srcfileKey
            )
            getBody = response_get_object["Body"].read()
            chunkdata_md5 = hashlib.md5(getBody)
            ContentMD5 = base64.b64encode(chunkdata_md5.digest()).decode('utf-8')

            # Put object
            print(f'\033[0;32;1m    --->Uploading\033[0m {srcfileKey} - small file')
            s3_dest_client.put_object(
                Body=getBody,
                Bucket=DesBucket,
                Key=srcfileKey,
                ContentMD5=ContentMD5,
                StorageClass=StorageClass
            )
            # 结束 Upload/download
            print(f'\033[0;34;1m        --->Complete\033[0m {srcfileKey}  - small file')
            break
        except Exception as e:
            logger.warning(f'Download/Upload small file Fail: {srcfileKey}, '
                           f'{str(e)}, Attempts: {retryTime}')
            if retryTime >= MaxRetry:
                logger.error(f'Fail MaxRetry Download/Upload small file: {srcfileKey}')
                return "MaxRetry"
            else:
                time.sleep(5 * retryTime)
    return


def alioss_download_uploadThread_small(srcfileKey):
    for retryTime in range(MaxRetry + 1):
        try:
            # Get Objcet
            print(f"\033[0;33;1m--->Downloading\033[0m {srcfileKey} - small file")
            response_get_object = ali_bucket.get_object(
                key=srcfileKey
            )
            getBody = b''
            for chunk in response_get_object:
                getBody += chunk
            chunkdata_md5 = hashlib.md5(getBody)

            # Put Object
            print(f"\033[0;32;1m    --->Uploading\033[0m {srcfileKey} - small file")
            s3_dest_client.put_object(
                Body=getBody,
                Bucket=DesBucket,
                Key=srcfileKey,
                ContentMD5=base64.b64encode(chunkdata_md5.digest()).decode('utf-8'),
                StorageClass=StorageClass
            )
            print(f'\033[0;34;1m        --->Complete\033[0m {srcfileKey} - small file')
            break
        except Exception as e:
            logger.warning(f'Download/Upload small file Fail: {srcfileKey} - small file, '
                           f'{str(e)}, Attempts: {retryTime}')
            if retryTime >= MaxRetry:
                logger.error(f'Fail MaxRetry Download/Upload small file: {srcfileKey} - small file')
                return "MaxRetry"
            else:
                time.sleep(5 * retryTime)
    return


# Upload file with different JobType
def upload_file(srcfile, desFilelist, UploadIdList, ChunkSize_default):  # UploadIdList就是multipart_uploaded_list
    logger.info(f'Start file: {srcfile["Key"]}')
    prefix_and_key = srcfile["Key"]
    if JobType == 'LOCAL_TO_S3':
        prefix_and_key = str(PurePosixPath(S3Prefix) / srcfile["Key"])
    if srcfile['Size'] >= ChunkSize_default:
        try:
            # 循环重试3次（如果MD5计算的ETag不一致）
            for md5_retry in range(3):
                # 检查文件是否已存在，存在不继续、不存在且没UploadID要新建、不存在但有UploadID得到返回的UploadID
                response_check_upload = check_file_exist(srcfile, desFilelist, UploadIdList)
                if response_check_upload == 'UPLOAD':
                    logger.info(f'New upload: {srcfile["Key"]}')
                    response_new_upload = s3_dest_client.create_multipart_upload(
                        Bucket=DesBucket,
                        Key=prefix_and_key,
                        StorageClass=StorageClass
                    )
                    # logger.info("UploadId: "+response_new_upload["UploadId"])
                    reponse_uploadId = response_new_upload["UploadId"]
                    partnumberList = []
                elif response_check_upload == 'NEXT':
                    logger.info(f'Duplicated. {srcfile["Key"]} same size, goto next file.')
                    raise NextFile()
                else:
                    reponse_uploadId = response_check_upload

                    # 获取已上传partnumberList
                    partnumberList = checkPartnumberList(srcfile, reponse_uploadId)

                # 获取索引列表，例如[0, 10, 20]
                response_indexList, ChunkSize_auto = split(srcfile, ChunkSize_default)

                # 执行分片upload
                upload_etag_full = uploadPart(reponse_uploadId, response_indexList, partnumberList, srcfile,
                                              ChunkSize_auto)

                # 合并S3上的文件
                response_complete = completeUpload(
                    reponse_uploadId, srcfile["Key"], len(response_indexList))
                logger.info(f'FINISH: {srcfile["Key"]} TO {response_complete["Location"]}')

                # 检查文件MD5
                if ifVerifyMD5:
                    if response_complete["ETag"] == upload_etag_full:
                        logger.info(f'MD5 ETag Matched - {srcfile["Key"]} - {response_complete["ETag"]}')
                        break
                    else:  # ETag 不匹配，删除S3的文件，重试
                        logger.warning(f'MD5 ETag NOT MATCHED {srcfile["Key"]}( Destination / Origin ): '
                                       f'{response_complete["ETag"]} - {upload_etag_full}')
                        s3_dest_client.delete_object(
                            Bucket=DesBucket,
                            Key=prefix_and_key
                        )
                        UploadIdList = []
                        logger.warning('Deleted and retry upload {srcfile["Key"]}')
                    if md5_retry == 2:
                        logger.warning('MD5 ETag NOT MATCHED Exceed Max Retries - {srcfile["Key"]}')
                else:
                    break
        except NextFile:
            pass

    # Small file procedure
    else:
        # Check file exist
        for f in desFilelist:
            if f["Key"] == prefix_and_key and \
                    (srcfile["Size"] == f["Size"]):
                logger.info(f'Duplicated. {prefix_and_key} same size, goto next file.')
                return
        # 找不到文件，或文件Size不一致 Submit upload
        if JobType == 'LOCAL_TO_S3':
            uploadTheard_small(srcfile, prefix_and_key)
        elif JobType == 'S3_TO_S3':
            download_uploadThread_small(srcfile["Key"])
        elif JobType == 'ALIOSS_TO_S3':
            alioss_download_uploadThread_small(srcfile["Key"])
    return


# Compare file exist on desination bucket
def check_file_exist(srcfile, desFilelist, UploadIdList):
    # 检查源文件是否在目标文件夹中
    prefix_and_key = srcfile["Key"]
    if JobType == 'LOCAL_TO_S3':
        prefix_and_key = str(PurePosixPath(S3Prefix) / srcfile["Key"])
    for f in desFilelist:
        if f["Key"] == prefix_and_key and \
                (srcfile["Size"] == f["Size"]):
            return 'NEXT'  # 文件完全相同
    # 找不到文件，或文件不一致，要重新传的
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
    return UploadID_latest["UploadId"]


# Check parts number exist on S3
def checkPartnumberList(srcfile, uploadId):
    try:
        prefix_and_key = srcfile["Key"]
        if JobType == 'LOCAL_TO_S3':
            prefix_and_key = str(PurePosixPath(S3Prefix) / srcfile["Key"])
        partnumberList = []
        PartNumberMarker = 0
        IsTruncated = True
        while IsTruncated:
            response_uploadedList = s3_dest_client.list_parts(
                Bucket=DesBucket,
                Key=prefix_and_key,
                UploadId=uploadId,
                MaxParts=1000,
                PartNumberMarker=PartNumberMarker
            )
            NextPartNumberMarker = response_uploadedList['NextPartNumberMarker']
            IsTruncated = response_uploadedList['IsTruncated']
            if NextPartNumberMarker > 0:
                for partnumberObject in response_uploadedList["Parts"]:
                    partnumberList.append(partnumberObject["PartNumber"])
            PartNumberMarker = NextPartNumberMarker
        if partnumberList:  # 如果为0则表示没有查到已上传的Part
            logger.info("Found uploaded partnumber: " + json.dumps(partnumberList))
    except Exception as checkPartnumberList_err:
        logger.error("checkPartnumberList_err" + json.dumps(checkPartnumberList_err))
        input('PRESS ENTER TO QUIT')
        sys.exit(0)
    return partnumberList


# split the file into a virtual part list of index, each index is the start point of the file
def split(srcfile, ChunkSize):
    partnumber = 1
    indexList = [0]
    if int(srcfile["Size"] / ChunkSize) + 1 > 10000:
        ChunkSize = int(srcfile["Size"] / 10000) + 1024  # 对于大于10000分片的大文件，自动调整Chunksize
        logger.info(f'Size excess 10000 parts limit. Auto change ChunkSize to {ChunkSize}')

    while ChunkSize * partnumber < srcfile["Size"]:  # 如果刚好是"="，则无需再分下一part，所以这里不能用"<="
        indexList.append(ChunkSize * partnumber)
        partnumber += 1
    return indexList, ChunkSize


# upload parts in the list
def uploadPart(uploadId, indexList, partnumberList, srcfile, ChunkSize_auto):
    partnumber = 1  # 当前循环要上传的Partnumber
    total = len(indexList)
    md5list = [hashlib.md5(b'')] * total
    complete_list = []
    # 线程池Start
    with futures.ThreadPoolExecutor(max_workers=MaxThread) as pool:
        for partStartIndex in indexList:
            # start to upload part
            if partnumber not in partnumberList:
                dryrun = False
            else:
                dryrun = True
            # upload 1 part/thread, or dryrun to only caculate md5
            if JobType == 'LOCAL_TO_S3':
                pool.submit(uploadThread, uploadId, partnumber,
                            partStartIndex, srcfile["Key"], total, md5list, dryrun, complete_list, ChunkSize_auto)
            elif JobType == 'S3_TO_S3':
                pool.submit(download_uploadThread, uploadId, partnumber,
                            partStartIndex, srcfile["Key"], total, md5list, dryrun, complete_list, ChunkSize_auto)
            elif JobType == 'ALIOSS_TO_S3':
                pool.submit(alioss_download_uploadThread, uploadId, partnumber,
                            partStartIndex, srcfile["Key"], srcfile["Size"], total, md5list,
                            dryrun, complete_list, ChunkSize_auto)
            partnumber += 1
    # 线程池End
    logger.info(f'All parts uploaded - {srcfile["Key"]} - size: {srcfile["Size"]}')

    # 计算所有分片列表的总etag: cal_etag
    digests = b"".join(m.digest() for m in md5list)
    md5full = hashlib.md5(digests)
    cal_etag = '"%s-%s"' % (md5full.hexdigest(), len(md5list))
    return cal_etag


# Single Thread Upload one part, from local to s3
def uploadThread(uploadId, partnumber, partStartIndex, srcfileKey, total, md5list, dryrun, complete_list, ChunkSize):
    prefix_and_key = str(PurePosixPath(S3Prefix) / srcfileKey)
    if not dryrun:
        print(f'\033[0;32;1m--->Uploading\033[0m {srcfileKey} - {partnumber}/{total}')
    with open(os.path.join(SrcDir, srcfileKey), 'rb') as data:
        retryTime = 0
        while retryTime <= MaxRetry:
            try:
                data.seek(partStartIndex)
                chunkdata = data.read(ChunkSize)
                chunkdata_md5 = hashlib.md5(chunkdata)
                md5list[partnumber - 1] = chunkdata_md5
                if not dryrun:
                    s3_dest_client.upload_part(
                        Body=chunkdata,
                        Bucket=DesBucket,
                        Key=prefix_and_key,
                        PartNumber=partnumber,
                        UploadId=uploadId,
                        ContentMD5=base64.b64encode(chunkdata_md5.digest()).decode('utf-8')
                    )
                    # 这里对单个part上传加了 MD5 校验，后面多part合并的时候会再做一次整个文件的
                break
            except Exception as err:
                retryTime += 1
                logger.info(f'UploadThreadFunc log: {srcfileKey} - {str(err)}')
                logger.info(f'Upload Fail - {srcfileKey} - Retry part - {partnumber} - Attempt - {retryTime}')
                if retryTime > MaxRetry:
                    logger.error(f'Quit for Max retries: {retryTime}')
                    input('PRESS ENTER TO QUIT')
                    sys.exit(0)
                time.sleep(5 * retryTime)  # 递增延迟重试
    complete_list.append(partnumber)
    if not dryrun:
        print(f'\033[0;34;1m    --->Complete\033[0m {srcfileKey} '
              f'- {partnumber}/{total} \033[0;34;1m{len(complete_list) / total:.2%}\033[0m')
    return


# download part from src. s3 and upload to dest. s3
def download_uploadThread(uploadId, partnumber, partStartIndex, srcfileKey, total, md5list, dryrun, complete_list,
                          ChunkSize):
    if ifVerifyMD5 or not dryrun:
        # 下载文件
        if not dryrun:
            print(f"\033[0;33;1m--->Downloading\033[0m {srcfileKey} - {partnumber}/{total}")
        else:
            print(f"\033[0;33;40m--->Downloading for verify MD5\033[0m {srcfileKey} - {partnumber}/{total}")
        retryTime = 0
        while retryTime <= MaxRetry:
            try:
                response_get_object = s3_src_client.get_object(
                    Bucket=SrcBucket,
                    Key=srcfileKey,
                    Range="bytes=" + str(partStartIndex) + "-" + str(partStartIndex + ChunkSize - 1)
                )
                getBody = response_get_object["Body"].read()
                chunkdata_md5 = hashlib.md5(getBody)
                md5list[partnumber - 1] = chunkdata_md5
                break
            except Exception as err:
                retryTime += 1
                logger.warning(f"DownloadThreadFunc - {srcfileKey} - Exception log: {str(err)}")
                logger.warning(f"Download part fail, retry part: {partnumber} Attempts: {retryTime}")
                if retryTime > MaxRetry:
                    logger.error(f"Quit for Max Download retries: {retryTime}")
                    input('PRESS ENTER TO QUIT')
                    sys.exit(0)
                time.sleep(5 * retryTime)  # 递增延迟重试
    if not dryrun:
        # 上传文件
        print(f'\033[0;32;1m    --->Uploading\033[0m {srcfileKey} - {partnumber}/{total}')
        retryTime = 0
        while retryTime <= MaxRetry:
            try:
                s3_dest_client.upload_part(
                    Body=getBody,
                    Bucket=DesBucket,
                    Key=srcfileKey,
                    PartNumber=partnumber,
                    UploadId=uploadId,
                    ContentMD5=base64.b64encode(chunkdata_md5.digest()).decode('utf-8')
                )
                break
            except Exception as err:
                retryTime += 1
                logger.warning(f"UploadThreadFunc - {srcfileKey} - Exception log: {str(err)}")
                logger.warning(f"Upload part fail, retry part: {partnumber} Attempts: {retryTime}")
                if retryTime > MaxRetry:
                    logger.error(f"Quit for Max Upload retries: {retryTime}")
                    input('PRESS ENTER TO QUIT')
                    sys.exit(0)
                time.sleep(5 * retryTime)  # 递增延迟重试
    complete_list.append(partnumber)
    if not dryrun:
        print(f'\033[0;34;1m        --->Complete\033[0m {srcfileKey} '
              f'- {partnumber}/{total} \033[0;34;1m{len(complete_list) / total:.2%}\033[0m')
    return


# download part from src. ali_oss and upload to dest. s3
def alioss_download_uploadThread(uploadId, partnumber, partStartIndex, srcfileKey, srcfileSize, total, md5list, dryrun,
                                 complete_list, ChunkSize):
    if ifVerifyMD5 or not dryrun:
        # 下载文件
        if not dryrun:
            print(f"\033[0;33;1m--->Downloading\033[0m {srcfileKey} - {partnumber}/{total}")
        else:
            print(f"\033[0;33;40m--->Downloading for verify MD5\033[0m {srcfileKey} - {partnumber}/{total}")
        retryTime = 0
        while retryTime <= MaxRetry:
            try:
                partEndIndex = partStartIndex + ChunkSize - 1
                if partEndIndex > srcfileSize:
                    partEndIndex = srcfileSize - 1
                # Ali OSS 如果range结尾超出范围会变成从头开始下载全部(什么脑子？)，所以必须人工修改为FileSize-1
                # 而S3或本地硬盘超出范围只会把结尾指针改为最后一个字节
                response_get_object = ali_bucket.get_object(
                    key=srcfileKey,
                    byte_range=(partStartIndex, partEndIndex)
                )
                getBody = b''
                for chunk in response_get_object:
                    getBody += chunk
                chunkdata_md5 = hashlib.md5(getBody)
                md5list[partnumber - 1] = chunkdata_md5
                break
            except Exception as err:
                retryTime += 1
                logger.warning(f"DownloadThreadFunc - {srcfileKey} - Exception log: {str(err)}")
                logger.warning(f"Download part fail, retry part: {partnumber} Attempts: {retryTime}")
                if retryTime > MaxRetry:
                    logger.error(f"Quit for Max Download retries: {retryTime}")
                    input('PRESS ENTER TO QUIT')
                    sys.exit(0)
                time.sleep(5 * retryTime)  # 递增延迟重试
    if not dryrun:
        # 上传文件
        print(f'\033[0;32;1m    --->Uploading\033[0m {srcfileKey} - {partnumber}/{total}')
        retryTime = 0
        while retryTime <= MaxRetry:
            try:
                s3_dest_client.upload_part(
                    Body=getBody,
                    Bucket=DesBucket,
                    Key=srcfileKey,
                    PartNumber=partnumber,
                    UploadId=uploadId,
                    ContentMD5=base64.b64encode(chunkdata_md5.digest()).decode('utf-8')
                )
                break
            except Exception as err:
                retryTime += 1
                logger.warning(f"UploadThreadFunc - {srcfileKey} - Exception log: {str(err)}")
                logger.warning(f"Upload part fail, retry part: {partnumber} Attempts: {retryTime}")
                if retryTime > MaxRetry:
                    logger.error(f"Quit for Max Download retries: {retryTime}")
                    input('PRESS ENTER TO QUIT')
                    sys.exit(0)
                time.sleep(5 * retryTime)  # 递增延迟重试
    complete_list.append(partnumber)
    if not dryrun:
        print(f'\033[0;34;1m        --->Complete\033[0m {srcfileKey} '
              f'- {partnumber}/{total} \033[0;34;1m{len(complete_list) / total:.2%}\033[0m')
    return


# Complete multipart upload, get uploadedListParts from S3 and construct completeStructJSON
def completeUpload(reponse_uploadId, srcfileKey, len_indexList):
    # 查询S3的所有Part列表uploadedListParts构建completeStructJSON
    prefix_and_key = srcfileKey
    if JobType == 'LOCAL_TO_S3':
        prefix_and_key = str(PurePosixPath(S3Prefix) / srcfileKey)
    uploadedListPartsClean = []
    PartNumberMarker = 0
    IsTruncated = True
    while IsTruncated:
        response_uploadedList = s3_dest_client.list_parts(
            Bucket=DesBucket,
            Key=prefix_and_key,
            UploadId=reponse_uploadId,
            MaxParts=1000,
            PartNumberMarker=PartNumberMarker
        )
        NextPartNumberMarker = response_uploadedList['NextPartNumberMarker']
        IsTruncated = response_uploadedList['IsTruncated']
        if NextPartNumberMarker > 0:
            for partObject in response_uploadedList["Parts"]:
                ETag = partObject["ETag"]
                PartNumber = partObject["PartNumber"]
                addup = {
                    "ETag": ETag,
                    "PartNumber": PartNumber
                }
                uploadedListPartsClean.append(addup)
        PartNumberMarker = NextPartNumberMarker
    if len(uploadedListPartsClean) != len_indexList:
        logger.warning(f'Uploaded parts size not match - {srcfileKey}')
        input('PRESS ENTER TO QUIT')
        sys.exit(0)
    completeStructJSON = {"Parts": uploadedListPartsClean}

    # S3合并multipart upload任务
    response_complete = s3_dest_client.complete_multipart_upload(
        Bucket=DesBucket,
        Key=prefix_and_key,
        UploadId=reponse_uploadId,
        MultipartUpload=completeStructJSON
    )
    logger.info(f'Complete merge file {srcfileKey}')
    return response_complete


# Compare local file list and s3 list
def compare_local_to_s3():
    logger.info('Comparing destination and source ...')
    fileList = get_local_file_list()
    desFilelist = get_s3_file_list(s3_dest_client, DesBucket)
    deltaList = []
    for source_file in fileList:
        match = False
        for destination_file in desFilelist:
            if (str((PurePosixPath(S3Prefix) / source_file["Key"])) == destination_file["Key"]) and \
                    (source_file["Size"] == destination_file["Size"]):
                match = True  # source 在 destination找到，并且Size一致
                break
        if not match:
            deltaList.append(source_file)
    if not deltaList:
        logger.warning('All source files are in destination Bucket/Prefix. Job well done.')
    else:
        logger.warning(f'There are {len(deltaList)} files not in destination or not the same size. List:')
        for delta_file in deltaList:
            logger.warning(str(delta_file))
    return


# Compare S3 buckets
def compare_buckets():
    logger.info('Comparing destination and source ...')
    if JobType == 'S3_TO_S3':
        if SrcFileIndex == "*":
            fileList = get_s3_file_list(s3_src_client, SrcBucket)
        else:
            fileList = head_s3_single_file(s3_src_client, SrcBucket)
    if JobType == 'ALIOSS_TO_S3':
        if SrcFileIndex == "*":
            fileList = get_ali_oss_file_list(ali_bucket)
        else:
            fileList = head_oss_single_file(ali_bucket)
    desFilelist = get_s3_file_list(s3_dest_client, DesBucket)
    deltaList = []
    for source_file in fileList:
        match = False
        for destination_file in desFilelist:
            if source_file == destination_file:
                match = True  # source 在 destination找到，并且Size一致
                break
        if not match:
            deltaList.append(source_file)
    if not deltaList:
        logger.warning('All source files are in destination Bucket/Prefix. Job well done.')
    else:
        logger.warning(f'There are {len(deltaList)} files not in destination or not the same size. List:')
        for delta_file in deltaList:
            logger.warning(json.dumps(delta_file))
    return


# Main
if __name__ == '__main__':
    start_time = time.time()
    ChunkSize_default = set_config()
    logger, log_file_name = set_log()

    # Define s3 client
    s3_config = Config(max_pool_connections=100)
    s3_dest_client = Session(profile_name=DesProfileName).client('s3', config=s3_config)
    if JobType == 'S3_TO_S3':
        s3_src_client = Session(profile_name=SrcProfileName).client('s3', config=s3_config)
    elif JobType == 'ALIOSS_TO_S3':
        # For ALIOSS_TO_S3
        import oss2

        ali_bucket = oss2.Bucket(oss2.Auth(ali_access_key_id, ali_access_key_secret), ali_endpoint, ali_SrcBucket)

    # Check destination S3 writable
    try:
        logger.info(f'Checking write permission for: {DesBucket}')
        s3_dest_client.put_object(
            Bucket=DesBucket,
            Key=str(PurePosixPath(S3Prefix) / 'access_test'),
            Body='access_test_content'
        )
    except Exception as e:
        logger.error(f'Can not write to {DesBucket}/{S3Prefix}, {str(e)}')
        input('PRESS ENTER TO QUIT')
        sys.exit(0)

    # 获取源文件列表
    logger.info('Get source file list')
    src_file_list = []
    if JobType == "LOCAL_TO_S3":
        SrcDir = str(Path(SrcDir))
        src_file_list = get_local_file_list()
    elif JobType == "S3_TO_S3":
        if SrcFileIndex == "*":
            src_file_list = get_s3_file_list(s3_src_client, SrcBucket)
        else:
            src_file_list = head_s3_single_file(s3_src_client, SrcBucket)
    elif JobType == 'ALIOSS_TO_S3':
        if SrcFileIndex == "*":
            src_file_list = get_ali_oss_file_list(ali_bucket)
        else:
            src_file_list = head_oss_single_file(ali_bucket)

    # 获取目标s3现存文件列表
    des_file_list = get_s3_file_list(s3_dest_client, DesBucket)

    # 获取Bucket中所有未完成的Multipart Upload
    multipart_uploaded_list = get_uploaded_list(s3_dest_client)

    # 是否清理所有未完成的Multipart Upload, 用于强制重传
    if multipart_uploaded_list:
        logger.warning(f'{len(multipart_uploaded_list)} Unfinished upload, clean them and restart?')
        logger.warning('NOTICE: IF CLEAN, YOU CANNOT RESUME ANY UNFINISHED UPLOAD')
        if not DontAskMeToClean:
            keyboard_input = input(
                "CLEAN unfinished upload and restart(input CLEAN) or resume loading(press enter)? Please confirm: (n/CLEAN)")
        else:
            keyboard_input = 'no'
        if keyboard_input == 'CLEAN':
            # 清理所有未完成的Upload
            for clean_i in multipart_uploaded_list:
                s3_dest_client.abort_multipart_upload(
                    Bucket=DesBucket,
                    Key=clean_i["Key"],
                    UploadId=clean_i["UploadId"]
                )
            multipart_uploaded_list = []
            logger.info('CLEAN FINISHED')
        else:
            logger.info('You choose not to clean, now try to resume unfinished upload')

    # 对文件列表中的逐个文件进行上传操作
    with futures.ThreadPoolExecutor(max_workers=MaxParallelFile) as file_pool:
        for src_file in src_file_list:
            file_pool.submit(upload_file, src_file, des_file_list, multipart_uploaded_list, ChunkSize_default)

    # 再次获取源文件列表和目标文件夹现存文件列表进行比较，每个文件大小一致，输出比较结果
    spent_time = int(time.time() - start_time)
    time_m, time_s = divmod(spent_time, 60)
    time_h, time_m = divmod(time_m, 60)
    if JobType == 'S3_TO_S3':
        print(
            f'\033[0;34;1mMISSION ACCOMPLISHED - Time: {time_h}H:{time_m}M:{time_s}S \033[0m- FROM: {SrcBucket}/{S3Prefix} TO {DesBucket}/{S3Prefix}')
        compare_buckets()
    elif JobType == 'ALIOSS_TO_S3':
        print(
            f'\033[0;34;1mMISSION ACCOMPLISHED - Time: {time_h}H:{time_m}M:{time_s}S \033[0m- FROM: {ali_SrcBucket}/{S3Prefix} TO {DesBucket}/{S3Prefix}')
        compare_buckets()
    elif JobType == 'LOCAL_TO_S3':
        print(
            f'\033[0;34;1mMISSION ACCOMPLISHED - Time: {time_h}H:{time_m}M:{time_s}S \033[0m- FROM: {SrcDir} TO {DesBucket}/{S3Prefix}')
        compare_local_to_s3()
    print('Logged to file:', os.path.abspath(log_file_name))
    input('PRESS ENTER TO QUIT')
