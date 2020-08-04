# -*- coding: utf-8 -*-
# PROJECT LONGBOW - AMAZON S3 DOWNLOAD TOOL WITH BREAK-POINT RESUMING
import os
import sys
import json
from boto3.session import Session
from botocore.client import Config
from concurrent import futures
from configparser import ConfigParser, RawConfigParser, NoOptionError
import uuid
import datetime
import logging
from pathlib import PurePosixPath, Path
import platform
import codecs
import sqlite3
import time
os.system("")  # workaround for some windows system to print color

global SrcBucket, S3Prefix, SrcFileIndex, SrcProfileName, DesDir, MaxRetry, MaxThread, MaxParallelFile, LoggingLevel


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

    config_file = os.path.join(file_path, 's3_download_config.ini')
    # If no config file, read the default config
    if not os.path.exists(config_file):
        config_file += '.default'
        print("No customized config, use the default config")
    cfg = ConfigParser()
    print(f'Reading config file: {config_file}')

    try:
        global SrcBucket, S3Prefix, SrcFileIndex, SrcProfileName, DesDir, MaxRetry, MaxThread, MaxParallelFile, LoggingLevel
        cfg.read(config_file, encoding='utf-8-sig')
        SrcBucket = cfg.get('Basic', 'SrcBucket')
        S3Prefix = cfg.get('Basic', 'S3Prefix')
        SrcFileIndex = cfg.get('Basic', 'SrcFileIndex')
        SrcProfileName = cfg.get('Basic', 'SrcProfileName')
        DesDir = cfg.get('Basic', 'DesDir')
        Megabytes = 1024 * 1024
        ChunkSize = cfg.getint('Advanced', 'ChunkSize') * Megabytes
        MaxRetry = cfg.getint('Advanced', 'MaxRetry')
        MaxThread = cfg.getint('Advanced', 'MaxThread')
        MaxParallelFile = cfg.getint('Advanced', 'MaxParallelFile')
        LoggingLevel = cfg.get('Advanced', 'LoggingLevel')
    except Exception as e:
        print("ERR loading s3_download_config.ini", str(e))
        input('PRESS ENTER TO QUIT')
        sys.exit(0)

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
            print(f"There is no aws_access_key in {cre_path}, please input for S3 Bucket: ")
            os.mkdir(pro_path)
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

        # Click List Buckets
        def ListBuckets(*args):
            SrcProfileName = SrcProfileName_txt.get()
            client = Session(profile_name=SrcProfileName).client('s3')
            bucket_list = []
            try:
                response = client.list_buckets()
                if 'Buckets' in response:
                    bucket_list = [b['Name'] for b in response['Buckets']]
            except Exception as e:
                messagebox.showerror('Error', f'Failt to List buckets. \n'
                                              f'Please verify your aws_access_key of profile: [{SrcProfileName}]\n'
                                              f'{str(e)}')
                bucket_list = ['CAN_NOT_GET_BUCKET_LIST']
            SrcBucket_txt['values'] = bucket_list
            SrcBucket_txt.current(0)
            # Finish ListBuckets

        # Click List Prefix
        def ListPrefix(*args):
            SrcProfileName = SrcProfileName_txt.get()
            client = Session(profile_name=SrcProfileName).client('s3')
            prefix_list = []
            this_bucket = SrcBucket_txt.get()
            max_get = 100
            try:
                response = client.list_objects_v2(
                    Bucket=this_bucket,
                    Delimiter='/',
                    RequestPayer='requester'
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

        def browse_file(*args):
            SrcProfileName = SrcProfileName_txt.get()
            S3Prefix = S3Prefix_txt.get()
            client = Session(profile_name=SrcProfileName).client('s3')
            file_list = []
            this_bucket = SrcBucket_txt.get()
            max_get = 100
            try:
                response = client.list_objects_v2(
                    Bucket=this_bucket,
                    Prefix=str(PurePosixPath(S3Prefix))+'/',
                    RequestPayer='requester',
                    Delimiter='/'
                )  # Only get the max 1000 files for simply list

                # For delete prefix in des_prefix
                if S3Prefix == '' or S3Prefix == '/':
                    # 目的bucket没有设置 Prefix
                    dp_len = 0
                else:
                    # 目的bucket的 "prefix/"长度
                    dp_len = len(str(PurePosixPath(S3Prefix)))+1

                if 'Contents' in response:
                    file_list = [c['Key'][dp_len:] for c in response['Contents']]  # 去掉Prefix
                if not file_list:
                    messagebox.showinfo('Message', f'There is no files in s3://{this_bucket}/{S3Prefix}')
                if response['IsTruncated']:
                    messagebox.showinfo('Message', f'More than {max_get} files, cannot fully list here.')
            except Exception as e:
                messagebox.showinfo('Error', f'Cannot get file list from bucket s3://{this_bucket}/{S3Prefix}, {str(e)}')
            file_txt['values'] = file_list
            file_txt.current(0)
            # Finish list files

        # Click START button
        def close():
            window.withdraw()
            ok = messagebox.askokcancel('Start downloading job',
                                        f'DOWNLOAD FROM s3://{SrcBucket_txt.get()}/{S3Prefix_txt.get()}\n'
                                        f'TO LOCAL {url_txt.get()}\n'
                                        f'Click OK to START')
            if not ok:
                window.deiconify()
                return
            window.quit()
            return
            # Finish close()

        # Start GUI
        window = Tk()
        window.title("LONGBOW - AMAZON S3 DOWNLOAD TOOL WITH BREAK-POINT RESUMING")
        window.geometry('705x350')
        window.configure(background='#ECECEC')
        window.protocol("WM_DELETE_WINDOW", sys.exit)

        Label(window, text="S3 Bucket").grid(column=0, row=1, sticky='w', padx=2, pady=2)
        SrcBucket_txt = Combobox(window, width=48)
        SrcBucket_txt.grid(column=1, row=1, sticky='w', padx=2, pady=2)
        SrcBucket_txt['values'] = SrcBucket
        SrcBucket_txt.current(0)
        Button(window, text="List Buckets", width=10, command=ListBuckets) \
            .grid(column=2, row=1, sticky='w', padx=2, pady=2)

        Label(window, text="S3 Prefix").grid(column=0, row=2, sticky='w', padx=2, pady=2)
        S3Prefix_txt = Combobox(window, width=48)
        S3Prefix_txt.grid(column=1, row=2, sticky='w', padx=2, pady=2)
        S3Prefix_txt['values'] = S3Prefix
        if S3Prefix != '':
            S3Prefix_txt.current(0)
        Button(window, text="List Prefix", width=10, command=ListPrefix) \
            .grid(column=2, row=2, sticky='w', padx=2, pady=2)

        Label(window, text="Filename or *").grid(column=0, row=3, sticky='w', padx=2, pady=2)
        file_txt = Combobox(window, width=48)
        file_txt.grid(column=1, row=3, sticky='w', padx=2, pady=2)
        file_txt['values'] = SrcFileIndex
        if SrcFileIndex != '':
            file_txt.current(0)
        Button(window, text="Select File", width=10, command=browse_file) \
            .grid(column=2, row=3, sticky='w', padx=2, pady=2)

        Label(window, text="AWS Profile").grid(column=0, row=4, sticky='w', padx=2, pady=2)
        SrcProfileName_txt = Combobox(window, width=15, state="readonly")
        SrcProfileName_txt['values'] = tuple(profile_list)
        SrcProfileName_txt.grid(column=1, row=4, sticky='w', padx=2, pady=2)
        if SrcProfileName in profile_list:
            position = profile_list.index(SrcProfileName)
            SrcProfileName_txt.current(position)
        else:
            SrcProfileName_txt.current(0)
        SrcProfileName = SrcProfileName_txt.get()
        SrcProfileName_txt.bind("<<ComboboxSelected>>", ListBuckets)

        Label(window, text="Folder").grid(column=0, row=5, sticky='w', padx=2, pady=2)
        url_txt = Entry(window, width=50)
        url_txt.grid(column=1, row=5, sticky='w', padx=2, pady=2)
        url_btn = Button(window, text="Select Folder", width=10, command=browse_folder)
        url_btn.grid(column=2, row=5, sticky='w', padx=2, pady=2)
        url_txt.insert(0, DesDir)

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

        save_config = BooleanVar()
        save_config.set(True)
        save_config_txt = Checkbutton(window, text="Save to s3_download_config.ini", var=save_config)
        save_config_txt.grid(column=1, row=9, padx=2, pady=2)

        Button(window, text="Start Download", width=15, command=close).grid(column=1, row=10, padx=5, pady=5)
        window.mainloop()

        DesDir = url_txt.get()
        SrcFileIndex = file_txt.get()
        SrcBucket = SrcBucket_txt.get()
        S3Prefix = S3Prefix_txt.get()
        SrcProfileName = SrcProfileName_txt.get()
        MaxThread = int(MaxThread_txt.get())
        MaxParallelFile = int(MaxParallelFile_txt.get())

        if save_config:
            cfg['Basic']['SrcBucket'] = SrcBucket
            cfg['Basic']['S3Prefix'] = S3Prefix
            cfg['Basic']['SrcFileIndex'] = SrcFileIndex
            cfg['Basic']['SrcProfileName'] = SrcProfileName
            cfg['Basic']['DesDir'] = DesDir
            cfg['Advanced']['MaxThread'] = str(MaxThread)
            cfg['Advanced']['MaxParallelFile'] = str(MaxParallelFile)

            config_file = os.path.join(file_path, 's3_download_config.ini')
            with codecs.open(config_file, 'w', 'utf-8') as f:
                cfg.write(f)
                print(f"Save config to {config_file}")
        # GUI window finish

    if S3Prefix == '/':
        S3Prefix = ''
    # Finish set_config()
    return ChunkSize


# Configure logging
def set_log():
    logger = logging.getLogger()
    # File logging
    if not os.path.exists("./log"):
        os.mkdir("log")
    this_file_name = os.path.splitext(os.path.basename(__file__))[0]
    file_time = datetime.datetime.now().isoformat().replace(':', '-')[:19]
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


# Get object list on S3
def get_s3_file_list(s3_client, bucket):
    logger.info('Get s3 file list ' + bucket)
    paginator = s3_client.get_paginator('list_objects_v2')
    __des_file_list = []
    try:
        response_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=S3Prefix,
            RequestPayer='requester'
        )
        for page in response_iterator:
            if "Contents" in page:
                for n in page["Contents"]:
                    key = n["Key"]
                    __des_file_list.append({
                        "Key": key,
                        "Size": n["Size"]
                    })
        logger.info(f'Bucket list length：{str(len(__des_file_list))}')
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
            Key=str(PurePosixPath(S3Prefix)/SrcFileIndex)
        )
        file = [{
            "Key": str(PurePosixPath(S3Prefix)/SrcFileIndex),
            "Size": response_fileList["ContentLength"]
        }]
    except Exception as err:
        logger.error(str(err))
        input('PRESS ENTER TO QUIT')
        sys.exit(0)
    return file


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


def size_to_str(size):
    def loop(integer, remainder, level):
        if integer >= 1024:
            remainder = integer % 1024
            integer //= 1024
            level += 1
            return loop(integer, remainder, level)
        else:
            return integer, round(remainder / 1024, 1), level

    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    integer, remainder, level = loop(int(size), 0, 0)
    if level+1 > len(units):
        level = -1
    return f'{integer+remainder} {units[level]}'


def download_thread(partnumber, partStartIndex, srcfileKey, total, complete_list, ChunkSize, wfile):
    try:
        logger.info(f'Downloading {srcfileKey} - {partnumber}/{total}')
        pstart_time = time.time()
        response_get_object = s3_src_client.get_object(
            Bucket=SrcBucket,
            Key=srcfileKey,
            RequestPayer='requester',
            Range="bytes=" + str(partStartIndex) + "-" + str(partStartIndex + ChunkSize - 1)
        )
        getBody = response_get_object["Body"].read()
        complete_list.append(partnumber)
        pload_time = time.time() - pstart_time
        pload_bytes = len(getBody)
        pload_speed = size_to_str(int(pload_bytes/pload_time)) + "/s"
        # 写入文件
        wfile.seek(partStartIndex)
        wfile.write(getBody)
        print(f'\033[0;34;1m --->Complete\033[0m {srcfileKey} '
              f'- {partnumber}/{total}\033[0;34;1m {len(complete_list) / total:.2%} - {pload_speed}\033[0m')

        # 写入partnumber数据库
        dir_and_key = Path(DesDir) / srcfileKey
        try:
            with sqlite3.connect('s3_download.db') as db:
                cursor = db.cursor()
                uuid1 = uuid.uuid1()
                cursor.execute(f"INSERT INTO S3P (ID, BUCKET, KEY, PARTNUMBER) "
                               f"VALUES ('{uuid1}', '{SrcBucket}', '{dir_and_key.as_uri()}', {partnumber})")
                db.commit()
                logger.info(f'Download part completed. Write to DB {srcfileKey} - {partnumber}/{total}')
        except Exception as e:
            logger.warning(f'Fail to insert DB: {dir_and_key.as_uri()}, {str(e)}')
    except Exception as e:
        logger.warning(f'Fail to download {srcfileKey} - {partnumber}/{total}. {str(e)}')
    return


def download_part(indexList, partnumberList, srcfile, ChunkSize_auto, wfile):
    partnumber = 1  # 当前循环要上传的Partnumber
    total = len(indexList)
    complete_list = []
    # 线程池Start
    with futures.ThreadPoolExecutor(max_workers=MaxThread) as pool:
        for partStartIndex in indexList:
            # start to download part
            if partnumber not in partnumberList:
                pool.submit(download_thread, partnumber, partStartIndex, srcfile["Key"], total,
                            complete_list, ChunkSize_auto, wfile)
            else:
                complete_list.append(partnumber)
            partnumber += 1
    # 线程池End
    logger.info(f'All parts downloaded - {srcfile["Key"]} - size: {srcfile["Size"]}')
    return


# 创建文件目录结构
def create_dir(file_dir):
    parent = file_dir.parent
    if not Path.exists(parent):
        create_dir(parent)
    try:
        Path.mkdir(file_dir)
    except Exception as e:
        logger.error(f'Fail to mkdir {str(e)}')


# Download file
def download_file(srcfile, ChunkSize_default):
    logger.info(f'Start file: {srcfile["Key"]}')
    dir_and_key = Path(DesDir) / srcfile["Key"]
    if Path.exists(dir_and_key):
        if dir_and_key.stat().st_size == srcfile["Size"] or dir_and_key.is_dir():
            logger.info(f'Duplicated: {dir_and_key.as_uri()} same size, goto next file.')
            return

    # 创建文件目录结构
    path = dir_and_key.parent
    if not Path.exists(path):
        create_dir(path)

    # 如果是子目录就跳过下载
    if srcfile["Key"][-1] == '/':
        Path.mkdir(dir_and_key)
        logger.info(f'Create empty subfolder: {dir_and_key.as_uri()}')
        return

    # 获取已下载的 part number list
    partnumberList = []
    try:
        with sqlite3.connect('s3_download.db') as db:
            cursor = db.cursor()
            p_sql = cursor.execute(f"SELECT PARTNUMBER FROM S3P WHERE BUCKET='{SrcBucket}' AND KEY='{dir_and_key.as_uri()}'")
            db.commit()
            partnumberList = [d[0] for d in p_sql]
            logger.info(f'Got partnumberList {dir_and_key.as_uri()} - {json.dumps(partnumberList)}')
    except Exception as e:
        logger.error(f'Fail to select partnumber from DB. {str(e)}')

    # 获取索引列表，例如[0, 10, 20]
    indexList, ChunkSize_auto = split(srcfile, ChunkSize_default)

    # 执行download
    s3tmp_name = dir_and_key.with_suffix('.s3tmp')
    if Path.exists(s3tmp_name):
        mode = 'r+b'
    else:
        # 如果没有临时文件，或被删除了，则新建文件并将partnumberList清空
        mode = 'wb'
        partnumberList = []
    with open(s3tmp_name, mode) as wfile:
        download_part(indexList, partnumberList, srcfile, ChunkSize_auto, wfile)

    # 修改文件名.s3part，清理partnumber数据库
    s3tmp_name.rename(dir_and_key)
    try:
        with sqlite3.connect('s3_download.db') as db:
            cursor = db.cursor()
            cursor.execute(f"DELETE FROM S3P WHERE BUCKET='{SrcBucket}' AND KEY='{dir_and_key.as_uri()}'")
            db.commit()
    except Exception as e:
        logger.warning(f'Fail to clean DB: {dir_and_key.as_uri()}. {str(e)}')
    logger.info(f'Finsh: {srcfile["Key"]} TO {dir_and_key.as_uri()}')
    return


# Compare local file list and s3 list
def compare_local_to_s3():
    logger.info('Comparing destination and source ...')
    if SrcFileIndex == "*":
        s3Filelist = get_s3_file_list(s3_src_client, SrcBucket)
    else:
        s3Filelist = head_s3_single_file(s3_src_client, SrcBucket)
    deltaList = []

    for srcfile in s3Filelist:
        dir_and_key = Path(DesDir) / srcfile["Key"]
        # 文件不存在
        if not Path.exists(dir_and_key):
            deltaList.append(srcfile)
            continue
        # 文件大小
        if srcfile["Key"][-1] != '/':
            if srcfile["Size"] != dir_and_key.stat().st_size:
                deltaList.append(srcfile)
                continue

    if not deltaList:
        logger.info('All source files are in destination, job well done.')
    else:
        logger.warning(f'There are {len(deltaList)} files not in destination or not the same size. List:')
        logger.warning(str(deltaList))
    return


# Main
if __name__ == '__main__':
    start_time = datetime.datetime.now()
    ChunkSize_default = set_config()
    logger, log_file_name = set_log()

    # Define s3 client
    s3_config = Config(max_pool_connections=100, retries={'max_attempts': MaxRetry})
    s3_src_client = Session(profile_name=SrcProfileName).client('s3', config=s3_config)

    # Define DB table
    with sqlite3.connect('s3_download.db') as db:
        cursor = db.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS S3P "
                       "(ID TEXT PRIMARY KEY, "
                       "BUCKET TEXT, "
                       "KEY TEXT, "
                       "PARTNUMBER INTEGER)")
        db.commit()

    # 获取源文件列表
    logger.info('Get source file list')
    if SrcFileIndex == "*":
        src_file_list = get_s3_file_list(s3_src_client, SrcBucket)
    else:
        src_file_list = head_s3_single_file(s3_src_client, SrcBucket)

    # 对文件列表中的逐个文件进行下载操作
    with futures.ThreadPoolExecutor(max_workers=MaxParallelFile) as file_pool:
        for src_file in src_file_list:
            file_pool.submit(download_file, src_file, ChunkSize_default)

    # 再次获取源文件列表和目标文件夹现存文件列表进行比较，每个文件大小一致，输出比较结果
    time_str = str(datetime.datetime.now() - start_time)
    compare_local_to_s3()
    print(f'\033[0;34;1mMISSION ACCOMPLISHED - Time: {time_str} \033[0m - FROM: {SrcBucket}/{S3Prefix} TO {DesDir}')
    print('Logged to file:', os.path.abspath(log_file_name))
    input('PRESS ENTER TO QUIT')
