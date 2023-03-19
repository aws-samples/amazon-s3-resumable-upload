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
import datetime
import hashlib
import logging
from pathlib import PurePosixPath, Path
import platform
import codecs
os.system("")  # workaround for some windows system to print color

global JobType, SrcFileIndex, DesProfileName, DesBucket, S3Prefix, MaxRetry, MaxThread, \
    MaxParallelFile, StorageClass, ifVerifyMD5, DontAskMeToClean, LoggingLevel, \
    SrcDir, SrcBucket, SrcProfileName, des_endpoint_url, src_endpoint_url \
    # , ali_SrcBucket, ali_access_key_id, ali_access_key_secret, ali_endpoint
max_get = 1000

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

    JobType_list = ['LOCAL_TO_S3', 'S3_TO_S3']
    url_list = ('AWS',
        'https://s3.amazonaws.com', 
        'https://s3.us-east-1.amazonaws.com', 
        'https://s3-accesspoint.us-east-1.amazonaws.com', 
        'https://storage.googleapis.com', 
        'https://oss-cn-beijing.aliyuncs.com')
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
            SrcDir, SrcBucket, SrcProfileName, des_endpoint_url, src_endpoint_url \
            # , ali_SrcBucket, ali_access_key_id, ali_access_key_secret, ali_endpoint
        cfg.read(config_file, encoding='utf-8-sig')
        JobType = cfg.get('Basic', 'JobType')
        des_endpoint_url = cfg.get('Basic', 'DesEndpointURL')
        DesBucket = cfg.get('Basic', 'DesBucket')
        S3Prefix = cfg.get('Basic', 'S3Prefix')
        SrcFileIndex = cfg.get('Basic', 'SrcFileIndex')
        DesProfileName = cfg.get('Basic', 'DesProfileName')
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
            src_endpoint_url = cfg.get('S3_TO_S3', 'SrcEndpointURL')
        except NoOptionError:
            SrcBucket = ''
            SrcProfileName = ''
            src_endpoint_url = ''
    except Exception as e:
        print("ERR loading s3_upload_config.ini", str(e))
        input('PRESS ENTER TO QUIT')
        sys.exit(0)

    # GUI only well support LOCAL_TO_S3 mode, start with --gui option
    # For other JobTpe, GUI is not a prefer option since it's better run on EC2 Linux
    if gui:
        # For GUI
        from tkinter import Tk, filedialog, StringVar, BooleanVar, messagebox, Text, END, DISABLED, NORMAL
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

        # Click Select File
        def browse_file(*args):
            local_file = filedialog.askopenfilename()
            url_txt.delete(0, END)
            url_txt.insert(0, os.path.split(local_file)[0])
            file_txt.delete(0, END)
            file_txt.insert(0, os.path.split(local_file)[1])
            # Finsih browse file
        
        def all_file(*args):
            file_txt.set("*")

        def get_client(direction):
            # Setup client
            if direction == 'src':
                ProfileName = SrcProfileName_txt.get()
                endpoint_url = SrcEndpointURL.get()
            elif direction == 'des':
                ProfileName = DesProfileName_txt.get()
                endpoint_url = DesEndpointURL.get()
            if endpoint_url in ['AWS', 'aws', '']:
                client = Session(profile_name=ProfileName).client('s3')
            else: 
                client = Session(profile_name=ProfileName).client('s3', endpoint_url=endpoint_url)
            return client

        
        def browse_s3(*args):
            S3Prefix = S3Prefix_txt.get()
            file_list = []
            this_bucket = SrcBucket_txt.get()
            client = get_client(direction='src')
            # For delete prefix in des_prefix
            if S3Prefix == '' or S3Prefix == '/':
                # 目的bucket没有设置 Prefix
                dp_len = 0
                Prefix = ''
            else:
                # 目的bucket的 "prefix/"长度
                dp_len = len(str(PurePosixPath(S3Prefix)))+1
                Prefix = str(PurePosixPath(S3Prefix))+'/'
            try:
                log_text.insert('insert',f'Listing files in s3://{this_bucket}/{Prefix}... ')
                log_text.yview_moveto(1)
                log_text.update()
                response = client.list_objects_v2(
                    Bucket=this_bucket,
                    Prefix=Prefix,
                    Delimiter='/',
                    MaxKeys=max_get
                )  # Only get the max files for simply list
                log_text.insert('insert','Done.\n')
                if 'Contents' in response:
                    file_list = [c['Key'][dp_len:] for c in response['Contents']]  # 去掉Prefix
                if not file_list:
                    messagebox.showinfo('Message', f'No files in s3://{this_bucket}/{Prefix}')
                if response['IsTruncated']:
                    messagebox.showinfo('Message', f'More than {max_get} files, cannot fully list here.')
            except Exception as e:
                messagebox.showinfo('Error', f'Cannot get file list from bucket s3://{this_bucket}/{Prefix}, {str(e)}')
            file_txt['values'] = file_list
            # Finish list files

        # Click List Des Buckets
        def ListDesBuckets(*args):
            # Setup client
            client = get_client(direction='des')
            bucket_list = []
            try:
                log_text.insert('insert','Listing all buckets in destination... ')
                log_text.yview_moveto(1)
                log_text.update()
                response = client.list_buckets()
                log_text.insert('insert','Done.\n')
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

        # Click List Src Buckets
        def ListSrcBuckets(*args):
            # Setup client
            client = get_client(direction='src')
            bucket_list = []
            try:
                log_text.insert('insert','Listing all buckets in source... ')
                log_text.yview_moveto(1)
                log_text.update()
                response = client.list_buckets()
                log_text.insert('insert','Done.\n')
                if 'Buckets' in response:
                    bucket_list = [b['Name'] for b in response['Buckets']]
            except Exception as e:
                messagebox.showerror('Error', f'Failt to List buckets. \n'
                                              f'Please verify your aws_access_key of profile: [{DesProfileName}]\n'
                                              f'{str(e)}')
                bucket_list = ['CAN_NOT_GET_BUCKET_LIST']
            SrcBucket_txt['values'] = bucket_list
            SrcBucket_txt.current(0)
            # Finish ListBuckets

        # Click List Prefix
        def ListAllPrefix(*args):
            S3Prefix_txt.set('')
            ListPrefix()


        def ListPrefix(*args):
            JobType = JobType_mode.get()
            if JobType == 'LOCAL_TO_S3':
                this_bucket = DesBucket_txt.get()
                client = get_client(direction='des')
            if JobType == 'S3_TO_S3':
                this_bucket = SrcBucket_txt.get()
                client = get_client(direction='src')

            prefix_list = []
            S3Prefix = S3Prefix_txt.get()
            var_prefix.set(f"This Prefix {S3Prefix}")
            if  S3Prefix == '' or S3Prefix == '/':
                Prefix = ''
            else:
                Prefix = str(PurePosixPath(S3Prefix))+'/'

            try:
                log_text.insert('insert',f'Listing prefixs in s3://{this_bucket}/{Prefix}... ')
                log_text.yview_moveto(1)
                log_text.update()
                response = client.list_objects_v2(
                    Bucket=this_bucket,
                    Prefix=Prefix,
                    Delimiter='/',
                    MaxKeys=max_get
                )  # Only get the max 1000 prefix for simply list
                log_text.insert('insert','Done.\n')
                if 'CommonPrefixes' in response:
                    prefix_list = [c['Prefix'] for c in response['CommonPrefixes']]
                # if not prefix_list:
                #     messagebox.showinfo('Message', f'There no "/" Prefix in: {this_bucket}')
                if response['IsTruncated']:
                    messagebox.showinfo('Message', f'More than {max_get} Prefix, cannot fully list here.')
            except Exception as e:
                messagebox.showinfo('Error', f'Cannot get prefix list from bucket: {this_bucket}, {str(e)}')
            S3Prefix_txt['values'] = prefix_list
            # Finish list prefix

        # Change JobType
        def job_change(*args):
            JobType = JobType_mode.get()
            if JobType == 'LOCAL_TO_S3':
                SrcProfileName_txt['state'] = DISABLED
                SrcEndpointURL['state'] = DISABLED
                SrcBucket_txt['state'] = DISABLED
                SrcProfileName_lb['state'] = DISABLED
                SrcEndpointURL_lb['state'] = DISABLED
                SrcBucket_lb['state'] = DISABLED
                SrcBucket_btn['state'] = DISABLED
                s3file_btn['state'] = DISABLED
                url_txt['state'] = NORMAL
                url_btn['state'] = NORMAL
                url_label['state'] = NORMAL
                file_btn['state'] = NORMAL

            if JobType == 'S3_TO_S3':
                SrcProfileName_txt['state'] = NORMAL
                SrcEndpointURL['state'] = NORMAL
                SrcBucket_txt['state'] = NORMAL
                SrcProfileName_lb['state'] = NORMAL
                SrcEndpointURL_lb['state'] = NORMAL
                SrcBucket_lb['state'] = NORMAL
                SrcBucket_btn['state'] = NORMAL
                s3file_btn['state'] = NORMAL
                url_txt['state'] = DISABLED
                url_btn['state'] = DISABLED
                url_label['state'] = DISABLED
                file_btn['state'] = DISABLED

        # Click START button
        def close():
            window.withdraw()
            ok = messagebox.askokcancel('Start uploading job',
                                        f'FROM s3://{SrcBucket_txt.get()}/{S3Prefix_txt.get()}\nTO s3://{DesBucket_txt.get()}/{S3Prefix_txt.get()}\n'
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
        window.geometry('920x490')
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

        Label(window, text="   ===to===>   ").grid(column=3, row=1, sticky='w', padx=2, pady=2)
        Label(window, text="   ===to===>   ").grid(column=3, row=2, sticky='w', padx=2, pady=2)
        Label(window, text="   ===to===>   ").grid(column=3, row=3, sticky='w', padx=2, pady=2)
        Label(window, text="   ===to===>   ").grid(column=3, row=4, sticky='w', padx=2, pady=2)

        url_label = Label(window, text="Local Folder")
        url_label.grid(column=0, row=1, sticky='w', padx=2, pady=2)
        url_txt = Entry(window, width=17)
        url_txt.grid(column=1, row=1, sticky='w', padx=2, pady=2)
        url_btn = Button(window, text="Select", width=10, command=browse_folder)
        url_btn.grid(column=2, row=1, sticky='w', padx=2, pady=2)
        url_txt.insert(0, SrcDir)
        
        SrcProfileName_lb = Label(window, text="Src AWS Profile")
        SrcProfileName_lb.grid(column=0, row=2, sticky='w', padx=2, pady=2)
        SrcProfileName_txt = Combobox(window, width=27, state="readonly")
        SrcProfileName_txt['values'] = tuple(profile_list)
        SrcProfileName_txt.grid(column=1, row=2, sticky='w', padx=2, pady=2, columnspan=2)
        if SrcProfileName in profile_list:
            position = profile_list.index(SrcProfileName)
            SrcProfileName_txt.current(position)
        else:
            SrcProfileName_txt.current(0)
        SrcProfileName = SrcProfileName_txt.get()
        SrcProfileName_txt.bind("<<ComboboxSelected>>", ListSrcBuckets)
        Label(window, text="Des AWS Profile").grid(column=4, row=2, sticky='w', padx=2, pady=2)
        DesProfileName_txt = Combobox(window, width=27, state="readonly")
        DesProfileName_txt['values'] = tuple(profile_list)
        DesProfileName_txt.grid(column=5, row=2, sticky='w', padx=2, pady=2, columnspan=2)
        if DesProfileName in profile_list:
            position = profile_list.index(DesProfileName)
            DesProfileName_txt.current(position)
        else:
            DesProfileName_txt.current(0)
        DesProfileName = DesProfileName_txt.get()
        DesProfileName_txt.bind("<<ComboboxSelected>>", ListDesBuckets)
        
        SrcEndpointURL_lb = Label(window, text="Src Enpoint URL")
        SrcEndpointURL_lb.grid(column=0, row=3, sticky='w', padx=2, pady=2)
        SrcEndpointURL = Combobox(window, width=27, values=(src_endpoint_url,)+url_list)
        SrcEndpointURL.grid(column=1, row=3, sticky='w', padx=2, pady=2, columnspan=2)
        SrcEndpointURL.current(0)
        Label(window, text="Des Enpoint URL").grid(column=4, row=3, sticky='w', padx=2, pady=2)
        DesEndpointURL = Combobox(window, width=27, values=(des_endpoint_url,) + url_list)
        DesEndpointURL.grid(column=5, row=3, sticky='w', padx=2, pady=2, columnspan=2)
        DesEndpointURL.current(0)

        SrcBucket_lb = Label(window, text="Src S3 Bucket")
        SrcBucket_lb.grid(column=0, row=4, sticky='w', padx=2, pady=2)
        SrcBucket_txt = Combobox(window, width=15, values=SrcBucket)
        SrcBucket_txt.grid(column=1, row=4, sticky='w', padx=2, pady=2)
        SrcBucket_txt.current(0)
        SrcBucket_btn = Button(window, text="List Buckets", width=10, command=ListSrcBuckets)
        SrcBucket_btn.grid(column=2, row=4, sticky='w', padx=2, pady=2)
        SrcBucket_txt.bind('<<ComboboxSelected>>', ListPrefix)
        Label(window, text="Des S3 Bucket").grid(column=4, row=4, sticky='w', padx=2, pady=2)
        DesBucket_txt = Combobox(window, width=15)
        DesBucket_txt.grid(column=5, row=4, sticky='w', padx=2, pady=2)
        DesBucket_txt['values'] = DesBucket
        DesBucket_txt.current(0)
        Button(window, text="List Buckets", width=10, command=ListDesBuckets) \
            .grid(column=6, row=4, sticky='w', padx=2, pady=2)

        Label(window, text="Src/Des S3 Prefix").grid(column=0, row=5, sticky='w', padx=2, pady=2)
        S3Prefix_txt = Combobox(window, width=65)
        S3Prefix_txt.grid(column=1, row=5, sticky='w', padx=2, pady=2, columnspan=4)
        S3Prefix_txt['values'] = S3Prefix
        if S3Prefix != '':
            S3Prefix_txt.current(0)
        var_prefix = StringVar()
        var_prefix.set(f"List Prefix {S3Prefix_txt.get()}")
        Button(window, text='List Prefix /', width=17, command=ListAllPrefix) \
            .grid(column=5, row=5, sticky='w', padx=2, pady=2)
        Button(window, textvariable=var_prefix, width=25, command=ListPrefix) \
            .grid(column=6, row=5, sticky='w', padx=2, pady=2)
        S3Prefix_txt.bind('<<ComboboxSelected>>', ListPrefix)

        Label(window, text="Filename or *").grid(column=0, row=6, sticky='w', padx=2, pady=2)
        file_txt = Combobox(window, width=17)
        file_txt.grid(column=1, row=6, sticky='w', padx=2, pady=2)
        file_btn = Button(window, text="Local File", width=10, command=browse_file) 
        file_btn.grid(column=2, row=6, sticky='w', padx=2, pady=2)
        file_txt['values'] = SrcFileIndex
        if SrcFileIndex != '':
            file_txt.current(0)
        Button(window, text="All Files", width=15, command=all_file) \
            .grid(column=3, row=6, sticky='w', padx=2, pady=2)
        s3file_btn = Button(window, text="Src S3 File", width=10, command=browse_s3)
        s3file_btn.grid(column=4, row=6, sticky='w', padx=2, pady=2)

        Label(window, text="MaxThread/File").grid(column=0, row=7, sticky='w', padx=2, pady=2)
        if MaxThread < 1 or MaxThread > 100:
            MaxThread = 5
        var_t = StringVar()
        var_t.set(str(MaxThread))
        MaxThread_txt = Spinbox(window, from_=1, to=100, width=15, textvariable=var_t)
        MaxThread_txt.grid(column=1, row=7, sticky='w', padx=2, pady=2)

        Label(window, text="MaxParallelFile").grid(column=0, row=8, sticky='w', padx=2, pady=2)
        if MaxParallelFile < 1 or MaxParallelFile > 100:
            MaxParallelFile = 5
        var_f = StringVar()
        var_f.set(str(MaxParallelFile))
        MaxParallelFile_txt = Spinbox(window, from_=1, to=100, width=15, textvariable=var_f)
        MaxParallelFile_txt.grid(column=1, row=8, sticky='w', padx=2, pady=2)

        Label(window, text="S3 StorageClass").grid(column=0, row=9, sticky='w', padx=2, pady=2)
        StorageClass_txt = Combobox(window, width=15, state="readonly")
        StorageClass_txt['values'] = tuple(StorageClass_list)
        StorageClass_txt.grid(column=1, row=9, sticky='w', padx=2, pady=2)
        if StorageClass in StorageClass_list:
            position = StorageClass_list.index(StorageClass)
            StorageClass_txt.current(position)
        else:
            StorageClass_txt.current(0)

        save_config = BooleanVar()
        save_config.set(True)
        save_config_txt = Checkbutton(window, text="Save to config.ini", var=save_config)
        save_config_txt.grid(column=2, row=20, padx=2, pady=2, columnspan=2)
        Button(window, text="Start Upload", width=20, command=close).grid(column=2, row=21, padx=5, pady=5, columnspan=2)

        log_text = Text(window, width=130, height=10)
        log_text.grid(column=0, row=22, padx=2, pady=2, columnspan=7)
        log_text.insert('insert','Logging...\n')
        job_change()
        window.mainloop()

        JobType = JobType_mode.get()
        DesProfileName = DesProfileName_txt.get()
        SrcProfileName = SrcProfileName_txt.get()
        des_endpoint_url = DesEndpointURL.get()
        src_endpoint_url = SrcEndpointURL.get()
        DesBucket = DesBucket_txt.get()
        SrcBucket = SrcBucket_txt.get()
        S3Prefix = S3Prefix_txt.get()
        SrcFileIndex = file_txt.get()
        SrcDir = url_txt.get()
        StorageClass = StorageClass_txt.get()
        MaxThread = int(MaxThread_txt.get())
        MaxParallelFile = int(MaxParallelFile_txt.get())

        if save_config:
            cfg['Basic']['JobType'] = JobType
            cfg['Basic']['DesProfileName'] = DesProfileName
            cfg['Basic']['DesEndpointURL'] = des_endpoint_url
            cfg['Basic']['DesBucket'] = DesBucket
            cfg['Basic']['S3Prefix'] = S3Prefix
            cfg['Basic']['SrcFileIndex'] = SrcFileIndex
            cfg['LOCAL_TO_S3']['SrcDir'] = SrcDir
            cfg['S3_TO_S3']['SrcProfileName'] = SrcProfileName
            cfg['S3_TO_S3']['SrcEndpointURL'] = src_endpoint_url
            cfg['S3_TO_S3']['SrcBucket'] = SrcBucket
            cfg['Advanced']['MaxThread'] = str(MaxThread)
            cfg['Advanced']['MaxParallelFile'] = str(MaxParallelFile)
            cfg['Advanced']['StorageClass'] = StorageClass
            config_file = os.path.join(file_path, 's3_upload_config.ini')
            with codecs.open(config_file, 'w', 'utf-8') as f:
                cfg.write(f)
                print(f"Save config to {config_file}")
        # GUI window finish

    S3Prefix = str(PurePosixPath(S3Prefix))  # 去掉结尾的'/'，如果有的话
    if S3Prefix == '/' or S3Prefix == '.':
        S3Prefix = ''
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


# Get local file list
def get_local_file_list(str_key=False):
    __src_file_list = []
    try:
        if SrcFileIndex == "*":
            for parent, dirnames, filenames in os.walk(SrcDir):
                for filename in filenames:  # 遍历输出文件信息
                    file_absPath = os.path.join(parent, filename)
                    file_relativePath = file_absPath[len(SrcDir) + 1:]
                    file_size = os.path.getsize(file_absPath)
                    key = Path(file_relativePath)
                    if str_key:
                        key = str(key)
                    __src_file_list.append({
                        "Key": key,
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
def get_s3_file_list(*, s3_client, bucket, S3Prefix, no_prefix=False):
    logger.info('Get s3 file list ' + bucket)

    # For delete prefix in des_prefix
    if S3Prefix == '':
        # 目的bucket没有设置 Prefix
        dp_len = 0
    else:
        # 目的bucket的 "prefix/"长度
        dp_len = len(S3Prefix) + 1

    paginator = s3_client.get_paginator('list_objects_v2')
    __des_file_list = []
    try:
        response_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=S3Prefix
        )
        for page in response_iterator:
            if "Contents" in page:
                for n in page["Contents"]:
                    key = n["Key"]
                    if no_prefix:
                        key = key[dp_len:]
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
        key = str(PurePosixPath(S3Prefix)/SrcFileIndex)
        response_fileList = s3_client.head_object(
            Bucket=bucket,
            Key=key
        )
        file = [{
            "Key": key,
            "Size": response_fileList["ContentLength"]
        }]
    except Exception as err:
        logger.error(str(err))
        input('PRESS ENTER TO QUIT')
        sys.exit(0)
    return file


# # Check single file on OSS
# def head_oss_single_file(__ali_bucket):
#     try:
#         response_fileList = __ali_bucket.head_object(
#             key=S3Prefix + SrcFileIndex
#         )
#         file = [{
#             "Key": S3Prefix + SrcFileIndex,
#             "Size": response_fileList.content_length
#         }]
#     except Exception as err:
#         logger.error(str(err))
#         input('PRESS ENTER TO QUIT')
#         sys.exit(0)
#     return file


# # Get object list on OSS
# def get_ali_oss_file_list(__ali_bucket):
#     logger.info('Get oss file list ' + ali_SrcBucket)
#     __des_file_list = []
#     try:
#         response_fileList = __ali_bucket.list_objects(
#             prefix=S3Prefix,
#             max_keys=1000
#         )

#         if len(response_fileList.object_list) != 0:
#             for n in response_fileList.object_list:
#                 __des_file_list.append({
#                     "Key": n.key,
#                     "Size": n.size
#                 })
#             while response_fileList.is_truncated:
#                 response_fileList = __ali_bucket.list_objects(
#                     prefix=S3Prefix,
#                     max_keys=1000,
#                     marker=response_fileList.next_marker
#                 )
#                 for n in response_fileList.object_list:
#                     __des_file_list.append({
#                         "Key": n.key,
#                         "Size": n.size
#                     })
#         else:
#             logger.info('File list is empty in the ali_oss bucket')
#     except Exception as err:
#         logger.error(str(err))
#         input('PRESS ENTER TO QUIT')
#         sys.exit(0)
#     return __des_file_list


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


def uploadThread_small(srcfile, prefix_and_key):
    print(f'\033[0;32;1m--->Uploading\033[0m {srcfile["Key"]} - small file')
    with open(os.path.join(SrcDir, srcfile["Key"]), 'rb') as data:
        for retryTime in range(MaxRetry + 1):
            try:
                pstart_time = time.time()
                chunkdata = data.read()
                chunkdata_md5 = hashlib.md5(chunkdata)
                s3_dest_client.put_object(
                    Body=chunkdata,
                    Bucket=DesBucket,
                    Key=prefix_and_key,
                    ContentMD5=base64.b64encode(chunkdata_md5.digest()).decode('utf-8'),
                    StorageClass=StorageClass
                )
                pload_time = time.time() - pstart_time
                pload_bytes = len(chunkdata)
                pload_speed = size_to_str(int(pload_bytes / pload_time)) + "/s"
                print(f'\033[0;34;1m    --->Complete\033[0m {srcfile["Key"]} - small file - {pload_speed}')
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
            pstart_time = time.time()
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
            pload_time = time.time() - pstart_time
            pload_bytes = len(getBody)
            pload_speed = size_to_str(int(pload_bytes / pload_time)) + "/s"
            print(f'\033[0;34;1m        --->Complete\033[0m {srcfileKey}  - small file - {pload_speed}')
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


# def alioss_download_uploadThread_small(srcfileKey):
#     for retryTime in range(MaxRetry + 1):
#         try:
#             pstart_time = time.time()
#             # Get Objcet
#             print(f"\033[0;33;1m--->Downloading\033[0m {srcfileKey} - small file")
#             response_get_object = ali_bucket.get_object(
#                 key=srcfileKey
#             )
#             getBody = b''
#             for chunk in response_get_object:
#                 if chunk != '':
#                     getBody += chunk
#             chunkdata_md5 = hashlib.md5(getBody)

#             # Put Object
#             print(f"\033[0;32;1m    --->Uploading\033[0m {srcfileKey} - small file")
#             s3_dest_client.put_object(
#                 Body=getBody,
#                 Bucket=DesBucket,
#                 Key=srcfileKey,
#                 ContentMD5=base64.b64encode(chunkdata_md5.digest()).decode('utf-8'),
#                 StorageClass=StorageClass
#             )
#             pload_time = time.time() - pstart_time
#             pload_bytes = len(getBody)
#             pload_speed = size_to_str(int(pload_bytes / pload_time)) + "/s"
#             print(f'\033[0;34;1m        --->Complete\033[0m {srcfileKey} - small file - {pload_speed}')
#             break
#         except Exception as e:
#             logger.warning(f'Download/Upload small file Fail: {srcfileKey} - small file, '
#                            f'{str(e)}, Attempts: {retryTime}')
#             if retryTime >= MaxRetry:
#                 logger.error(f'Fail MaxRetry Download/Upload small file: {srcfileKey} - small file')
#                 return "MaxRetry"
#             else:
#                 time.sleep(5 * retryTime)
#     return


# Upload file with different JobType
def upload_file(*, srcfile, desFilelist, UploadIdList, ChunkSize_default):  # UploadIdList就是multipart_uploaded_list
    logger.info(f'Start file: {srcfile["Key"]}')
    prefix_and_key = srcfile["Key"]
    if JobType == 'LOCAL_TO_S3':
        prefix_and_key = str(PurePosixPath(S3Prefix) / srcfile["Key"])
    if srcfile['Size'] >= ChunkSize_default:
        try:
            # 循环重试3次（如果MD5计算的ETag不一致）
            for md5_retry in range(3):
                # 检查文件是否已存在，存在不继续、不存在且没UploadID要新建、不存在但有UploadID得到返回的UploadID
                response_check_upload = check_file_exist(srcfile=srcfile,
                                                         desFilelist=desFilelist,
                                                         UploadIdList=UploadIdList)
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
                upload_etag_full = uploadPart(uploadId=reponse_uploadId,
                                              indexList=response_indexList,
                                              partnumberList=partnumberList,
                                              srcfile=srcfile,
                                              ChunkSize_auto=ChunkSize_auto)

                # 合并S3上的文件
                response_complete = completeUpload(reponse_uploadId=reponse_uploadId,
                                                   srcfileKey=srcfile["Key"],
                                                   len_indexList=len(response_indexList))
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
            uploadThread_small(srcfile, prefix_and_key)
        elif JobType == 'S3_TO_S3':
            download_uploadThread_small(srcfile["Key"])
        # elif JobType == 'ALIOSS_TO_S3':
        #     alioss_download_uploadThread_small(srcfile["Key"])
    return


# Compare file exist on desination bucket
def check_file_exist(*, srcfile, desFilelist, UploadIdList):
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
def uploadPart(*, uploadId, indexList, partnumberList, srcfile, ChunkSize_auto):
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
                pool.submit(uploadThread,
                            uploadId=uploadId,
                            partnumber=partnumber,
                            partStartIndex=partStartIndex,
                            srcfileKey=srcfile["Key"],
                            total=total,
                            md5list=md5list,
                            dryrun=dryrun,
                            complete_list=complete_list,
                            ChunkSize=ChunkSize_auto)
            elif JobType == 'S3_TO_S3':
                pool.submit(download_uploadThread,
                            uploadId=uploadId,
                            partnumber=partnumber,
                            partStartIndex=partStartIndex,
                            srcfileKey=srcfile["Key"],
                            total=total,
                            md5list=md5list,
                            dryrun=dryrun,
                            complete_list=complete_list,
                            ChunkSize=ChunkSize_auto)
            # elif JobType == 'ALIOSS_TO_S3':
            #     pool.submit(alioss_download_uploadThread,
            #                 uploadId=uploadId,
            #                 partnumber=partnumber,
            #                 partStartIndex=partStartIndex,
            #                 srcfileKey=srcfile["Key"],
            #                 srcfileSize=srcfile["Size"],
            #                 total=total,
            #                 md5list=md5list,
            #                 dryrun=dryrun,
            #                 complete_list=complete_list,
            #                 ChunkSize=ChunkSize_auto)
            partnumber += 1
    # 线程池End
    logger.info(f'All parts uploaded - {srcfile["Key"]} - size: {srcfile["Size"]}')

    # Local upload 的时候考虑传输过程中文件会变更的情况，重新扫描本地文件的MD5，而不是用之前读取的body去生成的md5list
    if ifVerifyMD5 and JobType == 'LOCAL_TO_S3':
        md5list = cal_md5list(indexList=indexList,
                              srcfileKey=srcfile["Key"],
                              ChunkSize=ChunkSize_auto)
    # 计算所有分片列表的总etag: cal_etag
    digests = b"".join(m.digest() for m in md5list)
    md5full = hashlib.md5(digests)
    cal_etag = '"%s-%s"' % (md5full.hexdigest(), len(md5list))
    return cal_etag


# convert bytes to human readable string
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


# 本地文件重新计算一次MD5
def cal_md5list(*, indexList, srcfileKey, ChunkSize):
    logger.info(f'Re-read local file to calculate MD5 again: {srcfileKey}')
    md5list = []
    with open(os.path.join(SrcDir, srcfileKey), 'rb') as data:
        for partStartIndex in indexList:
            data.seek(partStartIndex)
            chunkdata = data.read(ChunkSize)
            chunkdata_md5 = hashlib.md5(chunkdata)
            md5list.append(chunkdata_md5)
    return md5list


# Single Thread Upload one part, from local to s3
def uploadThread(*, uploadId, partnumber, partStartIndex, srcfileKey, total, md5list, dryrun, complete_list, ChunkSize):
    prefix_and_key = str(PurePosixPath(S3Prefix) / srcfileKey)
    if not dryrun:
        print(f'\033[0;32;1m--->Uploading\033[0m {srcfileKey} - {partnumber}/{total}')
    pstart_time = time.time()
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
                    # 这里对单个part上传做了 MD5 校验，后面多part合并的时候会再做一次整个文件的
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
    pload_time = time.time() - pstart_time
    pload_bytes = len(chunkdata)
    pload_speed = size_to_str(int(pload_bytes / pload_time)) + "/s"
    if not dryrun:
        print(f'\033[0;34;1m    --->Complete\033[0m {srcfileKey} '
              f'- {partnumber}/{total} \033[0;34;1m{len(complete_list) / total:.2%} - {pload_speed}\033[0m')
    return


# download part from src. s3 and upload to dest. s3
def download_uploadThread(*, uploadId, partnumber, partStartIndex, srcfileKey, total, md5list, dryrun, complete_list,
                          ChunkSize):
    pstart_time = time.time()
    getBody, chunkdata_md5 = b'', b''  # init
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
    pload_time = time.time() - pstart_time
    pload_bytes = len(getBody)
    pload_speed = size_to_str(int(pload_bytes / pload_time)) + "/s"
    if not dryrun:
        print(f'\033[0;34;1m        --->Complete\033[0m {srcfileKey} '
              f'- {partnumber}/{total} \033[0;34;1m{len(complete_list) / total:.2%} - {pload_speed}\033[0m')
    return


# download part from src. ali_oss and upload to dest. s3
# def alioss_download_uploadThread(*, uploadId, partnumber, partStartIndex, srcfileKey, srcfileSize, total, md5list,
#                                  dryrun, complete_list, ChunkSize):
#     pstart_time = time.time()
#     getBody, chunkdata_md5 = b'', b''  # init
#     if ifVerifyMD5 or not dryrun:
#         # 下载文件
#         if not dryrun:
#             print(f"\033[0;33;1m--->Downloading\033[0m {srcfileKey} - {partnumber}/{total}")
#         else:
#             print(f"\033[0;33;40m--->Downloading for verify MD5\033[0m {srcfileKey} - {partnumber}/{total}")
#         retryTime = 0
#         while retryTime <= MaxRetry:
#             try:
#                 partEndIndex = partStartIndex + ChunkSize - 1
#                 if partEndIndex >= srcfileSize:
#                     partEndIndex = srcfileSize - 1
#                 # Ali OSS 如果range结尾超出范围会变成从头开始下载全部(什么脑子？)，所以必须人工修改为FileSize-1
#                 # 而S3或本地硬盘超出范围只会把结尾指针改为最后一个字节
#                 response_get_object = ali_bucket.get_object(
#                     key=srcfileKey,
#                     byte_range=(partStartIndex, partEndIndex)
#                 )
#                 getBody = b''
#                 for chunk in response_get_object:
#                     if chunk != '':
#                         getBody += chunk
#                 chunkdata_md5 = hashlib.md5(getBody)
#                 md5list[partnumber - 1] = chunkdata_md5
#                 break
#             except Exception as err:
#                 retryTime += 1
#                 logger.warning(f"DownloadThreadFunc - {srcfileKey} - Exception log: {str(err)}")
#                 logger.warning(f"Download part fail, retry part: {partnumber} Attempts: {retryTime}")
#                 if retryTime > MaxRetry:
#                     logger.error(f"Quit for Max Download retries: {retryTime}")
#                     input('PRESS ENTER TO QUIT')
#                     sys.exit(0)
#                 time.sleep(5 * retryTime)  # 递增延迟重试
#     if not dryrun:
#         # 上传文件
#         print(f'\033[0;32;1m    --->Uploading\033[0m {srcfileKey} - {partnumber}/{total}')
#         retryTime = 0
#         while retryTime <= MaxRetry:
#             try:
#                 s3_dest_client.upload_part(
#                     Body=getBody,
#                     Bucket=DesBucket,
#                     Key=srcfileKey,
#                     PartNumber=partnumber,
#                     UploadId=uploadId,
#                     ContentMD5=base64.b64encode(chunkdata_md5.digest()).decode('utf-8')
#                 )
#                 break
#             except Exception as err:
#                 retryTime += 1
#                 logger.warning(f"UploadThreadFunc - {srcfileKey} - Exception log: {str(err)}")
#                 logger.warning(f"Upload part fail, retry part: {partnumber} Attempts: {retryTime}")
#                 if retryTime > MaxRetry:
#                     logger.error(f"Quit for Max Download retries: {retryTime}")
#                     input('PRESS ENTER TO QUIT')
#                     sys.exit(0)
#                 time.sleep(5 * retryTime)  # 递增延迟重试
#     complete_list.append(partnumber)
#     pload_time = time.time() - pstart_time
#     pload_bytes = len(getBody)
#     pload_speed = size_to_str(int(pload_bytes / pload_time)) + "/s"
#     if not dryrun:
#         print(f'\033[0;34;1m        --->Complete\033[0m {srcfileKey} '
#               f'- {partnumber}/{total} \033[0;34;1m{len(complete_list) / total:.2%} - {pload_speed}\033[0m')
#     return


# Complete multipart upload, get uploadedListParts from S3 and construct completeStructJSON
def completeUpload(*, reponse_uploadId, srcfileKey, len_indexList):
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
    fileList = get_local_file_list(str_key=True)
    desFilelist = get_s3_file_list(s3_client=s3_dest_client,
                                   bucket=DesBucket,
                                   S3Prefix=S3Prefix,
                                   no_prefix=True)
    deltaList = []
    for source_file in fileList:
        source_file["Key"] = str(PurePosixPath(source_file["Key"]))
        if source_file not in desFilelist:
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
    deltaList = []
    desFilelist = get_s3_file_list(s3_client=s3_dest_client,
                                   bucket=DesBucket,
                                   S3Prefix=S3Prefix)
    if JobType == 'S3_TO_S3':
        if SrcFileIndex == "*":
            fileList = get_s3_file_list(s3_client=s3_src_client,
                                        bucket=SrcBucket,
                                        S3Prefix=S3Prefix)
        else:
            fileList = head_s3_single_file(s3_src_client, SrcBucket)
    # elif JobType == 'ALIOSS_TO_S3':
    #     if SrcFileIndex == "*":
    #         fileList = get_ali_oss_file_list(ali_bucket)
    #     else:
    #         fileList = head_oss_single_file(ali_bucket)
    else:
        return

    for source_file in fileList:
        if source_file not in desFilelist:
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
    start_time = datetime.datetime.now()
    ChunkSize_default = set_config()
    logger, log_file_name = set_log()

    # Define s3 client
    s3_config = Config(max_pool_connections=200)
    if des_endpoint_url in ['AWS', 'aws', '']:
        s3_dest_client = Session(profile_name=DesProfileName).client('s3', config=s3_config)
    else:         
        s3_dest_client = Session(profile_name=DesProfileName).client('s3', config=s3_config, endpoint_url=des_endpoint_url)
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
        if src_endpoint_url in ['AWS', 'aws', '']:
            s3_src_client = Session(profile_name=SrcProfileName).client('s3', config=s3_config)
        else:
            s3_src_client = Session(profile_name=SrcProfileName).client('s3', config=s3_config, endpoint_url=src_endpoint_url)
        if SrcFileIndex == "*":
            src_file_list = get_s3_file_list(s3_client=s3_src_client,
                                             bucket=SrcBucket,
                                             S3Prefix=S3Prefix)
        else:
            src_file_list = head_s3_single_file(s3_src_client, SrcBucket)
    # elif JobType == 'ALIOSS_TO_S3':
    #     import oss2
    #     ali_bucket = oss2.Bucket(oss2.Auth(ali_access_key_id, ali_access_key_secret), ali_endpoint, ali_SrcBucket)
    #     if SrcFileIndex == "*":
    #         src_file_list = get_ali_oss_file_list(ali_bucket)
    #     else:
    #         src_file_list = head_oss_single_file(ali_bucket)

    # 获取目标s3现存文件列表
    des_file_list = get_s3_file_list(s3_client=s3_dest_client,
                                     bucket=DesBucket,
                                     S3Prefix=S3Prefix)

    # 获取Bucket中所有未完成的Multipart Upload
    multipart_uploaded_list = get_uploaded_list(s3_dest_client)

    # 是否清理所有未完成的Multipart Upload, 用于强制重传
    if multipart_uploaded_list:
        logger.warning(f'{len(multipart_uploaded_list)} Unfinished upload, clean them and restart?')
        logger.warning('NOTICE: IF CLEAN, YOU CANNOT RESUME ANY UNFINISHED UPLOAD')
        if not DontAskMeToClean:
            keyboard_input = input("CLEAN unfinished upload and restart(input CLEAN) or resume loading(press enter)? "
                                   "Please confirm: (n/CLEAN)")
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
            file_pool.submit(upload_file,
                             srcfile=src_file,
                             desFilelist=des_file_list,
                             UploadIdList=multipart_uploaded_list,
                             ChunkSize_default=ChunkSize_default)

    # 再次获取源文件列表和目标文件夹现存文件列表进行比较，每个文件大小一致，输出比较结果
    time_str = str(datetime.datetime.now() - start_time)
    if JobType == 'S3_TO_S3':
        str_from = f'{SrcBucket}/{S3Prefix}'
        compare_buckets()
    # elif JobType == 'ALIOSS_TO_S3':
    #     str_from = f'{ali_SrcBucket}/{S3Prefix}'
    #     compare_buckets()
    elif JobType == 'LOCAL_TO_S3':
        str_from = f'{SrcDir}'
        compare_local_to_s3()
    else:
        str_from = ""
    print(f'\033[0;34;1mMISSION ACCOMPLISHED - Time: {time_str} \033[0m - FROM: {str_from} TO {DesBucket}/{S3Prefix}')
    print('Logged to file:', os.path.abspath(log_file_name))
    input('PRESS ENTER TO QUIT')
