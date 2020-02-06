# AWS S3 MultiThread Resume Upload Tool v1.1  (S3多线程断点续传)   
Muliti-thread S3 upload tool, breaking-point resume supported, suitable for large files  
适合批量的大文件断点续传到 AWS S3  

Upload from local disk, copy files between Global AWS and China AWS S3, or migrate from AliCloud OSS to AWS S3  
从本地硬盘上传，或海外与中国区 AWS S3 之间互相拷贝，或从阿里云 OSS 迁移到 AWS S3。  

Features:  
具体功能包括：  

* Split multipart and get from source, multi-thread upload to S3 and merge, support resume upload (Part level).   
源文件的自动分片获取，多线程并发上传到目的S3再合并文件，断点续传(分片级别)。  

* Support source: local files, AWS S3, AliCloud OSS  
支持的源：本地文件、AWS S3、阿里云 OSS  

* Support destination: AWS S3  
支持的目的地：AWS S3  

* Multi-files concurrently transmission and each file multi-threads download and upload.    
多文件并发传输，且每个文件再多线程并发传输，充分压榨带宽。S3_TO_S3 或 ALIOSS_TO_S3 中间只过内存，不落盘。  

* Auto-retry, progressive increase put off, auto-resume upload parts, MD5 verification on S3  
网络超时自动多次重传。重试采用递增延迟，延迟间隔=次数*5秒。程序中断重启后自动查询S3上已有分片，断点续传(分片级别)。每个分片上传都在S3端进行MD5校验，每个文件上传完进行分片合并时可选再进行一次S3的MD5与本地进行二次校验，保证可靠传输。  

* Auto iterate subfolders, and can also specify only one file.  
自动遍历下级子目录，也可以指定单一文件拷贝。  

* Support setup S3 storage class, such as: standard, S3-IA, Glacier or Deep Archive  
可设置S3存储级别，如：标准、S3-IA、Glacier或深度归档。  

* Can setup ouput info level  
可设置输出消息级别，如设置WARNING级别，则只输出你最关注的信息。
--------  
## Notice 注意事项: 
* ChunkSize setting. Because AWS S3 only support 10,000 parts for one single file, so e.g. ChunkSize 5MB can only support single file max 50GB, if you need to upload single file size 500GB, then you need ChunkSize at least 50MB  
注意 ChunkSize 的大小设置。由于 AWS S3 API 最大只支持单文件10,000个分片。例如设置 ChunkSize 5MB 最大只能支持单个文件 50GB，如果要传单个文件 500GB，则需要设置 ChunkSize 至少为 50MB。  

* If you need to change ChunkSize when files are transmitting, please stop application and restart, then select "CLEAN unfinished upload". Application will clean and re-upload all unfinished files.  
注意 如果某个文件传输到一半，你要修改 ChunkSize 的话。请中断，然后在启动时选择CLEAN unfinished upload，程序会清除未完成文件，并重新上传整个文件，否则文件断点会不正确。  

Language: Python 3.7   
by James Huang  

![Architect](./img/img01.png)
  
![Architect](./img/img02.png)
## AWS Auth 认证配置  

Create file `"credentials"` in ~/.aws/ and save below content:  
创建文件名为 `"credentials"` 于 ~/.aws/ 目录(`C:\Users\USER_NAME\.aws\` for Windows users) 并保存以下内容:

    [default]
    region = <your region>
    aws_access_key_id = <your access key id>
    aws_secret_access_key = <your secret key>

上面 "default" 是默认 profle name，如果是S3 copy to S3你需要配置两个 profile ，一个是访问源 S3，一个是访问目的 S3。示例：

Example credentials:  
样例 credentials 文件中：

    [beijing]
    region=cn-north-1
    aws_access_key_id=XXXXXXXXXXXXXXX
    aws_secret_access_key=XXXXXXXXXXXXXXXXXXXXXX

    [oregon]
    region=us-west-2
    aws_access_key_id=XXXXXXXXXXXXXXX
    aws_secret_access_key=XXXXXXXXXXXXXXXXXXXXXX

See the [Security Credentials](http://aws.amazon.com/security-credentials) page for more detail

## Application Configure  应用配置

Edit 编辑 `s3_upload_config.py`
* 上面配置的 profile name 填入对应源和目的 profile name 项，例如：  
```python
SrcProfileName = 'beijing'
DesProfileName = 'oregon'
```
* Setup source type  
设置上传方式：   
```python
JobType = 'LOCAL_TO_S3' or 'S3_TO_S3' or 'ALIOSS_TO_S3'
```
* Setup address  
设置源文件路径和上传的目的地址，以及其他可选配置

## Run 运行应用
```bash
python3 s3_upload.py
```
## Requirements

该工具需要先安装 `boto3`, the AWS SDK for Python，可以用 pip 安装

    pip install boto3 --user

详见 [boto3](https://github.com/boto/boto3) github page
for more information on getting your keys. For more information on configuring `boto3`,
check out the Quickstart section in the [developer guide](https://boto3.readthedocs.org/en/latest/guide/quickstart.html).

You need to make sure the credentials you're using have the correct permissions to access the Amazon S3
service. If you run into 'Access Denied' errors while running this sample, please follow the steps below.

1. Log into the [AWS IAM Console](https://console.aws.amazon.com/iam/home)
2. Navigate to the Users page.
3. Find the AWS IAM user whose credentials you're using.
4. Under the 'Permissions' section, attach the policy called 'AmazonS3FullAccess'
5. Re-run the application. Now your user should have the right permissions to run the sample.

Please be aware of the [restrictions for bucket names](http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html) when you start creating your own buckets.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
