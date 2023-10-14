
# Amazon S3 Resumable Migration Version 2  (Amazon S3 断点续传迁移 Version 1)

多线程断点续传，充分利用带宽，适合批量的大文件S3上传/上载/迁移，支持Amazon S3, Ali OSS, Tencent COS, Google GCS 等对象存储，即将支持 Azure Blog Storage...
本次 Version 2 主要修改是同一个应用通过配置即可用做单机的上传，单机的下载，部署为集群版的扫描源文件，或集群版的传输工作节点，用Golang做了重构，提高性能。
  
## 功能  

* 多线程并发传输到多种对象存储，断点续传，自动重传。多文件任务并发，充分利用带宽。优化的流控机制。在典型测试中，迁移1.2TB数据从 us-east-1 S3 到 cn-northwest-1 S3 只用1小时。

* 支持的源和目的地：本地目录/文件, Amazon S3, Ali OSS, Tencent COS, Google GCS 等对象存储。无需区分工作模式，指定好源和目的URL或本地路径即可自动识别。可以是单个文件或对象，或整个目录，或S3桶/前缀等URL。

* 传输数据只以单个分片的形式过中转节点的内存，不落该节点本地盘，节省时间、存储并且数据更安全。可支撑 0 Size 至 TB 级别  

* 自动对比源/目的桶的文件名和大小，不一致的才传输。默认是一边List，一边传输，即逐个获取目的对象信息对比一个就传输一个，这样使用体验是输入命令之后就立马启动传输（类似AWS CLI）；可以使用时通过 -l 参数设置为全部List目的对象列表之后再进行传输，因为List比逐个Head效率更高，也节省请求次数的费用。本次Version 2支持了并行List，对于对象数量很多的情况，可以更快完成List。例如3千万对象的桶，如果按正常List（例如 aws s3 ls）要起码90分钟，而现在在使用64并发的情况(16vCPU)下缩减到只有1分钟。

* 支持设置目的地的各种对象存储级别，如：标准、S3-IA、Glacier或深度归档。支持指定目的S3的ACL。

* 支持设置源对象存储是no-sign-request和request-payer的情况

* 支持把源对象存储的 Metadata 也复制到目的对象存储。但要注意这个需要每个对象都Head去获取一次，会影响性能和增加对源S3的请求次数费用。

## 使用说明

### 安装Go运行环境

首次使用需要安装Golang运行环境，以Linux为例：

```shell
sudo yum install -y go git -y
git clone https://github.com/aws-samples/amazon-s3-resumable-upload
```

如果在中国区，可通过go代理来下载go依赖包，则多运行一句代理设置：go env -w GOPROXY=https://goproxy.cn,direct   

### 编译go代码

```shell
cd amazon-s3-resumable-upload
go build .  # 下载依赖包并编译程序
```

可使用 ./s3trans -h 获取帮助信息

### 快速使用  

* 下载S3文件到本地：  

```shell
./s3trans s3://bucket-name/prefix /local/path
# 以上是使用默认AWS profile in ~/.aws/credentials，如果是EC2则使用IAM Role。如果要指定源S3的profile则如下：
./s3trans s3://bucket-name/prefix /local/path --from_profile=source_profile
```

* 上传本地文件到S3：  

```shell
./s3trans /local/path s3://bucket-name/prefix
# 以上是使用默认AWS profile in ~/.aws/credentials，如果是EC2则使用IAM Role。如果要指定源S3的profile则如下：
./s3trans /local/path s3://bucket-name/prefix --to_profile=dest_profile
```

* 从S3到S3，如不指定region，则程序会先自动查询Bucket的Region：  

```shell
./s3trans s3://bucket-name/prefix s3://bucket-name/prefix --to_profile=dest_profile
# 以上from_profile没填则获取默认的profile或使用EC2 IAM Role。或用以下方式直接指定profile
./s3trans s3://bucket-name/prefix s3://bucket-name/prefix --from_profile=source_profile --to_profile=dest_profile
```

* 对于非AWS的S3兼容存储，则需要指定endpoint

```shell
./s3trans s3://bucket-gcs-test s3://bucket-virginia --from_profile=gcs_profile --to_profile=aws_profile --from_endpoint=https://storage.googleapis.com
# 以上endpoint也可以用简称替换，即：--from_endpoint=google_gcs，还可以是其他简称：ali_oss, tencent_cos, azure_blob(TODO: azure)
```

* -l 指定先List再同步数据（节省请求次数费用，但会增加一次List的时间）
* -n （n 即NumWorkers）指定并行List和并行传输线程数。最大并发对象数为n，每个对象最大并发为2n，List Bucket时最大并发为4n；推荐n <= vCPU numbe
* -y 忽略确认命令，直接执行

```shell
./s3trans C:\Users\Administrator\Downloads\test\ s3://huangzb-virginia/win2/ --to-profile sin  -l -n 8
```

* 其他使用帮助
  
```shell
./s3trans -h

s3trans transfers data from source to target.
	./s3trans FROM_URL TO_URL [OPTIONS]
	FROM_URL: The url of data source, e.g. /home/user/data or s3://bucket/prefix
	TO_URL: The url of data transfer target, e.g. /home/user/data or s3://bucket/prefix
	For example:
	./s3trans s3://bucket/prefix s3://bucket/prefix -from_profile sin -to_profile bjs
	./s3trans s3://bucket/prefix /home/user/data -from_profile sin

Usage:
  s3trans FROM_URL TO_URL [flags]

Flags:
      --acl string                The TARGET S3 bucket ACL, private means only the object owner can read&write, e.g. private|public-read|public-read-write|authenticated-read|aws-exec-read|bucket-owner-read|bucket-owner-full-control
      --from-endpoint string      The endpoint of data source, e.g. https://storage.googleapis.com; https://oss-<region>.aliyuncs.com; https://cos.<region>.myqcloud.com . If AWS s3 or local path, no need to specify this.
      --from-profile string       The AWS profile in ~/.aws/credentials of data source
      --from-region string        The region of data transfer source, e.g. cn-north-1. If no specified, the region will be auto detected with the credentials you provided in profile.
  -h, --help                      help for s3trans
      --http-timeout int          API request timeout (seconds) (default 30)
  -l, --list-target               List the TARGET S3 bucket, compare exist objects BEFORE transfer. List is more efficient than head each object to check if it exists, but transfer may start slower because it needs to wait for listing all objects to compare. To mitigate this, this app leverage Concurrency Listing for fast list; If no list-target para, transfer without listing the target S3 bucket, but before transfering each object, head each target object to check, this costs more API call, but start faster.
      --max-retries int           API request max retries (default 5)
      --no-sign-request           The SOURCE bucket is not needed to sign the request
  -n, --num-workers int           NumWorkers*1 for concurrency files; NumWorkers*2 for parts of each file; NumWorkers*4 for listing target bucket; Recommend NumWorkers <= vCPU number (default 4)
      --request-payer             The SOURCE bucket requires requester to pay, set this
      --resumable-threshold int   When the file size (MB) is larger than this value, the file will be resumable transfered. (default 50)
  -s, --skip-compare              If True, skip to compare the name and size between source and target S3 object. Just overwrite all objects. No list target nor head target object to check if it already exists.
      --sqs-profile string        The SQS queue leverage which AWS profile in ~/.aws/credentials
      --sqs-url string            The SQS queue URL to send or consume message from, e.g. https://sqs.us-east-1.amazonaws.com/my_account/my_queue_name
      --storage-class string      The TARGET S3 bucket storage class, e.g. STANDARD|REDUCED_REDUNDANCY|STANDARD_IA|ONEZONE_IA|INTELLIGENT_TIERING|GLACIER|DEEP_ARCHIVE|OUTPOSTS|GLACIER_IR|SNOW or others of S3 compatibale
      --to-endpoint string        The endpoint of data transfer target, e.g. https://storage.googleapis.com . If AWS s3 or local path, no need to specify this.
      --to-profile string         The AWS profile in ~/.aws/credentials of data transfer target
      --to-region string          The region of data transfer target, e.g. us-east-1. If no specified, the region will be auto detected with the credentials you provided in profile.
      --transfer-metadata         If True, get metadata from source S3 bucket and upload the metadata to target object. This costs more API calls.
      --work-mode string          SQS_SEND | SQS_CONSUME; SQS_SEND means listing source FROM_URL S3 and target TO_URL S3 to compare and send message to SQS queue, SQS_CONSUME means consume message from SQS queue and transfer objects from FROM_URL S3 to TO_URL S3. 
  -y, --y                         Ignore waiting for confirming command
```

## License
  
This library is licensed under the MIT-0 License. See the LICENSE file.
  
  ******
  Author: Huang, Zhuobin (James)
  ******
  