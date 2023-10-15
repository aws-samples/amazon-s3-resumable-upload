# S3 Resumable Migration Version 2  ( S3 断点续传迁移 Version 2)

English README: [README.en.md](README.en.md)

多线程断点续传，适合批量的大文件S3上传/下载本地/对象存储迁移，支持Amazon S3, Ali OSS, Tencent COS, Google GCS 等兼容S3 API的对象存储，也将支持 Azure Blog Storage...
本次 Version 2 在同一个应用通过配置即可用做各种场景：单机的上传，单机的下载，部署为集群版的扫描源文件，或集群版的传输工作节点，用Golang做了重构，提高性能，并支持了一系列扩展功能：排除列表、源no-sign-request、源request-payer、目的storage-class、目的ACL、传输 Metadata 等。
  
## 功能  

* 多线程并发传输到多种对象存储，断点续传，自动重传。多文件任务并发，充分利用带宽。优化的流控机制。在典型测试中，迁移1.2TB数据从 us-east-1 S3 到 cn-northwest-1 S3 只用1小时。

* 支持的源和目的地：本地目录/文件, Amazon S3, Ali OSS, Tencent COS, Google GCS 等对象存储。无需区分工作模式，指定好源和目的URL或本地路径即可自动识别。可以是单个文件或对象，或整个目录，或S3桶/前缀等URL。

* 传输数据只以单个分片的形式过中转节点的内存，不落该节点本地盘，节省时间、存储并且数据更安全。可支撑 0 Size 至 TB 级别  

* 自动对比源/目的桶的文件名和大小，不一致的才传输。默认是一边List，一边传输，即逐个获取目的对象信息对比一个就传输一个，这样使用体验是输入命令之后就立马启动传输（类似AWS CLI）；可以使用时通过 -l 参数设置为全部List目的对象列表之后再进行传输，因为List比逐个Head效率更高，也节省请求次数的费用。本次Version 2支持了并行List，对于对象数量很多的情况，可以更快完成List。例如3千万对象的桶，如果按正常List（例如 aws s3 ls）要起码90分钟，而现在在使用64并发的情况(16vCPU)下缩减到只有1分钟。

* 支持设置目的地的各种对象存储级别，如：STANDARD|REDUCED_REDUNDANCY|STANDARD_IA|ONEZONE_IA|INTELLIGENT_TIERING|GLACIER|DEEP_ARCHIVE|OUTPOSTS|GLACIER_IR|SNOW

* 支持指定目的S3的ACL: private|public-read|public-read-write|authenticated-read|aws-exec-read|bucket-owner-read|bucket-owner-full-control

* 支持设置源对象存储是no-sign-request和request-payer的情况

* 支持把源对象存储的 Metadata 也复制到目的对象存储(--transfer-metadata)。但要注意这个需要每个对象都Head去获取一次，会影响性能和增加对源S3的请求次数费用。

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

### 使用  

* 下载S3文件到本地：  

```shell
./s3trans s3://bucket-name/prefix /local/path --from-region=my_region
# 以上是使用默认AWS profile in ~/.aws/credentials，如果是EC2则使用IAM Role。如果要指定源S3的profile则如下：
./s3trans s3://bucket-name/prefix /local/path --from-profile=source_profile
```

* 上传本地文件到S3：  

```shell
./s3trans /local/path s3://bucket-name/prefix
# 以上是使用默认AWS profile in ~/.aws/credentials，如果是EC2则使用IAM Role。如果要指定源S3的profile则如下：
./s3trans /local/path s3://bucket-name/prefix --to-profile=dest_profile
```

* 从S3到S3，如不指定region，则程序会先自动查询Bucket的Region：  

```shell
./s3trans s3://bucket-name/prefix s3://bucket-name/prefix --from-region=my_region --to-profile=dest_profile
# 以上from-profile没填则获取默认的profile或使用EC2 IAM Role。或用以下方式直接指定profile
./s3trans s3://bucket-name/prefix s3://bucket-name/prefix --from-profile=source_profile --to-profile=dest_profile
```

* 对于非AWS的S3兼容存储，则需要指定endpoint

```shell
./s3trans s3://bucket-gcs-test s3://bucket-virginia --from-profile=gcs_profile --to-profile=aws_profile --from-endpoint=https://storage.googleapis.com
# 以上endpoint也可以用简称替换，即：--from-endpoint=google_gcs，还可以是其他简称：ali_oss, tencent_cos, azure_blob(TODO: azure)
```

* -l 指定先List再同步数据（节省请求次数费用，但会增加一次List的时间）
* -n （n 即NumWorkers）指定并行List和并行传输线程数。最大并发对象数为n，每个对象最大并发为2n，List Bucket时最大并发为4n；推荐n <= vCPU numbe
* -y 忽略确认命令，直接执行

```shell
./s3trans C:\Users\Administrator\Downloads\test\ s3://huangzb-virginia/win2/ --to-profile sin  -l -n 8 -y
```

* 支持设置排除列表 (--ignore-list-path) 如果数据源的S3 Key或源本地路径符合排除列表的则不传输
例如，排除列表路径设置为 --ignore-list-path="./ignore-list.txt" 文件内容为：  

```text
test2/
test1
```

则源数据中遇到这些路径都会被跳过，不传输：test2/abc.zip, test1/abc.zip, test1, test1.zip, test2/cde/efg等...
而这些路径则会正常传输，因为开头Prefix不一致：test3/test1, test3/test2/ 等...

## 集群模式

### 集群模式的 List 模块使用

对比源Bucket/Prefix和目的Bucket/Prefix，把不一致的对象信息写入SQS队列，以便后续的传输节点使用。  
需要指定源S3和目的S3的URL，另还需要指定一个SQS用于发送任务列表，包括SQS的url和能访问这个SQS所用的AWS profile。  
可选：  
设置把对比扫描出来的任务列表存入文件 --joblist-write-to-filepath；  
设置把SQS发送的日志存入文件 --sqs-log-to-filename  

```shell
./s3trans s3://from_bucket/ s3://to_bucket/prefix --from-profile us --to-profile bjs \
    --work-mode SQS_SEND
    --sqs-profile us \
    --sqs-url "https://sqs.region.amazonaws.com/my_account_number/sq_queue_sname" \
    --joblist-write-to-filepath "./my_joblist.log" \
    --sqs-log-to-filename  "./sqssent.log" \
    -y -l -n 8
```

### 集群模式的传输节点使用

从SQS队列中获取任务列表，然后传输数据。

```shell
./s3trans s3://from_bucket/prefix s3://to_bucket/ --from-profile us --to-profile bjs \
    --work-mode SQS_CONSUME
    --sqs-profile us \
    --sqs-url "https://sqs.region.amazonaws.com/my_account_number/sq_queue_sname" \
    -y -l -n 8
```

### 其他使用帮助
  
```shell
./s3trans -h

s3trans 从源传输数据到目标
	./s3trans FROM_URL TO_URL [OPTIONS]
	FROM_URL: 数据源的URL，例如 /home/user/data or s3://bucket/prefix
	TO_URL: 传输目标的URL，例如 /home/user/data or s3://bucket/prefix
	命令举例：
	./s3trans s3://bucket/prefix s3://bucket/prefix -from_profile sin -to_profile bjs
	./s3trans s3://bucket/prefix /home/user/data -from_profile sin

Usage:
  s3trans FROM_URL TO_URL [flags]

Flags:
      --acl string                目标S3桶的ACL，private表示只有对象所有者可以读写，例如 private|public-read|public-read-write|authenticated-read|aws-exec-read|bucket-owner-read|bucket-owner-full-control
      --from-endpoint string      数据源的 API Endpoint 例如 https://storage.googleapis.com; https://oss-<region>.aliyuncs.com; https://cos.<region>.myqcloud.com 如果是AWS s3或本地路径，无需指定这个 Endpoint
      --from-profile string       数据源在~/.aws/credentials中的AWS profile
      --from-region string        数据源的区域，例如 cn-north-1. 如果未指定，则会使用您在profile中提供的credentials指定的区域 
  -h, --help                      帮助文档
      --http-timeout int          API请求超时（秒）（默认30）
  -l, --list-target               列出目标S3桶，传输之前比较现有对象。列表比检查每个对象是否存在更有效，但传输可能会因为需要等待列出所有对象进行比较而开始较慢。为了缓解这个问题，此应用程序利用并行列出进行快速列表；如果没有list-target参数，不列出目标S3桶，但在传输每个对象之前，检查每个目标对象，这会消耗更多API调用，但开始更快。
      --max-retries int           API请求最大重试次数（默认5）
      --no-sign-request           源桶不需要请求签名（即允许匿名）
  -n, --num-workers int           NumWorkers*1 并发文件；NumWorkers*2 每个文件的并发分片；NumWorkers*4 List目标桶的并发数；推荐NumWorkers <= vCPU数量（默认4）
      --request-payer             源桶要求请求者支付
      --resumable-threshold int   当文件大小（MB）大于此值时，使用断点续传。（默认50）
  -s, --skip-compare              跳过比较源和目标S3对象的名称和大小。直接覆盖所有对象。不列出目标也不检查目标对象是否已存在。
      --sqs-profile string        访问SQS队列使用~/.aws/credentials中的哪个AWS profile
      --sqs-url string            用于发送或消费消息的SQS队列URL，例如 https://sqs.us-east-1.amazonaws.com/my_account/my_queue_name
      --storage-class string      目标S3桶的存储类，例如 STANDARD|REDUCED_REDUNDANCY|STANDARD_IA|ONEZONE_IA|INTELLIGENT_TIERING|GLACIER|DEEP_ARCHIVE|OUTPOSTS|GLACIER_IR|SNOW 或其他S3兼容的
      --to-endpoint string        数据传输目标的端点，例如 https://storage.googleapis.com . 如果是AWS s3或本地路径，无需指定这个
      --to-profile string         数据传输目标在~/.aws/credentials中的AWS profile
      --to-region string          数据传输目标的区域，例如 cn-north-1. 如果未指定，则会使用您在profile中提供的credentials指定的区域 
      --transfer-metadata         如果为真，从源S3桶获取元数据并上传到目标对象。这会消耗更多的API调用。
      --work-mode string          SQS_SEND | SQS_CONSUME | DRYRUN; SQS_SEND SQS_SEND表示列出源来源_URL S3和目标TO_URL S3进行比较，并发送消息到SQS队列；SQS_CONSUME表示从SQS队列消费消息并从来源_URL S3传输对象到TO_URL S3 
  -y, --y                         忽略等待确认，直接执行；DRYRUN是只比较源和目的桶，统计数量和Size，不传输数据
```

## 其他说明

### S3 触发 SQS 的 Policy示例

写入SQS权限："Service": "s3.amazonaws.com"
读取SQS权限：EC2 Role 或直接填 AWS Account Number

```json
{
  "Version": "2008-10-17",
  "Id": "__default_policy_ID",
  "Statement": [
    {
      "Sid": "__owner_statement",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::my_account_number:root"
      },
      "Action": "SQS:*",
      "Resource": "arn:aws:sqs:us-west-2:my_account_number:s3_migration_file_list"
    },
    {
      "Sid": "__sender_statement",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "SQS:SendMessage",
      "Resource": "arn:aws:sqs:us-west-2:my_account_number:s3_migration_file_list"
    },
    {
      "Sid": "__receiver_statement",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::my_account_number:root"
      },
      "Action": [
        "SQS:ChangeMessageVisibility",
        "SQS:DeleteMessage",
        "SQS:ReceiveMessage"
      ],
      "Resource": "arn:aws:sqs:us-west-2:my_account_number:s3_migration_file_list"
    }
  ]
}

```

## License
  
This library is licensed under the MIT-0 License. See the LICENSE file.
  
  ******
  Author: Huang, Zhuobin (James)
  ******
  
  