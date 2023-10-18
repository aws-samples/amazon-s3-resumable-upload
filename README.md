# S3 Resumable Migration Version 2  ( S3 断点续传迁移 Version 2)

English README: [README.en.md](README.en.md)

多线程断点续传，适合批量的大文件S3上传/下载本地/对象存储迁移，支持Amazon S3, Ali OSS, Tencent COS, Google GCS 等兼容S3 API的对象存储，也将支持 Azure Blog Storage...  
本 Version 2 在同一个应用通过配置即可用做各种场景：单机的上传，单机的下载，部署为集群版的扫描源文件，或作为集群版的分布式传输工作节点；用Golang做了重构，提高性能；支持了一系列扩展功能：排除列表、源no-sign-request、源request-payer、目的storage-class、目的ACL、传输 Metadata 等。
  
## 功能  

* 多线程并发传输到多种对象存储，断点续传，自动重传。多文件任务并发，充分利用带宽。优化的流控机制。在典型测试中，迁移1.2TB数据从 us-east-1 S3 到 cn-northwest-1 S3 只用1小时。

* 支持的源和目的地：本地目录或单个文件, Amazon S3, Ali OSS, Tencent COS, Google GCS 等对象存储。无需区分工作模式，指定好源和目的URL或本地路径即可自动识别并开始传输。可以是单个文件或对象，或整个目录，或S3桶/前缀等URL。

* 传输数据只以单个分片的形式过中转节点的内存，不落盘到节点，节省时间且更安全。可支撑 0 Size 至 TB 级别 。  

* 支持设置目的地的各种对象存储级别，如：STANDARD|REDUCED_REDUNDANCY|STANDARD_IA|ONEZONE_IA|INTELLIGENT_TIERING|GLACIER|DEEP_ARCHIVE|OUTPOSTS|GLACIER_IR|SNOW

* 支持指定目的S3的ACL: private|public-read|public-read-write|authenticated-read|aws-exec-read|bucket-owner-read|bucket-owner-full-control

* 支持设置源对象存储是no-sign-request和request-payer的情况

* 支持获取源对象存储的 Metadata 也复制到目的对象存储。但要注意这个需要每个对象都Head去获取一次，会影响性能和增加对源S3的请求次数费用。

* 自动对比源/目的桶的文件名和大小，不一致的才传输。默认是一边List，一边传输，即逐个获取目的对象信息对比一个就传输一个，这样使用体验是输入命令之后就立马启动传输（类似AWS CLI）；可选设置 -l 参数，为List目的对象列表之后再进行传输，因为List比逐个Head对比效率更高，也节省请求次数的费用。

* 本次 Version 2 支持了多线程并行 List ，对于对象数量很多的情况，可以更快完成List。例如3千万对象的桶，如果按正常 List（例如 aws s3 ls）要90分钟以上，而现在在使用64并发的情况(16vCPU)下缩减到只有 1 到 2 分钟。

* 支持把对比扫描出来的任务列表存入文件；支持把已发送到SQS的日志存入文件；支持设置排除列表，如果数据源Key或源本地路径符合排除列表的则不传输；支持DRYRUN模式，只比较源和目的桶，统计数量和Size，不传输数据；支持不做对比不检查目的对象，直接覆盖的模式。

* 支持设置断点续传阈值；设置并行线程数；设置请求超时时间；设置最大重试次数；支持设置是否忽略确认命令，直接执行；

## 使用说明

### 安装Go运行环境

首次使用需要安装Golang运行环境，以Linux为例：

```shell
sudo yum install -y go git -y
```

如果在中国区，可通过go代理来下载go依赖包，则多运行一句代理设置：

```go
go env -w GOPROXY=https://goproxy.cn,direct   
```

### 下载和编译本项目的Go代码

```shell
git clone https://github.com/aws-samples/amazon-s3-resumable-upload
cd amazon-s3-resumable-upload
go build .  # 下载依赖包并编译程序
```

可使用 ./s3trans -h 获取帮助信息

### 使用  

* 下载S3文件到本地：  

```shell
./s3trans s3://bucket-name/prefix /local/path 
# 以上是使用默认AWS profile in ~/.aws/credentials，如果是EC2且没有配置 profile 而是使用IAM Role，需指定一下 Region
./s3trans s3://bucket-name/prefix /local/path --from-region=my_region
# 如果要指定S3的profile则如下：
./s3trans s3://bucket-name/prefix /local/path --from-profile=source_profile
```

* 上传本地文件到S3：  

```shell
./s3trans /local/path s3://bucket-name/prefix
# 以上是使用默认AWS profile in ~/.aws/credentials，如果是EC2且没有配置 profile 而是使用IAM Role，需指定一下 Region
./s3trans /local/path s3://bucket-name/prefix  --to-region=my_region
# 如果要指定S3的profile则如下：
./s3trans /local/path s3://bucket-name/prefix --to-profile=dest_profile
```

* 从S3到S3，如不指定region，则程序会先自动查询Bucket的Region：  

```shell
./s3trans s3://bucket-name/prefix s3://bucket-name/prefix --from-profile=source_profile --to-profile=dest_profile
# 如果from-profile不填则获取默认的profile或使用EC2 IAM Role，需指定一下region
./s3trans s3://bucket-name/prefix s3://bucket-name/prefix --from-region=my_region --to-profile=dest_profile
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
需要指定源S3和目的S3的URL，另还需要指定一个SQS用于发送任务列表，包括SQS的url和能访问这个SQS所用的AWS profile。不指定 sqs profile 则程序会自动从 EC2 IAM Role 获取权限，Region名称会从sqs-url自动提取。  
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

从SQS队列中获取任务列表，然后传输数据。需要指定源S3和目的S3的URL，另还需要指定一个SQS用于发送任务列表，包括SQS的url和能访问这个SQS所用的AWS profile。不指定 sqs profile 则程序会自动从 EC2 IAM Role 获取权限，Region名称会从sqs-url自动提取。  

```shell
./s3trans s3://from_bucket/prefix s3://to_bucket/ --from-profile us --to-profile bjs \
    --work-mode SQS_CONSUME
    --sqs-profile us \
    --sqs-url "https://sqs.region.amazonaws.com/my_account_number/sq_queue_sname" \
    -y -l -n 8
```

### 其他使用帮助
   
./s3trans -h  
  
s3trans 从源传输数据到目标  
	./s3trans FROM_URL TO_URL [OPTIONS]  
	FROM_URL: 数据源的URL，例如 /home/user/data or s3://bucket/prefix  
	TO_URL: 传输目标的URL，例如 /home/user/data or s3://bucket/prefix  
  
Usage:  
  s3trans FROM_URL TO_URL [flags]  
```shell
Flags:  
      --acl string                目标S3桶的ACL，private表示只有对象所有者可以读写，例如 private|public-read|public-read-write|authenticated-read|aws-exec-read|bucket-owner-read|bucket-owner-full-control ，不设置则默认根据S3的默认设置，通常是 private 模式 
      --from-endpoint string      数据源的 API Endpoint 例如 https://storage.googleapis.com; https://oss-shenzhen.aliyuncs.com; https://cos.<region>.myqcloud.com 如果是AWS S3或本地路径，无需指定这个 Endpoint  
      --from-profile string       数据源在~/.aws/credentials中的AWS profile，如果不指定profile则用default profile，如果没有default profile，则需指定region  
      --from-region string        数据源的区域，例如 cn-north-1. 如果未指定，但有设置 profile 则会自动找S3的所在 Region  
  -h, --help                      帮助文档  
      --http-timeout int          API请求超时（秒）（默认30）  
  -l, --list-target               推荐使用。列出目标S3桶，传输之前先比较现有对象。因为列表方式比逐个对象请求检查是否存在更有效率，只是因为需要等待列出所有对象进行比较，然后再开始传输所以感觉启动较慢。为了缓解这个问题，此应用程序利用多线程并行List，进行快速列表；如果没有设置--list-target参数，就不List目标S3桶了，而是在传输每个对象之前，检查每个目标对象，这会消耗更多API调用，但开始更快；如果完全不希望做对比，直接覆盖，则用下面提到的--skip-compare参数，而不用--list-target了； 
      --max-retries int           API请求最大重试次数（默认5）  
      --no-sign-request           源桶不需要请求签名（即允许匿名）的情况  
  -n, --num-workers int           NumWorkers x 1 个并发线程传输文件；NumWorkers x 2 每个文件的并发分片同时传输的线程数；NumWorkers x 4 List目标桶的并发线程数；推荐NumWorkers <= vCPU数量（默认4）  
      --request-payer             源桶要求请求者支付的情况  
      --resumable-threshold int   当文件大小（MB）大于此值时，使用断点续传。（默认50）  
  -s, --skip-compare              跳过比较源和目标S3对象的名称和大小。直接覆盖所有对象。不列出目标也不检查目标对象是否已存在。  
      --sqs-profile string        work-mode为SQS_SEND或SQS_CONSUME的场景下，为访问SQS队列使用~/.aws/credentials中的哪个AWS profile，不指定sqs profile则程序会自动从EC2 IAM Role获取权限，Region名称会从sqs-url自动提取。  
      --sqs-url string            work-mode为SQS_SEND或SQS_CONSUME的场景下，指定发送或消费消息的SQS队列URL，例如 https://sqs.us-east-1.amazonaws.com/my_account/my_queue_name  
      --storage-class string      目标S3桶的存储类，例如 STANDARD|REDUCED_REDUNDANCY|STANDARD_IA|ONEZONE_IA|INTELLIGENT_TIERING|GLACIER|DEEP_ARCHIVE|OUTPOSTS|GLACIER_IR|SNOW 或其他S3兼容的  
      --to-endpoint string        数据传输目标的端点，例如 https://storage.googleapis.com . 如果是AWS S3或本地路径，无需指定这个  
      --to-profile string         数据传输目标在~/.aws/credentials中的AWS profile，如果不指定profile则用default profile，如果没有default profile，则需指定region    
      --to-region string          数据传输目标的区域，例如 cn-north-1. 如果未指定，但有设置 profile 则会自动找S3的所在 Region  
      --transfer-metadata         从源S3桶获取元数据并上传到目标对象。这需要每传输一个对象都通过API调用获取源文件元数据。  
      --work-mode string          SQS_SEND | SQS_CONSUME | DRYRUN; SQS_SEND：扫描节点，表示列出源S3和目标S3进行比较，并发送传输任务消息到SQS队列；SQS_CONSUME： 工作节点，表示从SQS队列获取任务消息并从来源S3传输对象到S3  
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
      "Resource": "arn:aws:sqs:us-west-2:my_account_number:s3_migration_queque"
    },
    {
      "Sid": "__sender_statement",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "SQS:SendMessage",
      "Resource": "arn:aws:sqs:us-west-2:my_account_number:s3_migration_queque"
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
      "Resource": "arn:aws:sqs:us-west-2:my_account_number:s3_migration_queque"
    }
  ]
}

```

### 配置文件

如果不使用上面的命令行参数，而使用配置文件，可以在程序运行目录下写一个config.yaml文件，内容如下。然后只需要运行 ./s3trans FROM_URL TO_URL 即可。

```yaml
from-profile: "your_from_profile"
to-profile: "your_to_profile"
from-endpoint: "your_from_endpoint"
to-endpoint: "your_to_endpoint"
from-region: "your_from_region"
to-region: "your_to_region"
storage-class: "your_storage_class"
acl: "your_acl"
no-sign-request: false
request-payer: false
db-location: "./your_download_status.db"
list-target: false
skip-compare: false
transfer-metadata: false
http-timeout: 30
max-retries: 5
retry-delay: 5
chunk-size: 5
resumable-threshold: 50
num-workers: 4
y: false
work-mode: "your_work_mode"
sqs-url: "your_sqs_url"
sqs-profile: "your_sqs_profile"
joblist-write-to-filepath: "your_joblist_write_to_filepath"
sqs-log-to-filename: "your_sqs_log_to_filename"
ignore-list-path: "your_ignore_list_path"
```

还可以把以上配置写入环境变量。

## License
  
This library is licensed under the MIT-0 License. See the LICENSE file.
  
  ******
  Author: Huang, Zhuobin (James)
  ******
  