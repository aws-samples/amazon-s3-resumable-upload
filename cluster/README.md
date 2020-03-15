# Amazon S3 MultiThread Resume Migration Solution  (Amazon S3多线程断点续传迁移)   

Amazon EC2 Autoscaling 集群，支撑海量文件于海外和中国S3之间传输   
  
  集群和无服务器版架构图如下：  
  
![Cluster Diagram](./img/02.png)  
  
## 特点
* 海外和国内S3互传：集群版适用于海量文件传输，无服务器版适合不定期突发传输。  
* 快速且稳定：多节点 X 单节点多文件 X 单文件多线程，支撑海量巨型文件并发传输。启用BBR加速。  
* 可靠：SQS消息队列管理文件级任务，断点续传，超时中断保护。每个分片MD5完整性校验。Single Point of True，最终文件合并以S3上的分片为准，确保分片一致。  
* 安全：内存转发不写盘，传输SSL加密，开源代码可审计，采用IAM Role和利用ParameterStore加密存储AcceesKey。  
* 可控运营：任务派发与传输速度相匹配，系统容量可控可预期；DynamoDB和SQS读写次数只与文件数相关，而与文件大小基本无关；日志自动收集；AWS CDK自动部署；  
* 弹性成本优化：集群自动扩展，结合EC2 Spot节省成本；无服务器Lambda只按调用次数计费；支持直接存入S3各种存储级别，节省长期存储成本。
* 可以于无服务器 Lambda 版本一起运行，支持混合流量  
  
## 工作原理  
### 流程  
0. 预备阶段（可选）：Jobsender 获取源和目的 Bucket List并比对差异，形成Job List。并定期进行。

1. Jobsender 发送 Job messages to SQS （每个 job 是S3上的一个文件），并记录到 DDB (仅做统计进度)
S3 新增也可以直接触发SQS

2. EC2 or/and Lambda 从SQS获取Job. 每个EC2 instance获取多个Job，每个Lambda runtime获取一个Job

3. EC2 or/and Lambda 对每个Job创建多线程，每个线程各自获取文件分片，并上传到目标 S3

* EC2和Lambda可以分用也可以同时工作，各自取SQS Job，但要注意SQS的超时时间设定

### 任务启动四种方式之一
* Job Sender 去List多个源和目的桶进行比对

* S3新增文件，直接触发SQS

* 批量任务完成后，Job Sender 再次List比对

* 定时任务触发Job Sender进行比对  

四种启动方式示意图：
![四种启动方式图](./img/03.png)  

### 性能与可控运营  
![核心原理图](./img/04.png)  
* 单Worker节点并发多线程从SQS获取多个文件任务，每个文件任务只会被一个Worker获得。Worker对每个任务并发多线程进行传输，这样对系统流量更可控，稳定性比把一个文件分散多节点要高，特别适合大量大文件传输。经测试，对于大量的GB级文件，单机 25 线程以上（5文件x5线程）可达到跨境1Gbps带宽吞吐。如文件时MB级，则可以设置单节点并发处理更多的文件。
* 服务器内置并启用 TCP BBR加速，实测比默认的Cubic模式快约2.5倍。  
* Autoscaling，多机协同，吞吐量叠加，建议分布到不同可用区。  
* 建议集群部署在源S3相同Region，并启用 VPC S3 Endpoint。  
* 在DynamoDB中记录工作节点和进度，清晰获知单个文件工作状态，传输一次文件Job只需要写3-4次DynamoDB。
* 设置 SSM ParaStore/Lambda Env 即可调整要发送Job的Bucket/Prefix，无需登录服务器

### 可靠与安全性  
* 每个分片传输完成都会对收到的S3返回Etag信息，做MD5完整性校验。  
* 多个超时中断与重试保护，保证单个文件的送达及时性：  
EC2 worker上有任务超时机制，config.ini中默认配置1小时  
SQS 消息设置 Message InvisibleTime 对超时消息进行恢复，驱动节点做断点重传，建议与Worker超时时间一致。如果混合集群和Serverless架构，则以时间长的那方来设置InvisibleTime  
SQS 配置死信队列DLQ，确保消息被多次重新仍失败进入DLQ做额外保护处理。CDK中默认配置24次。 
* Single point of TRUE：把最终S3作为分片列表的管理，合并文件时缺失分片则重新传。
* 一次性任务在全部任务完成后，运行jobsender进行List比对。或定期运行核对源和目的S3的一致性。
* 片只经worker内存缓存即转发，不写入本地磁盘（速度&安全）
* 传输为S3 API: SSL加密传输
* 开源代码，可审计。只使用 AWS SDK 和 Python3 内置的库，无任何其他第三方库。
* 一侧S3访问采用IAM Role，另一侧S3访问的access key保存在SSM ParaStore中(KMS加密)，或Lambda的EnvVar(KMS加密)，不在代码或配置文件保存任何密钥  

### 弹性成本  
* EC2 worker Autoscaling Group 处理持续长期任务
* Spot Instances 降低成本，Worker节点无状态，可承受随时中断
* EC2 jobsender&worker允许合一部署
* Lambda Serverless 处理不定期，突发任务
* 部分Region和场景需要部署NAT，可以利用现有VPC或做弹性开关机
* 可根据自己的场景测试寻找最佳性价比：
	调整最低配置 EC2Type和LambdaMem，适应最佳的 Jobs/node * Threads/Jobs * Nodes 组合，包括考虑：数据传送周期、文件大小、批次大小、可容忍延迟、成本
* 存入目标S3 存储级别可直接设置 IA, Deep Archive 等

## 部署
### 1. CDK自动部署
* CDK 自动化部署以下所有资源除了 2. 手工配置的Key：  
VPC,  
IAM Role,  
SQS,  
DynamoDB,  
EC2 JobSender, EC2 ASG Workers,  
SSM Parameter Store: s3_migrate_bucket_para  
* EC2 User Data 自动启用 TCP BBR，并自动启动 s3_migration_cluster_jobsender.py 或 s3_migration_cluster_worker.py
User data在EC2启动时自动拉去github上的程序和默认配置。建议把程序和配置放你自己的S3上面，让user data启动时拉取你修改后的配置，并使用通用 Amazon Linux 2 AMI。
* 如果有需要可以修改 EC2上的配置文件 s3_migration_config.ini
```
* JobType = PUT 或 GET 决定了Worker把自己的IAM Role用来访问源还是访问目的S3，, PUT表示EC2跟目标S3不在一个Account，GET表示EC2跟源S3不在一个Account

* Des_bucket_default/Des_prefix_default
是给S3新增文件触发SQS的场景，用来配置目标桶/前缀的。
对于Jobsender扫描S3并派发Job的场景，不需要配置这两项。即使配置了，程序看到SQS消息里面有就会使用消息里面的目标桶/前缀

* table_queue_name 访问的SQS和DynamoDB的表名，需与CloudFormation/CDK创建的ddb/sqs名称一致

* ssm_parameter_bucket 在 SSM ParameterStore 上保存的参数名，用于保存buckets的信息，需与CloudFormation/CDK创建的 parameter store 的名称一致

* ssm_parameter_credentials 在 SSM ParameterStore 上保存的另一个账户体系下的S3访问密钥，需与CloudFormation/CDK创建的 parameter store 的名称一致

* StorageClass = STANDARD|REDUCED_REDUNDANCY|STANDARD_IA|ONEZONE_IA|INTELLIGENT_TIERING|GLACIER|DEEP_ARCHIVE
选择目标存储的存储类型

* ResumableThreshold, 单位MBytes，小于该值的文件，则开始传文件时不查S3上已有的Multipart Upload，不做断点续传，而直接覆盖，节省性能  

* MaxRetry, 单个Part上传失败后，最大重试次数

* MaxThread, 单文件同时working的Thread进程数量  

* MaxParallelFile, 并行操作文件数量

* JobTimeout，单个文件传输超时时间，Seconds 秒

* LoggingLevel = WARNING | INFO | DEBUG
* 不建议修改：ifVerifyMD5Twice, ChunkSize, CleanUnfinishedUpload, LocalProfileMode
* 隐藏参数 max_pool_connections=50 在 s3_migration_lib.py
```
* Lambda 可单独设置和部署，也可以与EC2一起消费同一个SQS Queue，也可以分别独立的Queue  
* 注意三个超时时间的配合： SQS, EC2 JobTimeout, Lambda(CDK 默认部署是SQS/EC2 JobTimeout为1小时)  

### 2. 手工配置
* 请额外手工配置这个 SSM Parameter Store: s3_migration_credentials 用于访问跟EC2不在一个账号系统下的那个S3桶的访问密钥，在目标Account 的IAM user配置获取。配置示例：  
```
{
  "aws_access_key_id": "your_aws_access_key_id",
  "aws_secret_access_key": "your_aws_secret_access_key",
  "region": "cn-northwest-1"
}
```

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
