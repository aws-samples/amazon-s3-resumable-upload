
# Amazon S3 Resumable Migration Version 2  (Amazon S3 断点续传迁移 Version 1)

多线程断点续传，充分利用带宽，适合批量的大文件S3上传/上载/迁移，支持Amazon S3, Ali OSS, Tencent COS, Google GCS 等对象存储，即将支持 Azure Blog Storage...
本次 Version 2 主要修改是同一个应用通过配置即可用做单机的上传，单机的下载，部署为集群版的扫描源文件，或集群版的传输工作节点，用Golang做了重构，提高性能。
  
## 功能  

* 多线程并发传输到多种对象存储，断点续传，自动重传。多文件任务并发，充分利用带宽。优化的流控机制。在典型测试中，迁移1.2TB数据从 us-east-1 S3 到 cn-northwest-1 S3 只用1小时。

* 支持的源和目的地：本地目录/文件, Amazon S3, Ali OSS, Tencent COS, Google GCS 等对象存储，即将支持 Azure Blog Storage。无需区分工作模式，指定好源和目的URL或本地路径即可自动识别。可以是单个文件或对象，或整个目录，或S3桶/前缀等URL。

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
go build .  # 编译程序
```

可使用 ./s3trans -h 获取帮助信息

### 快速使用  

* 下载S3文件到本地：  

```shell
s3trans s3://bucket-name/prefix /local/path
# 以上是使用默认AWS profile in ~/.aws/credentials，如果是EC2则使用IAM Role。如果要指定源S3的profile则如下：
s3trans s3://bucket-name/prefix /local/path --from_profile=source_profile
```

* 上传本地文件到S3：  

```shell
s3trans /local/path s3://bucket-name/prefix
# 以上是使用默认AWS profile in ~/.aws/credentials，如果是EC2则使用IAM Role。如果要指定源S3的profile则如下：
s3trans /local/path s3://bucket-name/prefix --to_profile=dest_profile
```

* 从S3到S3，如不指定region，则程序会先自动查询Bucket的Region：  

```shell
s3trans s3://bucket-name/prefix s3://bucket-name/prefix --to_profile=dest_profile
以上from_profile没填则获取默认的profile或使用EC2 IAM Role。或用以下方式直接指定profile
s3trans s3://bucket-name/prefix s3://bucket-name/prefix --from_profile=source_profile --to_profile=dest_profile
```

* 对于非AWS的S3兼容存储，则需要指定endpoint

```shell
s3trans s3://bucket-gcs-test s3://bucket-virginia --from_profile=gcs_profile --to_profile=aws_profile --from_endpoint=https://storage.googleapis.com
# 以上endpoint也可以用简称替换，即：--from_endpoint=google_gcs，还可以是其他简称：ali_oss, tencent_cos, azure_blob(azure开发中...)
```

* -l 指定先List再同步数据（节省请求次数费用，但会增加一次List的时间）
* -n （n 即NumWorkers）指定并行List和并行传输线程数。最大并发对象数为n，每个对象最大并发为2n，List Bucket时最大并发为4n；推荐n <= vCPU numbe

```shell
s3trans C:\Users\Administrator\Downloads\test\ s3://huangzb-virginia/win2/ --to-profile sin  -l -n 8
```

## License
  
This library is licensed under the MIT-0 License. See the LICENSE file.
  
  ******
  Author: Huang, Zhuobin (James)
  ******
  