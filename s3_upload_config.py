# -*- coding: utf-8 -*-

"""Basic Configure"""
JobType = "S3_TO_S3"  # 'LOCAL_TO_S3' | 'S3_TO_S3' | 'ALIOSS_TO_S3'
SrcFileIndex = "*"  # 指定要上传的文件的文件名, type = str，Upload全部文件则用 "*"
S3Prefix = "multipart/"  # S3_TO_S3源S3的Prefix，LOCAL_TO_S3则为目标S3的Prefix, type = str
DesProfileName = "cn"  # 在~/.aws 中配置的能访问目标S3的 profile name
DesBucket = "my-cn-bucket"  # 目标文件bucket, type = str

"""Configure for LOCAL_TO_S3"""
SrcDir = "/Users/huangzb/Downloads/"
# 原文件本地存放目录, S3_TO_S3则该字段无效 type = str

"""Configure for S3_TO_S3"""
SrcBucket = "my-us-bucket"  # 源Bucket，LOCAL_TO_S3则本字段无效
SrcProfileName = "us"  # 在~/.aws 中配置的能访问源S3的 profile name，LOCAL_TO_S3则本字段无效

"""Configure for ALIOSS_TO_S3"""
ali_SrcBucket = "img-process"  # 阿里云OSS 源Bucket，LOCAL_TO_S3/S3_TO_S3则本字段无效
ali_access_key_id = "xxxxxxxxxxx"  # 阿里云 RAM 用户访问密钥
ali_access_key_secret = "xxxxxxxxxxxx"
ali_endpoint = "oss-cn-beijing.aliyuncs.com"  # OSS endpoint，在OSS控制台界面可以找到

"""Advanced Configure"""
Megabytes = 1024*1024
ChunkSize = 50 * Megabytes  # 文件分片大小，不小于5M，单文件分片总数不能超过10000, type = int
MaxRetry = 200  # 单个Part上传失败后，最大重试次数, type = int
MaxThread = 3  # 单文件同时上传的进程数量, type = int
MaxParallelFile = 3  # 并行操作文件数量, type = int
# 即同时并发的进程数 = MaxParallelFile * MaxThread
IgnoreSmallFile = False  # 是否跳过小于chunksize的小文件, type = bool
StorageClass = "STANDARD"
# 'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'
ifVerifyMD5 = False  # 是否做两次的MD5校验
# 为True则一个文件完成上传合并分片之后再次进行整个文件的ETag校验MD5。
# 对于S3_TO_S3，该开关True会在断点续传的时候重新下载所有已传过的分片来计算MD5。
# 该开关不影响每个分片上传时候的校验，即使为False也会校验每个分片MD5。
DontAskMeToClean = False  # False 遇到存在现有的未完成upload时，不再询问是否Clean，默认不Clean，自动续传
LoggingLevel = "INFO"  # 日志输出级别 'WARNING' | 'INFO' | 'DEBUG'
