# -*- coding: utf-8 -*-

"""Basic Configure"""
JobType = "LOCAL_TO_S3"  # 'LOCAL_TO_S3' | 'S3_TO_S3' | 'ALIOSS_TO_S3'
SrcFileIndex = "*"  # 指定要上传的文件的文件名, type = str，Upload全部文件则用 "*"
S3Prefix = "test-zero"  # S3_TO_S3 源S3的Prefix(与目标S3一致)，LOCAL_TO_S3则为目标S3的Prefix, type = str
DesProfileName = "zhy"  # 在~/.aws 中配置的能访问目标S3的 profile name
DesBucket = "hawkey999"  # 目标文件bucket, type = str

"""Configure for LOCAL_TO_S3"""
SrcDir = r"/Users/huangzb/Documents/CodingProject/Multi2s3Project/testSrc"
# 原文件本地存放目录，目录最后一个字符串不要加斜杠, S3_TO_S3则该字段无效 type = str

"""Configure for S3_TO_S3"""
SrcBucket = "hawkey999"  # 源Bucket，LOCAL_TO_S3则本字段无效
SrcProfileName = "zhy"  # 在~/.aws 中配置的能访问源S3的 profile name，LOCAL_TO_S3则本字段无效

"""Configure for ALIOSS_TO_S3"""
ali_SrcBucket = "img-process"
ali_access_key_id = "xxxx"
ali_access_key_secret = "xxx"
ali_endpoint = "oss-cn-beijing.aliyuncs.com"

"""Advanced Configure"""
Megabytes = 1024*1024
ChunkSize = 10 * Megabytes  # 文件分片大小，不小于5M，单文件分片总数不能超过10000, type = int
MaxRetry = 20  # 单个Part上传失败后，最大重试次数, type = int
MaxThread = 5  # 单文件同时上传的进程数量, type = int
MaxParallelFile = 5  # 并行操作文件数量, type = int
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
