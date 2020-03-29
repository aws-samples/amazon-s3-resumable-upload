# Amazon S3 MultiThread Resume Migration Serverless Solution  
  
Cluster & Serverless Version 0.94  
  
## Serverless Version  
Amazon EC2 Autoscaling Group Cluster and Serverless AWS Lambda can be deployed together, or seperated used in different senario  
* Transmission between AWS Global and AWS China: Serverless version is suitable for unschedule burst migration.  
* Serverless solution with AWS Lambda and SQS can support large file of tens of GBytes size with unique resumable technique, no worry of 15 mins timeout of Lambda.  
* **Fast and stable**: Multiple nodes X Multiple files/node X Multiple threads/file. Support mass of hugh file concurrently migration. Auto-seperate small file ( including 0 Size) and large file for different procedure.  
* Optional to setup VPC NAT with TCP BBR enable and control EIP to accelerating in some special case. In normal case, e.g. us-east-1, no need for NAT to accelerate.   
* **Reliability**: Amazon SQS queue managed files level jobs, break-point resume trasmission, with timeout protection. Every part will be verified with MD5 after transmission. Single Point of True, final file merging takes the destination S3 as standard, to ensure integrity.  
* **Security**: Transfer in memory, no writing to disk. SSL encryption on transmission. Open source for audit. Leverage IAM role and Lambda Environment Variable(KMS) to store credential Access Key.  
* **Controlable operation**: Job dispatched match the speed of transmission. System capacity predictable. DynamoDB and SQS read/write frequency only related to file numbers, no related to file size. Auto-collect logs to CloudWatch log group. AWS CDK auto deploy.  
* **Elastic cost optimization**: Serverless AWS Lambda only pay for invocation. Support all kind of S3 Storage Class, save long term storage cost.
  
  Serverless Version Architecture  
  
![Serverless Diagram](./img/05.png)  

### Performance Test:
* us-east-1 AWS Lambda ( No need of NAT) 1310 of files, from 4MB to 1.3GB, totally 50GB. Accomplish transmission to China cn-northwest-1 in 10 minutes. Max Lambda concurrency runtime 153. As file size growing up, transmission spent time is no growing up significantly. Each file is handled by one Lambda runtime, bigger file, will be more threads running concurrently in one runtime. In this case, Lambda setup as 1GB mem, according to your job file size, you can optimize to best cost.

![Performance testing](./img/07b.png)  

* ap-northeast-1 AWS Lambda ( Using VPC NAT Instance with EIP and TCP BBR enabled ). Single file of 80GB video file and 45GB zip file. It takes about from 2 to 6 hours to complete transmit to cn-northwest-1. It is running in ONE Lambda concurrency.  

After one AWS Lambda runtime timeout of 15 minutes, SQS message InvisibleTime timeout also. The message recover to Visible, it trigger another Lambda runtime to fetch part number list from dest. Amazon S3 and continue subsequent parts upload. Here is snapshot of logs of Lambda:   

![Big file break-point resume](./img/06.png)  
1. AWS Lambda get dest. Amazon S3 upload id list, select the last time of record
2. Try to get part number list from dest. Amazon S3
3. While network disconnect, auto delay and retry
4. Got the part number list from dest. Amazon S3
5. Auto matching the rest part number 
6. Download and upload the rest parts

### Auto Deploy Dashboard  
![dashboard](./img/09.png)

### CDK auto deploy
Please follow instruction document to install CDK and deploy  
https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html   

```bash
cd cdk-serverless
cdk deploy
```
This CDK is written in PYTHON, and will deploy the following resources:  
* Create Amazon S3 Bucket. When new object created in this bucket, it will trigger sqs and start jobs.  
If you want to transmit existing S3 bucket, please leverage Jobsender from Cluster version of this solution to scan and compare S3, and then it will send jobs messages to Amazon SQS to trigger Lambda doing its jobs.  
* Notice: AWS CDK/CloudFormation doesn't support leverage existing S3 bucket to triiger SQS or Lambda.
* Create Amazon SQS Queue and a related SQS Queue Death Letter Queue. Set InVisibleTime 15 minutes, valid 14 days, 100 Maximum Receives Redrive Policy to DLQ  
* Create Amazon DynamoDB Table for statistic purpose  
* Upload AWS Lambda code and config environment variable. PLEASE MODIFY app.py OF THESE VARIABLE, OR MODIFY THEM IN LAMBDA ENV VAR AFTER CDK DEPLOY  
```
Des_bucket_default = 'your_des_bucket'
Des_prefix_default = 'my_prefix'
StorageClass = 'STANDARD'
aws_access_key_id = 'xxxxxxxxx'
aws_secret_access_key = 'xxxxxxxxxxxxxxx'
aws_access_key_region = 'cn-northwest-1'
```
In Amazon S3 new object trigger mode, you need to put the Default Des_bucket/prefix here.  
In Jobsender scan and send job mode, since Jobsender will get Des_bucket/prefix information from Parameter Store, then Lambda will ignore Default Des_bucket/prefix here. So you just need to put space for these two Env Var.
  
* Notice: For security reason, no recommend to put aws_access_key_id and aws_secret_access_key of the Destination Account here in CDK file. That might cause you leak it if not intented to commit the CDK file to some code repo.  It is better to config it in Lambda Env Var after CDK deployment. But cons is if you deploy cdk again, the variable will be overwritten.
* Config Lambda timeout 15 mintues and 1GB mem  
* Auto config Lambda role to access the new created S3, SQS queue and the DynamoDB Table  
* AWS CDK will create a new CloudWatch Dashboard: s3_migrate_serverless to monitor SQS queue and Lambda running status
* Will create 3 customized Log Filter for Lambda Log Group. They are to filter Uploading, Downloading, Complete parts Bytes, and statistic them as Lambda traffic and publish to Dashboard
* Create an Alarm, while detect SQS queue is empty. I.e. no Visible and no InVisible message, then send SNS to subcription email to inform all jobs are done. The email address is defined in CDK app.py, please change it or change in SNS after CDK deploy.


### If Manually Config the whole solution:  
If you don't want to use AWS CDK to auto deploy, you can follow above CDK deploy statment to create related resource, and remind:
* Amazon SQS Access Policy to allow Amazon S3 bucket to send message as below, please change the account and bucket in this json:
```json
{
  "Version": "2012-10-17",
  "Id": "arn:aws:sqs:us-east-1:your_account:s3_migrate_file_list/SQSDefaultPolicy",
  "Statement": [
    {
      "Sid": "Sidx",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "SQS:SendMessage",
      "Resource": "arn:aws:sqs:us-east-1:your_account:s3_migrate_file_list",
      "Condition": {
        "ArnLike": {
          "aws:SourceArn": "arn:aws:s3:::source_bucket"
        }
      }
    }
  ]
}
```
* Config Amazon S3 bucket event trigger SQS queue  
* Config AWS Lambda execution role as:  
Access Read/Write Amazon SQS and DynamoDB  
Read Srouce Amazon S3 Bucket  
Put Amazon CloudWatch Logs  
* Config AWS Lambda Language Python 3.8 and upload these two code files, located in
```
amazon-s3-resumable-upload/serverless/cdk-serverless/lambda/  
lambda_function.py  
s3_migration_lib.py  
```
* Config Lambda Environment Variable:
```
Des_bucket_default  
Des_prefix_default  
```
In Amazon S3 new object trigger mode, you need to put the Default Des_bucket/prefix here.  
In Jobsender scan and send job mode, since Jobsender will get Des_bucket/prefix information from Parameter Store, then Lambda will ignore Default Des_bucket/prefix here. So you just need to put space for these two Env Var.
```
StorageClass  
```
Select Destination S3 storage class
STANDARD|REDUCED_REDUNDANCY|STANDARD_IA|ONEZONE_IA|INTELLIGENT_TIERING|GLACIER|DEEP_ARCHIVE
```
aws_access_key_id  
aws_secret_access_key  
aws_access_key_region  
```
To access the S3 account which is not the same account as Lambda. You can get from that account IAM user credential.   
aws_access_key_region code, e.g. cn-north-1
```
table_queue_name  
```
The DynamoDB table name, same as the tale created by CloudFormation/CDK  

* Lambda timeout 15 mins, mem 1GB or you can try optimize it according to your workload. If neccessary, you can change these 2 para in python code to help optimize performance MaxThread, max_pool_connections for your workload. 
* The JobType in python code is to define Amazon S3 credential usage.  
PUT mode means Lambda role as source Amazon S3 client and access_key credentials as destination S3 client.  
GET mode means Lambda role as destination Amazon S3 client and access_key credentials as source S3 client.
* Config Lambda is trigger by Amazon SQS, batch size 1.  

* Create Lambda log group with 3 Log filter, to match below Pattern:
```
Namespace: s3_migrate
Filter name: Downloading-bytes
Pattern: [info, date, sn, p="--->Downloading", bytes, key]
Value: $bytes
default value: 0
Filter name: Uploading-bytes
Pattern: [info, date, sn, p="--->Uploading", bytes, key]
Value: $bytes
default value: 0
Filter name: Complete-bytes
Pattern: [info, date, sn, p="--->Complete", bytes, key]
Value: $bytes
default value: 0
```
So Lambda traffic bytes/min will be emit to CloudWatch Metric for monitoring. Config monitor Statistic: Sum with 1 min.

* Config Amazon SQS Alarm, expression metric Visible + InVisible Messages <= 0 3 of 3 then trigger Amazon SNS Notification for empty queue.

## Limitation 
* It doesn't support version control, but only get the lastest version of object from S3. Don't change the original file while copying.  
If you change, it will cause the destination file wrong. Workaround is to run the Jobsender(cluster version) after each batch of migration job. Jobsender will compare source and destination s3 file key and size, if different size, it will send jobs to sqs to trigger copy again. But if size is same, it will take it as same file.  

* Don't change the chunksize while start data copying.  

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
