#!/usr/bin/env python3

from aws_cdk import core

from cdk.cdk_vpc_stack import CdkVpcStack
from cdk.cdk_ec2_stack import CdkEc2Stack
from cdk.cdk_resource_stack import CdkResourceStack

############
# Define bucket before deploy CDK
bucket_para = [{
    "src_bucket": "broad-references",
    "src_prefix": "",
    "des_bucket": "s3-migration-test-nx",
    "des_prefix": "broad-references"
    }, {
    "src_bucket": "gatk-test-data",
    "src_prefix": "",
    "des_bucket": "s3-migration-test-nx",
    "des_prefix": "gatk-test-data"
    }]

# key_name = "id_rsa"  # Optional if use SSM-SessionManager

'''
BEFORE DEPLOY CDK, please setup a "s3_migration_credentials" secure parameter in ssm parameter store MANUALLY!
This is the access_key which is not in the same account as ec2.
For example, if ec2 running in Global, this is China Account access_key. Example as below:
{
    "aws_access_key_id": "your_aws_access_key_id",
    "aws_secret_access_key": "your_aws_secret_access_key",
    "region": "cn-northwest-1"
}
CDK don not allow to deploy secure para, so you have to do it mannually
And then in this template will assign ec2 role to access it.
请在部署CDK前，先在ssm parameter store手工创建一个名为 "s3_migration_credentials" 的 secure parameter：
这个是跟EC2不在一个Account体系下的另一个Account的access_key
例如EC2在Global，则这个是China Account access_key，反之EC2在中国，这就是Global Account
CDK 不允许直接部署加密Key，所以你需要先去手工创建，然后在CDK中会赋予EC2角色有读取权限
'''

app = core.App()
vpc_stack = CdkVpcStack(app, "s3-migration-cluster-vpc")
vpc = vpc_stack.vpc

resource_stack = CdkResourceStack(app, "s3-migration-cluster-resource", bucket_para)

ec2_stack = CdkEc2Stack(app, "s3-migration-cluster-ec2", vpc, bucket_para,
                        # key_name,   # Optional if use SSM-SessionManager
                        resource_stack.ddb_file_list,
                        resource_stack.sqs_queue,
                        resource_stack.sqs_queue_DLQ,
                        resource_stack.ssm_bucket_para,
                        resource_stack.ssm_credential_para,
                        resource_stack.s3bucket)

app.synth()
