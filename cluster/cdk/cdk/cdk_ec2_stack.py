from aws_cdk import core
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_autoscaling as autoscaling
import aws_cdk.aws_iam as iam
import aws_cdk.aws_logs as log
import aws_cdk.aws_s3 as s3

# Adjust ec2 type for worker autoscaling group here
worker_type = "c5.large"

linux_ami = ec2.AmazonLinuxImage(generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX_2,
                                 edition=ec2.AmazonLinuxEdition.STANDARD,
                                 virtualization=ec2.AmazonLinuxVirt.HVM,
                                 storage=ec2.AmazonLinuxStorage.GENERAL_PURPOSE
                                 )
with open("./cdk/user_data_worker.sh") as f:
    user_data_worker = f.read()
with open("./cdk/user_data_jobsender.sh") as f:
    user_data_jobsender = f.read()


class CdkEc2Stack(core.Stack):

    def __init__(self, scope: core.Construct, _id: str, vpc, bucket_para,
                 # key_name,
                 ddb_file_list, sqs_queue, sqs_queue_DLQ, ssm_bucket_para, ssm_credential_para, s3bucket,
                 **kwargs) -> None:
        super().__init__(scope, _id, **kwargs)

        # Create jobsender node
        jobsender = ec2.Instance(self, "jobsender",
                                 instance_name="s3_migrate_cluster_jobsender",
                                 instance_type=ec2.InstanceType.of(
                                     ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MICRO),
                                 machine_image=linux_ami,
                                 # key_name=key_name,
                                 user_data=ec2.UserData.custom(user_data_jobsender),
                                 vpc=vpc,
                                 vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC))
        # jobsender.connections.allow_from_any_ipv4(ec2.Port.tcp(22), "Internet access SSH")
        # SSH is optional if you use SSM-SessionManager
        jobsender.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"))
        jobsender.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess"))
        # jobsender.role.add_managed_policy(
        #     iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"))

        # Create Autoscaling Group with fixed 2*EC2 hosts
        worker_asg = autoscaling.AutoScalingGroup(self, "worker-asg",
                                                  vpc=vpc,
                                                  vpc_subnets=ec2.SubnetSelection(
                                                      subnet_type=ec2.SubnetType.PUBLIC),
                                                  instance_type=ec2.InstanceType(
                                                      instance_type_identifier=worker_type),
                                                  machine_image=linux_ami,
                                                  # key_name=key_name,  # Optional if use SSM-SessionManager
                                                  user_data=ec2.UserData.custom(user_data_worker),
                                                  desired_capacity=0,
                                                  min_capacity=0,
                                                  max_capacity=10,
                                                  cooldown=core.Duration.minutes(20),
                                                  spot_price="0.5"
                                                  )
        # worker_asg.connections.allow_from_any_ipv4(ec2.Port.tcp(22), "Internet access SSH")

        # 赋予EC2 SSM 管理角色，这样SSM可以远程管理，可以用SessionManager登录
        worker_asg.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"))

        # 创建 CloudWatchLogs 允许EC2发logs
        log_group = log.LogGroup(self, "s3-migration-ec2-loggroup",
                                 retention=log.RetentionDays.ONE_MONTH)
        log_group.grant_write(worker_asg)
        log_group.grant_write(jobsender)

        # 允许访问新建的DynamoDB表
        ddb_file_list.grant_full_access(jobsender)
        ddb_file_list.grant_full_access(worker_asg)

        # 允许 EC2 访问 新建的SQS队列和对应的DLQ
        sqs_queue.grant_consume_messages(jobsender)
        sqs_queue.grant_send_messages(jobsender)
        sqs_queue.grant_consume_messages(worker_asg)
        sqs_queue_DLQ.grant_consume_messages(jobsender)
        sqs_queue_DLQ.grant_consume_messages(worker_asg)

        # 允许 EC2 访问 SSM Parameter Store，获取bucket信息，和获取credential信息
        ssm_bucket_para.grant_read(jobsender)
        ssm_bucket_para.grant_read(worker_asg)
        ssm_credential_para.grant_read(jobsender)
        ssm_credential_para.grant_read(worker_asg)

        # 允许 EC2 访问新建的S3bucket
        s3bucket.grant_read(jobsender)
        s3bucket.grant_read(worker_asg)

        # 允许 EC2 访问现有的S3
        bucket_name = ''
        for b in bucket_para:
            if bucket_name != b['src_bucket']:  # 如果列了多个相同的Bucket，就跳过
                bucket_name = b['src_bucket']
                s3exist_bucket = s3.Bucket.from_bucket_name(self,
                                                            bucket_name,  # 用这个做id
                                                            bucket_name=bucket_name)
                s3exist_bucket.grant_read(jobsender)
                s3exist_bucket.grant_read(worker_asg)

        core.CfnOutput(self, "JobSenderEC2", value=jobsender.instance_id)
        core.CfnOutput(self, "WorkerEC2AutoscalingGroup", value=worker_asg.auto_scaling_group_name)
