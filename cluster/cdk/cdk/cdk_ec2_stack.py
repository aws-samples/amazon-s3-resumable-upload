from aws_cdk import core
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_autoscaling as autoscaling
import aws_cdk.aws_iam as iam

# Adjust ec2 type for worker autoscaling group here
worker_type = "c5.large"

linux_ami = ec2.AmazonLinuxImage(generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX_2,
                                 edition=ec2.AmazonLinuxEdition.STANDARD,
                                 virtualization=ec2.AmazonLinuxVirt.HVM,
                                 storage=ec2.AmazonLinuxStorage.GENERAL_PURPOSE
                                 )
with open("./cdk/user_data_worker.sh") as f:
    user_data_worker = f.read()
with open("./cdk/user_data_master.sh") as f:
    user_data_master = f.read()


class CdkEc2Stack(core.Stack):

    def __init__(self, scope: core.Construct, _id: str, vpc,
                 # key_name,
                 ddb_file_list, sqs_queue, sqs_queue_DLQ, ssm_bucket_para, ssm_credential_para, **kwargs) -> None:
        super().__init__(scope, _id, **kwargs)

        # Create master node
        master = ec2.Instance(self, "master",
                              instance_name="s3_migrate_cluster_master",
                              instance_type=ec2.InstanceType.of(
                                  ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MICRO),
                              machine_image=linux_ami,
                              # key_name=key_name,
                              user_data=ec2.UserData.custom(user_data_master),
                              vpc=vpc,
                              vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC))
        master.connections.allow_from_any_ipv4(ec2.Port.tcp(22), "Internet access SSH")
        # SSH is optional if you use SSM-SessionManager
        master.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"))
        master.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess"))
        master.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"))

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
        worker_asg.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"))
        worker_asg.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess"))
        worker_asg.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"))

        ddb_file_list.grant_full_access(master)
        ddb_file_list.grant_full_access(worker_asg)

        sqs_queue.grant_consume_messages(master)
        sqs_queue.grant_consume_messages(worker_asg)
        sqs_queue_DLQ.grant_consume_messages(master)
        sqs_queue_DLQ.grant_consume_messages(worker_asg)

        ssm_bucket_para.grant_read(master)
        ssm_bucket_para.grant_read(worker_asg)
        ssm_credential_para.grant_read(master)
        ssm_credential_para.grant_read(worker_asg)

        core.CfnOutput(self, "Output", value=master.instance_id)
