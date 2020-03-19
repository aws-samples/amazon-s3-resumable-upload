from aws_cdk import core
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_autoscaling as autoscaling
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_cloudwatch as cw
import aws_cdk.aws_cloudwatch_actions as action
import aws_cdk.aws_sns as sns
import aws_cdk.aws_sns_subscriptions as sub

# Adjust ec2 type here
worker_type = "c5.large"
jobsender_type = "t3.micro"

# Setup your alarm email
alarm_email = "alarm_your_email@email.com"

# Specify your AMI here
linux_ami = ec2.AmazonLinuxImage(generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX_2,
                                 edition=ec2.AmazonLinuxEdition.STANDARD,
                                 virtualization=ec2.AmazonLinuxVirt.HVM,
                                 storage=ec2.AmazonLinuxStorage.GENERAL_PURPOSE
                                 )
# Load your user data for ec2
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

        # Create jobsender ec2 node
        jobsender = ec2.Instance(self, "jobsender",
                                 instance_name="s3_migrate_cluster_jobsender",
                                 instance_type=ec2.InstanceType(
                                     instance_type_identifier=jobsender_type),
                                 machine_image=linux_ami,
                                 # key_name=key_name,
                                 user_data=ec2.UserData.custom(user_data_jobsender),
                                 vpc=vpc,
                                 vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC))

        # jobsender.connections.allow_from_any_ipv4(ec2.Port.tcp(22), "Internet access SSH")
        # Don't need SSH since we use Session Manager

        # Assign EC2 Policy to use SSM and CWAgent
        jobsender.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"))
        jobsender.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchAgentServerPolicy"))

        # jobsender.role.add_managed_policy(
        #     iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"))
        # Don't give full access s3 to ec2, violate security rule

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
                                                  desired_capacity=1,
                                                  min_capacity=1,
                                                  max_capacity=10,
                                                  cooldown=core.Duration.minutes(20),
                                                  spot_price="0.5"
                                                  )

        # worker_asg.connections.allow_from_any_ipv4(ec2.Port.tcp(22), "Internet access SSH")
        # Don't need SSH since we use Session Manager

        # Assign EC2 Policy to use SSM and CWAgent
        worker_asg.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"))
        worker_asg.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchAgentServerPolicy"))

        # Allow EC2 access new DynamoDB Table
        ddb_file_list.grant_full_access(jobsender)
        ddb_file_list.grant_full_access(worker_asg)

        # Allow EC2 access new sqs and its DLQ
        sqs_queue.grant_consume_messages(jobsender)
        sqs_queue.grant_send_messages(jobsender)
        sqs_queue.grant_consume_messages(worker_asg)
        sqs_queue_DLQ.grant_consume_messages(jobsender)

        # Allow EC2 access SSM Parameter Store, get bucket infor and get credential
        ssm_bucket_para.grant_read(jobsender)
        ssm_credential_para.grant_read(jobsender)
        ssm_credential_para.grant_read(worker_asg)

        # Allow EC2 access new s3 bucket
        s3bucket.grant_read(jobsender)
        s3bucket.grant_read(worker_asg)

        # Allow EC2 access exist s3 bucket
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

        # Dashboard to monitor SQS and EC2
        board = cw.Dashboard(self, "s3_migrate", dashboard_name="s3_migrate")

        board.add_widgets(cw.GraphWidget(title="SQS-Jobs",
                                         left=[sqs_queue.metric_approximate_number_of_messages_visible(),
                                               sqs_queue.metric_approximate_number_of_messages_not_visible()]),
                          cw.GraphWidget(title="SQS-DeadLetterQueue",
                                         left=[sqs_queue_DLQ.metric_approximate_number_of_messages_visible(),
                                               sqs_queue_DLQ.metric_approximate_number_of_messages_not_visible()]),
                          cw.SingleValueWidget(title="Running/Waiting Jobs",
                                               metrics=[sqs_queue.metric_approximate_number_of_messages_not_visible(),
                                                        sqs_queue.metric_approximate_number_of_messages_visible()]),
                          cw.SingleValueWidget(title="Death Jobs",
                                               metrics=[
                                                   sqs_queue_DLQ.metric_approximate_number_of_messages_not_visible(),
                                                   sqs_queue_DLQ.metric_approximate_number_of_messages_visible()])
                          )
        ec2_metric_net = cw.Metric(namespace="AWS/EC2",
                                   metric_name="NetworkOut",
                                   dimensions={"AutoScalingGroupName": worker_asg.auto_scaling_group_name})
        ec2_metric_cpu = cw.Metric(namespace="AWS/EC2",
                                   metric_name="CPUUtilization",
                                   dimensions={"AutoScalingGroupName": worker_asg.auto_scaling_group_name})
        board.add_widgets(cw.GraphWidget(title="EC2-AutoscalingGroup-NETWORK",
                                         left=[ec2_metric_net]),
                          cw.GraphWidget(title="EC2-AutoscalingGroup-CPU",
                                         left=[ec2_metric_cpu])
                          )

        # Autoscaling up when visible message > 100 every 5 mins
        worker_asg.scale_on_metric("scaleup", metric=sqs_queue.metric_approximate_number_of_messages_visible(),
                                   scaling_steps=[autoscaling.ScalingInterval(
                                       change=2,
                                       lower=100
                                   ),
                                       autoscaling.ScalingInterval(
                                           change=0,
                                           upper=100,
                                           lower=0
                                       )],
                                   adjustment_type=autoscaling.AdjustmentType.CHANGE_IN_CAPACITY)

        # Alarm for queue empty, i.e. no visible message and no in-visible message
        metric_all_message = cw.MathExpression(
            expression="a + b",
            label="empty_queue_expression",
            using_metrics={
                "a": sqs_queue.metric_approximate_number_of_messages_visible(),
                "b": sqs_queue.metric_approximate_number_of_messages_not_visible()
            }
        )
        alarm_0 = cw.Alarm(self, "SQSempty",
                           alarm_name="SQS queue empty",
                           metric=metric_all_message,
                           threshold=0,
                           comparison_operator=cw.ComparisonOperator.LESS_THAN_OR_EQUAL_TO_THRESHOLD,
                           evaluation_periods=3,
                           datapoints_to_alarm=3,
                           treat_missing_data=cw.TreatMissingData.IGNORE
                           )
        alarm_topic = sns.Topic(self, "SQS queue empty")
        alarm_topic.add_subscription(subscription=sub.EmailSubscription(alarm_email))
        alarm_0.add_alarm_action(action.SnsAction(alarm_topic))

        # If queue empty, set worker to 1
        action_shutdown = autoscaling.StepScalingAction(self, "shutdown",
                                                        auto_scaling_group=worker_asg,
                                                        adjustment_type=autoscaling.AdjustmentType.EXACT_CAPACITY
                                                        )
        action_shutdown.add_adjustment(adjustment=1, lower_bound=1)
        alarm_0.add_alarm_action(action.AutoScalingAction(action_shutdown))
