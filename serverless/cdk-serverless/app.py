#!/usr/bin/env python3
from aws_cdk import core
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_dynamodb as ddb
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_lambda as lam
from aws_cdk.aws_lambda_event_sources import SqsEventSource
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_cloudwatch as cw
import aws_cdk.aws_cloudwatch_actions as action
import aws_cdk.aws_sns as sns
import aws_cdk.aws_sns_subscriptions as sub
import aws_cdk.aws_logs as logs

Des_bucket_default = 'your_des_bucket'
Des_prefix_default = 'my_prefix'
StorageClass = 'STANDARD'
aws_access_key_id = 'xxxxxxxxx'
aws_secret_access_key = 'xxxxxxxxxxxxxxx'
aws_access_key_region = 'cn-northwest-1'

# Setup your alarm email
alarm_email = "alarm_your_email@email.com"

# After deploy CDK, please add access key config in Lambda Evironment, so it can access destination S3
# 请在CDK部署完成后，到Lambda的环境变量中修改 access key 配置，以便让Lambda有权限访问目标S3


class CdkResourceStack(core.Stack):

    def __init__(self, scope: core.Construct, _id: str, **kwargs) -> None:
        super().__init__(scope, _id, **kwargs)

        ddb_file_list = ddb.Table(self, "ddb",
                                  partition_key=ddb.Attribute(name="Key", type=ddb.AttributeType.STRING),
                                  billing_mode=ddb.BillingMode.PAY_PER_REQUEST)

        sqs_queue_DLQ = sqs.Queue(self, "sqs_DLQ",
                                  visibility_timeout=core.Duration.minutes(15),
                                  retention_period=core.Duration.days(14)
                                  )
        sqs_queue = sqs.Queue(self, "sqs_queue",
                              visibility_timeout=core.Duration.minutes(15),
                              retention_period=core.Duration.days(14),
                              dead_letter_queue=sqs.DeadLetterQueue(
                                  max_receive_count=100,
                                  queue=sqs_queue_DLQ
                              )
                              )
        handler = lam.Function(self, "lambdaFunction",
                               code=lam.Code.asset("./lambda"),
                               handler="lambda_function.lambda_handler",
                               runtime=lam.Runtime.PYTHON_3_8,
                               memory_size=1024,
                               timeout=core.Duration.minutes(15),
                               environment={
                                   'table_queue_name': ddb_file_list.table_name,
                                   'Des_bucket_default': Des_bucket_default,
                                   'Des_prefix_default': Des_prefix_default,
                                   'StorageClass': StorageClass,
                                   'aws_access_key_id': aws_access_key_id,
                                   'aws_secret_access_key': aws_secret_access_key,
                                   'aws_access_key_region': aws_access_key_region
                               })
        ddb_file_list.grant_read_write_data(handler)
        handler.add_event_source(SqsEventSource(sqs_queue))

        s3bucket = s3.Bucket(self, "s3bucket")
        s3bucket.grant_read(handler)
        s3bucket.add_event_notification(s3.EventType.OBJECT_CREATED,
                                        s3n.SqsDestination(sqs_queue))

        # You can import an existing bucket and grant access to lambda
        # exist_s3bucket = s3.Bucket.from_bucket_name(self, "import_bucket",
        #                                             bucket_name="you_bucket_name")
        # exist_s3bucket.grant_read(handler)

        # But You have to add sqs as imported bucket event notification manually, it doesn't support by CloudFormation
        # An work around is to add on_cloud_trail_event for the bucket, but will trigger could_trail first
        # 因为是导入的Bucket，需要手工建Bucket Event Trigger SQS，以及设置SQS允许该bucekt触发的Permission

        core.CfnOutput(self, "DynamoDB_Table", value=ddb_file_list.table_name)
        core.CfnOutput(self, "SQS_Job_Queue", value=sqs_queue.queue_name)
        core.CfnOutput(self, "SQS_Job_Queue_DLQ", value=sqs_queue_DLQ.queue_name)
        core.CfnOutput(self, "Worker_Lambda_Function", value=handler.function_name)
        core.CfnOutput(self, "New_S3_Bucket", value=s3bucket.bucket_name)

        # Create Lambda logs filter to create network traffic metric
        handler.log_group.add_metric_filter("Complete-bytes",
                                            metric_name="Complete-bytes",
                                            metric_namespace="s3_migrate",
                                            default_value=0,
                                            metric_value="$bytes",
                                            filter_pattern=logs.FilterPattern.literal(
                                                '[info, date, sn, p="--->Complete", bytes, key]'))
        handler.log_group.add_metric_filter("Uploading-bytes",
                                            metric_name="Uploading-bytes",
                                            metric_namespace="s3_migrate",
                                            default_value=0,
                                            metric_value="$bytes",
                                            filter_pattern=logs.FilterPattern.literal(
                                                '[info, date, sn, p="--->Uploading", bytes, key]'))
        handler.log_group.add_metric_filter("Downloading-bytes",
                                            metric_name="Downloading-bytes",
                                            metric_namespace="s3_migrate",
                                            default_value=0,
                                            metric_value="$bytes",
                                            filter_pattern=logs.FilterPattern.literal(
                                                '[info, date, sn, p="--->Downloading", bytes, key]'))
        lambda_metric_Complete = cw.Metric(namespace="s3_migrate",
                                           metric_name="Complete-bytes",
                                           statistic="Sum",
                                           period=core.Duration.minutes(1))
        lambda_metric_Upload = cw.Metric(namespace="s3_migrate",
                                         metric_name="Uploading-bytes",
                                         statistic="Sum",
                                         period=core.Duration.minutes(1))
        lambda_metric_Download = cw.Metric(namespace="s3_migrate",
                                           metric_name="Downloading-bytes",
                                           statistic="Sum",
                                           period=core.Duration.minutes(1))
        # Dashboard to monitor SQS and Lambda
        board = cw.Dashboard(self, "s3_migrate", dashboard_name="s3_migrate_serverless")

        board.add_widgets(cw.GraphWidget(title="Lambda-NETWORK",
                                         left=[lambda_metric_Download, lambda_metric_Upload, lambda_metric_Complete]),
                          # TODO: here monitor all lambda concurrency not just the working one. Limitation from CDK
                          # Lambda now supports monitor single lambda concurrency, will change this after CDK support
                          cw.GraphWidget(title="Lambda-all-concurrent",
                                         left=[handler.metric_all_concurrent_executions()]),

                          cw.GraphWidget(title="Lambda-invocations/errors/throttles",
                                         left=[handler.metric_invocations(),
                                               handler.metric_errors(),
                                               handler.metric_throttles()]),
                          cw.GraphWidget(title="Lambda-duration",
                                         left=[handler.metric_duration()]),
                          )

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

        # Alarm for queue empty
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
                           alarm_name="SQS queue empty-Serverless",
                           metric=metric_all_message,
                           threshold=0,
                           comparison_operator=cw.ComparisonOperator.LESS_THAN_OR_EQUAL_TO_THRESHOLD,
                           evaluation_periods=3,
                           datapoints_to_alarm=3,
                           treat_missing_data=cw.TreatMissingData.IGNORE
                           )
        alarm_topic = sns.Topic(self, "SQS queue empty-Serverless")
        alarm_topic.add_subscription(subscription=sub.EmailSubscription(alarm_email))
        alarm_0.add_alarm_action(action.SnsAction(alarm_topic))

        core.CfnOutput(self, "Dashboard", value="CloudWatch Dashboard name s3_migrate_serverless")
        core.CfnOutput(self, "Alarm", value="CloudWatch SQS queue empty Alarm for Serverless: "+alarm_email)


###############
app = core.App()
CdkResourceStack(app, "s3-migration-serverless")
# MyStack(app, "MyStack", env=core.Environment(region="REGION",account="ACCOUNT")
app.synth()
