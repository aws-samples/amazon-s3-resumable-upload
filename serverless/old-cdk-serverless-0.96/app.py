#!/usr/bin/env python3
from aws_cdk import core
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_dynamodb as ddb
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_lambda as lam
from aws_cdk.aws_lambda_event_sources import SqsEventSource
import aws_cdk.aws_ssm as ssm
import aws_cdk.aws_cloudwatch as cw
import aws_cdk.aws_cloudwatch_actions as action
import aws_cdk.aws_events as event
import aws_cdk.aws_events_targets as target
import aws_cdk.aws_sns as sns
import aws_cdk.aws_sns_subscriptions as sub
import aws_cdk.aws_logs as logs
import aws_cdk.aws_apigateway as api
import json

# Define bucket parameter before deploy CDK
bucket_para = [{
    "src_bucket": "broad-references",
    "src_prefix": "",
    "des_bucket": "s3-migration-test-nx",
    "des_prefix": "broad-references-20200405b"
}, {
    "src_bucket": "covid19-lake",
    "src_prefix": "",
    "des_bucket": "covid19-lake",
    "des_prefix": ""
}]
StorageClass = 'STANDARD'

# Des_bucket_default is only used for non-jobsender senario: S3 trigger SQS then to Lambda,
# There is no destination buckets information in SQS message in this case, so you need to setup Des_bucket_default
Des_bucket_default = 's3-migration-test-nx'
Des_prefix_default = 's3-migration-cdk-from-us'

# The region credential (not the same account as Lambda) setting in SSM Parameter Store
ssm_parameter_credentials = 's3_migration_credentials'
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

# Setup your alarm email
alarm_email = "alarm_your_email@email.com"

# Setup ignore files list, you can put the file name/wildcard for ignoring in this txt file:
try:
    with open("s3_migration_ignore_list.txt", "r") as f:
        ignore_list = f.read()
except Exception:
    ignore_list = ""


class CdkResourceStack(core.Stack):

    def __init__(self, scope: core.Construct, _id: str, **kwargs) -> None:
        super().__init__(scope, _id, **kwargs)

        # Setup SSM parameter of credentials, bucket_para, ignore_list
        ssm_credential_para = ssm.StringParameter.from_secure_string_parameter_attributes(
            self, "ssm_parameter_credentials",
            parameter_name=ssm_parameter_credentials,
            version=1
        )

        ssm_bucket_para = ssm.StringParameter(self, "s3bucket_serverless",
                                              string_value=json.dumps(bucket_para, indent=4)
                                              )

        ssm_parameter_ignore_list = ssm.StringParameter(self, "s3_migrate_ignore_list",
                                                        string_value=ignore_list)

        # Setup DynamoDB
        ddb_file_list = ddb.Table(self, "s3migrate_serverless",
                                  partition_key=ddb.Attribute(name="Key", type=ddb.AttributeType.STRING),
                                  billing_mode=ddb.BillingMode.PAY_PER_REQUEST)

        # Setup SQS
        sqs_queue_DLQ = sqs.Queue(self, "s3migrate_serverless_Q_DLQ",
                                  visibility_timeout=core.Duration.minutes(15),
                                  retention_period=core.Duration.days(14)
                                  )
        sqs_queue = sqs.Queue(self, "s3migrate_serverless_Q",
                              visibility_timeout=core.Duration.minutes(15),
                              retention_period=core.Duration.days(14),
                              dead_letter_queue=sqs.DeadLetterQueue(
                                  max_receive_count=3,
                                  queue=sqs_queue_DLQ
                              )
                              )

        # Setup API for Lambda to get IP address (for debug networking routing purpose)
        checkip = api.RestApi(self, "lambda-checkip-api",
                              cloud_watch_role=True,
                              deploy=True,
                              description="For Lambda get IP address",
                              default_integration=api.MockIntegration(
                                  integration_responses=[api.IntegrationResponse(
                                      status_code="200",
                                      response_templates={"application/json": "$context.identity.sourceIp"})
                                  ],
                                  request_templates={"application/json": '{"statusCode": 200}'}
                              ),
                              endpoint_types=[api.EndpointType.REGIONAL])
        checkip.root.add_method("GET", method_responses=[api.MethodResponse(
            status_code="200",
            response_models={"application/json": api.Model.EMPTY_MODEL}
        )])

        # Setup Lambda functions
        handler = lam.Function(self, "s3-migrate-worker",
                               code=lam.Code.asset("./lambda"),
                               handler="lambda_function_worker.lambda_handler",
                               runtime=lam.Runtime.PYTHON_3_8,
                               memory_size=1024,
                               timeout=core.Duration.minutes(15),
                               tracing=lam.Tracing.ACTIVE,
                               environment={
                                   'table_queue_name': ddb_file_list.table_name,
                                   'Des_bucket_default': Des_bucket_default,
                                   'Des_prefix_default': Des_prefix_default,
                                   'StorageClass': StorageClass,
                                   'checkip_url': checkip.url,
                                   'ssm_parameter_credentials': ssm_parameter_credentials
                               })

        handler_jobsender = lam.Function(self, "s3-migrate-jobsender",
                                         code=lam.Code.asset("./lambda"),
                                         handler="lambda_function_jobsender.lambda_handler",
                                         runtime=lam.Runtime.PYTHON_3_8,
                                         memory_size=1024,
                                         timeout=core.Duration.minutes(15),
                                         tracing=lam.Tracing.ACTIVE,
                                         environment={
                                             'table_queue_name': ddb_file_list.table_name,
                                             'StorageClass': StorageClass,
                                             'checkip_url': checkip.url,
                                             'sqs_queue': sqs_queue.queue_name,
                                             'ssm_parameter_credentials': ssm_parameter_credentials,
                                             'ssm_parameter_ignore_list': ssm_parameter_ignore_list.parameter_name,
                                             'ssm_parameter_bucket': ssm_bucket_para.parameter_name
                                         })

        # Allow lambda read/write DDB, SQS
        ddb_file_list.grant_read_write_data(handler)
        ddb_file_list.grant_read_write_data(handler_jobsender)
        sqs_queue.grant_send_messages(handler_jobsender)
        # SQS trigger Lambda worker
        handler.add_event_source(SqsEventSource(sqs_queue, batch_size=1))

        # Option1: Create S3 Bucket, all new objects in this bucket will be transmitted by Lambda Worker
        s3bucket = s3.Bucket(self, "s3_new_migrate")
        s3bucket.grant_read(handler)
        s3bucket.add_event_notification(s3.EventType.OBJECT_CREATED,
                                        s3n.SqsDestination(sqs_queue))

        # Option2: Allow Exist S3 Buckets to be read by Lambda functions.
        # Lambda Jobsender will scan and compare the these buckets and trigger Lambda Workers to transmit
        bucket_name = ''
        for b in bucket_para:
            if bucket_name != b['src_bucket']:  # 如果列了多个相同的Bucket，就跳过
                bucket_name = b['src_bucket']
                s3exist_bucket = s3.Bucket.from_bucket_name(self,
                                                            bucket_name,  # 用这个做id
                                                            bucket_name=bucket_name)
                s3exist_bucket.grant_read(handler_jobsender)
                s3exist_bucket.grant_read(handler)

        # Allow Lambda read ssm parameters
        ssm_bucket_para.grant_read(handler_jobsender)
        ssm_credential_para.grant_read(handler)
        ssm_credential_para.grant_read(handler_jobsender)
        ssm_parameter_ignore_list.grant_read(handler_jobsender)

        # Schedule cron event to trigger Lambda Jobsender per hour:
        event.Rule(self, 'cron_trigger_jobsender',
                   schedule=event.Schedule.rate(core.Duration.hours(1)),
                   targets=[target.LambdaFunction(handler_jobsender)])

        # Create Lambda logs filter to create network traffic metric
        handler.log_group.add_metric_filter("Complete-bytes",
                                            metric_name="Complete-bytes",
                                            metric_namespace="s3_migrate",
                                            metric_value="$bytes",
                                            filter_pattern=logs.FilterPattern.literal(
                                                '[info, date, sn, p="--->Complete", bytes, key]'))
        handler.log_group.add_metric_filter("Uploading-bytes",
                                            metric_name="Uploading-bytes",
                                            metric_namespace="s3_migrate",
                                            metric_value="$bytes",
                                            filter_pattern=logs.FilterPattern.literal(
                                                '[info, date, sn, p="--->Uploading", bytes, key]'))
        handler.log_group.add_metric_filter("Downloading-bytes",
                                            metric_name="Downloading-bytes",
                                            metric_namespace="s3_migrate",
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
        handler.log_group.add_metric_filter("ERROR",
                                            metric_name="ERROR-Logs",
                                            metric_namespace="s3_migrate",
                                            metric_value="1",
                                            filter_pattern=logs.FilterPattern.literal(
                                                '"ERROR"'))
        handler.log_group.add_metric_filter("WARNING",
                                            metric_name="WARNING-Logs",
                                            metric_namespace="s3_migrate",
                                            metric_value="1",
                                            filter_pattern=logs.FilterPattern.literal(
                                                '"WARNING"'))
        log_metric_ERROR = cw.Metric(namespace="s3_migrate",
                                     metric_name="ERROR-Logs",
                                     statistic="Sum",
                                     period=core.Duration.minutes(1))
        log_metric_WARNING = cw.Metric(namespace="s3_migrate",
                                       metric_name="WARNING-Logs",
                                       statistic="Sum",
                                       period=core.Duration.minutes(1))

        # Dashboard to monitor SQS and Lambda
        board = cw.Dashboard(self, "s3_migrate_serverless")

        board.add_widgets(cw.GraphWidget(title="Lambda-NETWORK",
                                         left=[lambda_metric_Download, lambda_metric_Upload, lambda_metric_Complete]),
                          # TODO: here monitor all lambda concurrency not just the working one. Limitation from CDK
                          # Lambda now supports monitor single lambda concurrency, will change this after CDK support
                          cw.GraphWidget(title="Lambda-all-concurrent",
                                         left=[handler.metric_all_concurrent_executions(
                                             period=core.Duration.minutes(1))]),

                          cw.GraphWidget(title="Lambda-invocations/errors/throttles",
                                         left=[handler.metric_invocations(period=core.Duration.minutes(1)),
                                               handler.metric_errors(period=core.Duration.minutes(1)),
                                               handler.metric_throttles(period=core.Duration.minutes(1))]),
                          cw.GraphWidget(title="Lambda-duration",
                                         left=[handler.metric_duration(period=core.Duration.minutes(1))]),
                          )

        board.add_widgets(cw.GraphWidget(title="SQS-Jobs",
                                         left=[sqs_queue.metric_approximate_number_of_messages_visible(
                                             period=core.Duration.minutes(1)
                                         ),
                                             sqs_queue.metric_approximate_number_of_messages_not_visible(
                                                 period=core.Duration.minutes(1)
                                             )]),
                          cw.GraphWidget(title="SQS-DeadLetterQueue",
                                         left=[sqs_queue_DLQ.metric_approximate_number_of_messages_visible(
                                             period=core.Duration.minutes(1)
                                         ),
                                             sqs_queue_DLQ.metric_approximate_number_of_messages_not_visible(
                                                 period=core.Duration.minutes(1)
                                             )]),
                          cw.GraphWidget(title="ERROR/WARNING Logs",
                                         left=[log_metric_ERROR],
                                         right=[log_metric_WARNING]),
                          cw.SingleValueWidget(title="Running/Waiting and Dead Jobs",
                                               metrics=[sqs_queue.metric_approximate_number_of_messages_not_visible(
                                                   period=core.Duration.minutes(1)
                                               ),
                                                   sqs_queue.metric_approximate_number_of_messages_visible(
                                                       period=core.Duration.minutes(1)
                                                   ),
                                                   sqs_queue_DLQ.metric_approximate_number_of_messages_not_visible(
                                                       period=core.Duration.minutes(1)
                                                   ),
                                                   sqs_queue_DLQ.metric_approximate_number_of_messages_visible(
                                                       period=core.Duration.minutes(1)
                                                   )],
                                               height=6)
                          )
        # Alarm for queue - DLQ
        alarm_DLQ = cw.Alarm(self, "SQS_DLQ",
                             metric=sqs_queue_DLQ.metric_approximate_number_of_messages_visible(),
                             threshold=0,
                             comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
                             evaluation_periods=1,
                             datapoints_to_alarm=1)
        alarm_topic = sns.Topic(self, "SQS queue-DLQ has dead letter")
        alarm_topic.add_subscription(subscription=sub.EmailSubscription(alarm_email))
        alarm_DLQ.add_alarm_action(action.SnsAction(alarm_topic))

        core.CfnOutput(self, "Dashboard", value="CloudWatch Dashboard name s3_migrate_serverless")
        
        # Alarm for queue empty, i.e. no visible message and no in-visible message
        # metric_all_message = cw.MathExpression(
        #     expression="a + b",
        #     label="empty_queue_expression",
        #     using_metrics={
        #         "a": sqs_queue.metric_approximate_number_of_messages_visible(),
        #         "b": sqs_queue.metric_approximate_number_of_messages_not_visible()
        #     }
        # )
        # alarm_0 = cw.Alarm(self, "SQSempty",
        #                    alarm_name="SQS queue empty-Serverless",
        #                    metric=metric_all_message,
        #                    threshold=0,
        #                    comparison_operator=cw.ComparisonOperator.LESS_THAN_OR_EQUAL_TO_THRESHOLD,
        #                    evaluation_periods=3,
        #                    datapoints_to_alarm=3,
        #                    treat_missing_data=cw.TreatMissingData.IGNORE
        #                    )
        # alarm_topic = sns.Topic(self, "SQS queue empty-Serverless")
        # alarm_topic.add_subscription(subscription=sub.EmailSubscription(alarm_email))
        # alarm_0.add_alarm_action(action.SnsAction(alarm_topic))

###############
app = core.App()
CdkResourceStack(app, "s3-migrate-serverless")
app.synth()
