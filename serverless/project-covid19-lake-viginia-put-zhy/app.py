#!/usr/bin/env python3
from aws_cdk import core
import aws_cdk.aws_s3 as s3
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

Des_bucket_default = 'covid19-lake'  # The bucket in China
Des_prefix_default = ''
Src_bucket_default = 'covid19-lake'  # The bucket in US
Src_prefix_default = ''
StorageClass = 'STANDARD'

# The China region credential setting in SSM Parameter Store
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

        ssm_credential_para = ssm.StringParameter.from_secure_string_parameter_attributes(
            self, "ssm_parameter_credentials",
            parameter_name=ssm_parameter_credentials,
            version=1
        )

        ssm_parameter_ignore_list = ssm.StringParameter(self, "covid19_lake_s3_migrate_ignore_list",
                                                        string_value=ignore_list)

        ddb_file_list = ddb.Table(self, "covid19_s3_migrate_ddb",
                                  partition_key=ddb.Attribute(name="Key", type=ddb.AttributeType.STRING),
                                  billing_mode=ddb.BillingMode.PAY_PER_REQUEST)

        sqs_queue_DLQ = sqs.Queue(self, "covid19_s3_migrate_DLQ",
                                  visibility_timeout=core.Duration.minutes(15),
                                  retention_period=core.Duration.days(14)
                                  )
        sqs_queue = sqs.Queue(self, "covid19_s3_migrate_queue",
                              visibility_timeout=core.Duration.minutes(15),
                              retention_period=core.Duration.days(14),
                              dead_letter_queue=sqs.DeadLetterQueue(
                                  max_receive_count=3,
                                  queue=sqs_queue_DLQ
                              )
                              )

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

        handler = lam.Function(self, "covid19-s3-migrate-worker",
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

        handler_jobsender = lam.Function(self, "covid19-s3-migrate-jobsender",
                                         code=lam.Code.asset("./lambda"),
                                         handler="lambda_function_jobsender.lambda_handler",
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
                                             'sqs_queue': sqs_queue.queue_name,
                                             'Src_bucket_default': Src_bucket_default,
                                             'Src_prefix_default': Src_prefix_default,
                                             'ssm_parameter_credentials': ssm_parameter_credentials,
                                             'ssm_parameter_ignore_list': ssm_parameter_ignore_list.parameter_name
                                         })

        ddb_file_list.grant_read_write_data(handler)
        ddb_file_list.grant_read_write_data(handler_jobsender)
        sqs_queue.grant_send_messages(handler_jobsender)
        handler.add_event_source(SqsEventSource(sqs_queue, batch_size=1))

        s3bucket = s3.Bucket.from_bucket_name(self, 'covid19-lake', bucket_name='covid19-lake')
        s3bucket.grant_read(handler)
        s3bucket.grant_read(handler_jobsender)

        ssm_credential_para.grant_read(handler)
        ssm_credential_para.grant_read(handler_jobsender)
        ssm_parameter_ignore_list.grant_read(handler_jobsender)

        core.CfnOutput(self, "DynamoDB_Table", value=ddb_file_list.table_name)
        core.CfnOutput(self, "SQS_Job_Queue", value=sqs_queue.queue_name)
        core.CfnOutput(self, "SQS_Job_Queue_DLQ", value=sqs_queue_DLQ.queue_name)

        # Schedule cron event to trigger Lambda Jobsender:
        event.Rule(self, 'cron_trigger_convid19_lake_jobsender',
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
        board = cw.Dashboard(self, "covid19_s3_migrate", dashboard_name="covid19_s3_migrate_serverless")

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
                             alarm_name="s3-migration-serverless-SQS Dead Letter Queue",
                             metric=sqs_queue_DLQ.metric_approximate_number_of_messages_visible(),
                             threshold=0,
                             comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
                             evaluation_periods=1,
                             datapoints_to_alarm=1)
        alarm_topic = sns.Topic(self, "SQS queue-DLQ has dead letter")
        alarm_topic.add_subscription(subscription=sub.EmailSubscription(alarm_email))
        alarm_DLQ.add_alarm_action(action.SnsAction(alarm_topic))

        core.CfnOutput(self, "Dashboard", value="CloudWatch Dashboard name s3_migrate_serverless")


###############
app = core.App()
CdkResourceStack(app, "covid19-s3-migrate-serverless")
# MyStack(app, "MyStack", env=core.Environment(region="REGION",account="ACCOUNT")
app.synth()
