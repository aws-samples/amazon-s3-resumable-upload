from aws_cdk import core
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_dynamodb as ddb
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_ssm as ssm
import json
import aws_cdk.aws_s3_notifications as s3n

table_queue_name = 's3_migration_file_list'
ssm_parameter_bucket = 's3_migration_bucket_para'
ssm_parameter_credentials = 's3_migration_credentials'
ssm_parameter_credentials_version = 1

class CdkResourceStack(core.Stack):

    def __init__(self, scope: core.Construct, _id: str, bucket_para, **kwargs) -> None:
        super().__init__(scope, _id, **kwargs)

        self.ddb_file_list = ddb.Table(self, "ddb",
                                       table_name=table_queue_name,
                                       partition_key=ddb.Attribute(name="Key", type=ddb.AttributeType.STRING),
                                       billing_mode=ddb.BillingMode.PAY_PER_REQUEST)

        self.sqs_queue_DLQ = sqs.Queue(self, "sqs_DLQ",
                                       queue_name=table_queue_name + "-DLQ",
                                       visibility_timeout=core.Duration.hours(1),
                                       retention_period=core.Duration.days(14)
                                       )
        self.sqs_queue = sqs.Queue(self, "sqs_queue",
                                   queue_name=table_queue_name,
                                   visibility_timeout=core.Duration.hours(1),
                                   retention_period=core.Duration.days(14),
                                   dead_letter_queue=sqs.DeadLetterQueue(
                                       max_receive_count=24,
                                       queue=self.sqs_queue_DLQ
                                   )
                                   )
        self.ssm_bucket_para = ssm.StringParameter(self, "para-bucket",
                                                   string_value=json.dumps(bucket_para),
                                                   parameter_name=ssm_parameter_bucket
                                                   )

        # You need to manually setup ssm_credential_para in SSM Parameter Store before deploy CDK
        # Here import ssm_credential_para, MIND THE VERSION NUMBER MUST BE EXACT THE SAME !!!
        # 你需要先手工配置了一个ssm_credential_para，然后在这里导入，注意版本号一致!!!
        self.ssm_credential_para = ssm.StringParameter.from_secure_string_parameter_attributes(
            self, "ssm_parameter_credentials",
            parameter_name=ssm_parameter_credentials,
            version=ssm_parameter_credentials_version
        )

        # New a S3 bucket, new object in this bucket will trigger SQS jobs
        # This is not for existing S3 bucket. Jobsender will scan the existing bucket and create sqs jobs.
        # 这里新建一个S3 bucket，里面新建Object就会触发SQS启动搬迁工作。
        # 对于现有的S3 bucket，不在这里配置，由jobsender进行扫描并生成SQS Job任务。
        self.s3bucket = s3.Bucket(self, "newbucket")
        self.s3bucket.add_event_notification(s3.EventType.OBJECT_CREATED,
                                             s3n.SqsDestination(self.sqs_queue))
