from aws_cdk import core
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_dynamodb as ddb
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_ssm as ssm
import json

table_queue_name = 's3_migrate_file_list'
ssm_parameter_bucket = 's3_migrate_bucket_para'
ssm_parameter_credentials = 's3_migrate_credentials'


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
                                       max_receive_count=3,
                                       queue=self.sqs_queue_DLQ
                                   )
                                   )
        self.ssm_bucket_para = ssm.StringParameter(self, "para-bucket",
                                                   string_value=json.dumps(bucket_para),
                                                   parameter_name=ssm_parameter_bucket
                                                   )
        self.ssm_credential_para = ssm.StringParameter.from_secure_string_parameter_attributes(
            self, "para-credential",
            parameter_name=ssm_parameter_credentials,
            version=2
        )