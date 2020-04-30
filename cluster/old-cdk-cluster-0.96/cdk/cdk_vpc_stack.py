from aws_cdk import core
import aws_cdk.aws_ec2 as ec2


class CdkVpcStack(core.Stack):

    def __init__(self, scope: core.Construct, _id: str, **kwargs) -> None:
        super().__init__(scope, _id, **kwargs)

        self.vpc = ec2.Vpc(self, "VPC",
                           max_azs=2,
                           cidr="10.10.0.0/16",
                           subnet_configuration=[ec2.SubnetConfiguration(
                               subnet_type=ec2.SubnetType.PUBLIC,
                               name="Public",
                               cidr_mask=24
                           )
                           ]
                           )

        self.vpc.add_s3_endpoint("s3ep",
                                 [ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC)])
        # self.vpc.add_dynamo_db_endpoint("ddbep",
        #                                 [ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC)])
        # self.vpc.add_interface_endpoint("sqsep",
        #                                 ec2.InterfaceVpcEndpointAwsService.SQS)
