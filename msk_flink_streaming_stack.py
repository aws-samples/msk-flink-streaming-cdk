# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# Note: MSK cluster takes almost 40 mins to deploy

# To use the experimental L2 constructs for Managed Apache Flink and Managed Apache Kafka, install the following: 
# pip install aws-cdk.aws_kinesisanalytics_flink_alpha

from aws_cdk import (
    # Duration,
    NestedStack,
    Stack,
    RemovalPolicy,
    Fn,
    # aws_sqs as sqs,
    aws_ec2 as ec2,
    aws_msk as msk,
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_events_targets as targets,
    aws_events as events,
    Duration,
    aws_s3_assets as assets,
    aws_s3_deployment as s3deployment,
    aws_logs as logs,
    aws_sns as sns,
    aws_lambda_event_sources,
    BundlingOptions,
    custom_resources as cr,
    CfnOutput
)
from constructs import Construct
import aws_cdk.aws_kinesisanalytics_flink_alpha as flink # L2 Construct for Managed Apache Flink 
import aws_cdk.aws_msk_alpha as msk_alpha # L2 Construct for Managed Apache Kafka


class FlinkStack(NestedStack):
    def __init__(self, 
                scope: Construct, 
                construct_id: str, 
                vpc,
                security_group,
                bootstrap_brokers,
                cluster,
                **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        
        # Bucket where output of Apache Flink is stored 
        output_bucket = s3.Bucket(
            self,
            "flink-output-bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            # Removal policy unsuitable for production
            removal_policy = RemovalPolicy.DESTROY,
            auto_delete_objects= True
        )
        
        # Bucket where code for Apache Flink is stored 
        flink_code_bucket = s3.Bucket(
            self,
            "flink-code-bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            # Removal policy unsuitable for production
            removal_policy = RemovalPolicy.DESTROY,
            auto_delete_objects= True
        )

        flink_app_code_zip = s3deployment.BucketDeployment(self, "flink_app_code_zip",
            sources=[s3deployment.Source.asset("./PythonKafkaSink.zip")],
            destination_bucket=flink_code_bucket,
            extract=False
        )
        
        flink_app_role = iam.Role(self, "FlinkAppRole",
            assumed_by=iam.ServicePrincipal("kinesisanalytics.amazonaws.com"),
        )
        
        # Tighten per different consumers/producers & your security requirements
        # See: https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html
        flink_app_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "kafka-cluster:Connect",
                "kafka-cluster:DescribeCluster",
                "kafka-cluster:AlterCluster",
                "kafka-cluster:*Topic*",
                "kafka-cluster:WriteData",
                "kafka-cluster:ReadData",
                "kafka-cluster:DescribeGroup"
            ],
            resources=[
               "*" 
            ]
            )
        )
        
        # Apache Flink Application
        flink_app = flink.Application(self, "Flink-App",
            code=flink.ApplicationCode.from_asset("./PythonKafkaSink.zip"),
            runtime=flink.Runtime.FLINK_1_13,
            vpc=vpc,
            security_groups=[security_group],
            role=flink_app_role,
            property_groups=
            {
               "kinesis.analytics.flink.run.options" : {
                    "python" : "PythonKafkaSink/main.py", 
                    "jarfile" : "PythonKafkaSink/lib/aws-iam-sql-kafka-connector-1.jar"
                }
                    ,
               "producer.config.0" : {
                    "input.topic.name" : "kfp_sensor_topic",
                    "bootstrap.servers": bootstrap_brokers
                },
                "consumer.config.0": {
                    "output.topic.name": "kfp_sns_topic",
                    "output.s3.bucket": output_bucket.bucket_name
                }
            }
            
        )
        
        # Grant Apache Flink access to read and write to output bucket
        output_bucket.grant_read_write(flink_app)
        
        
        
        
class LambdaStack(NestedStack):
    def __init__(self, 
            scope: Construct, 
            construct_id: str,
            vpc,
            security_group,
            cluster,
            **kwargs):
        super().__init__(scope, construct_id, **kwargs)
    
        # IAM Role used by Lambda functions
        lambda_role = iam.Role(self, "lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                    # Tighten per your security requirements
                    iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaMSKExecutionRole"),
                    iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSNSFullAccess"),
                    iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                ],
        )
        
        # Tighten per different consumers/producers & your security requirements
        # See: https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "kafka-cluster:Connect",
                "kafka-cluster:DescribeCluster",
                "kafka-cluster:AlterCluster",
                "kafka-cluster:*Topic*",
                "kafka-cluster:WriteData",
                "kafka-cluster:ReadData",
                "kafka-cluster:DescribeGroup"
            ],
            resources=[
                "*" 
            ]
            )
        )
            
        # Producer Function
        lambdaFn = lambda_.Function(
            self, "kfpLambdaStreamProducer",
            code=lambda_.Code.from_asset(
                "./LambdaFunctions",
                bundling=BundlingOptions(
                image=lambda_.Runtime.PYTHON_3_8.bundling_image,
                command=[
                    "bash", "-c",
                    "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                ],
            ),),
            handler="kfpLambdaStreamProducer.lambda_handler",
            timeout=Duration.seconds(150),
            runtime=lambda_.Runtime.PYTHON_3_8,
            environment={'topicName':'kfp_sensor_topic',
                         'mskClusterArn':cluster.attr_arn}, #cluster_arn
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType('PRIVATE_WITH_EGRESS')),
            role=lambda_role,
            security_groups=[security_group], # WARNING: tighten up security group 
        )
        
        # SNS Alarm Topic
        alarm_sns_topic = sns.Topic(self, "alarm_sns_topic",
            display_name="Temperature Alarm Topic"
        )
        
        # SNS Function
        sns_lambdaFn = lambda_.Function(
            self, "sns_alarm_function",
            code=lambda_.Code.from_asset("./LambdaFunctions"),
            handler="kfpLambdaConsumerSNS.lambda_handler",
            timeout=Duration.seconds(300),
            runtime=lambda_.Runtime.PYTHON_3_8,
            environment={'SNSTopicArn':alarm_sns_topic.topic_arn},
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType('PRIVATE_WITH_EGRESS')),
            role=lambda_role,
            security_groups=[security_group],# WARNING: tighten up security group 
        )
        
        sns_lambdaFn.add_event_source(aws_lambda_event_sources.ManagedKafkaEventSource(
            cluster_arn=cluster.attr_arn, #cluster_arn
            topic='kfp_sns_topic',
            starting_position=lambda_.StartingPosition.TRIM_HORIZON
        ))

        # Run Producer Lambda function every 300 seconds
        # See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
        rule = events.Rule(
            self, "scheduledEvent",
            schedule=events.Schedule.rate(Duration.seconds(300)),
        )
        rule.add_target(targets.LambdaFunction(lambdaFn))
    
    
    


class MSKFlinkStreamingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = ec2.Vpc(self, 
            "MSK-VPC",
            #cidr=172.1.0.0/16,
            nat_gateways=1,
            )
            
            
        # Security Group that allows all traffic within itself 
        all_sg = ec2.SecurityGroup(self,
            "all_sg", 
            vpc=vpc,
            allow_all_outbound=True,
        )
            
        all_sg.add_ingress_rule(
          all_sg,
          ec2.Port.all_traffic(), # WARNING: Change to port just needed by MSK
          "allow all traffic in SG",
        )

        # Load cluster configurations from file
        # Currently still need to use the L1 MSK construct to load a config file
        config_file = open('./cluster_config', 'r')
        server_properties = config_file.read()
        cfn_configuration = msk.CfnConfiguration(self, "MyCfnConfig",
            name="MSKConfig",
            server_properties=server_properties
        )

        
        # MSK Cluster L1 Construct
        msk_cluster = msk.CfnCluster(self, "msk-cluster",
            cluster_name="msk-cluster",
            number_of_broker_nodes=len(vpc.private_subnets),
            kafka_version='3.4.0',
            broker_node_group_info=msk.CfnCluster.BrokerNodeGroupInfoProperty(
                instance_type="kafka.m5.large",
                storage_info=msk.CfnCluster.StorageInfoProperty(
                    ebs_storage_info=msk.CfnCluster.EBSStorageInfoProperty(
                        volume_size=50
                    )
                ),
                client_subnets=[
                    subnet.subnet_id
                    for subnet
                    in vpc.private_subnets],
                security_groups=[all_sg.security_group_id], 
            ),
            encryption_info = msk.CfnCluster.EncryptionInfoProperty(
                encryption_in_transit=msk.CfnCluster.EncryptionInTransitProperty(
                    client_broker="TLS",
                )
            ),
            configuration_info=msk.CfnCluster.ConfigurationInfoProperty(
                arn=cfn_configuration.attr_arn,
                revision=1
            ),
            client_authentication=msk.CfnCluster.ClientAuthenticationProperty(
                sasl=msk.CfnCluster.SaslProperty(
                    iam=msk.CfnCluster.IamProperty(
                        enabled=True
                    ),
                ),
            ),
        )
        
        # IAM Role used by AWS Custom Resources
        cr_iam_role = iam.Role(self, "cr-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        
        cr_iam_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "kafka:DescribeCluster",
                "kafka:UpdateConnectivity",
                "kafka:GetBootstrapBrokers"
            ],
            resources=[
                msk_cluster.attr_arn
            ]
            )
        )
        
        # Get Kafka Cluster current version
        cluster_info=cr.AwsCustomResource(self, "DescribeKafkaCluster",
            on_create=cr.AwsSdkCall(
                service="Kafka",
                action="DescribeCluster",
                parameters={
                    "ClusterArn": msk_cluster.attr_arn, #cluster_arn
                },
                physical_resource_id=cr.PhysicalResourceId.of("DescribeKafkaCluster")
                ),
            role=cr_iam_role
        )
        
        cluster_info.node.add_dependency(cr_iam_role)
        current_cluster_version = cluster_info.get_response_field("ClusterInfo.CurrentVersion")
        
        # Enable MSK multi-vpc private connectitvity
        enable_msk_multivpc_resource = cr.AwsCustomResource(self, "EnableKafkaMultiVPC",
            function_name="EnableMSKMultiVPC",
            on_update=cr.AwsSdkCall(
                service="Kafka",
                action="UpdateConnectivity",
                parameters={
                    "ClusterArn": msk_cluster.attr_arn,#cluster_arn,
                    "CurrentVersion": current_cluster_version,
                    "ConnectivityInfo": {
                        "VpcConnectivity": {
                            "ClientAuthentication": {
                                "Sasl": {
                                    "Iam": {
                                        "Enabled": True
                                    }
                                }
                            }
                        }
                    }
                },
                physical_resource_id=cr.PhysicalResourceId.of("ActivateKafkaMultiVPC")
            ),
            role=cr_iam_role,
        )
        
        enable_msk_multivpc_resource.node.add_dependency(cr_iam_role)
        enable_msk_multivpc_resource.node.add_dependency(msk_cluster)

        
        # Get MSK Bootstrap Broker Connection Strings
        # TODO: Parameterise so it changes with Auth method
        msk_iam_bootstrap_brokers = cr.AwsCustomResource(self, 'getBootstrapBrokers',
            on_update=cr.AwsSdkCall(
                service='Kafka',
                action='getBootstrapBrokers',
                physical_resource_id=cr.PhysicalResourceId.of('getMSKBootstrapBrokers'),
                parameters = {
                    "ClusterArn": msk_cluster.attr_arn
                }
            ) ,
            role=cr_iam_role
        ).get_response_field('BootstrapBrokerStringSaslIam') # need to change this to what connection type you want i.e. IAM, TLS etc.
        
        msk_iam_bootstrap_brokers.node.add_dependency(cr_iam_role)
        
        # Lambda Producer & Consumer Stack
        lambdaStack = LambdaStack(self, "LambdaStack",
            vpc=vpc,
            security_group=all_sg,
            cluster=msk_cluster
        )
        
        # Flink Consumer Stack
        flinkStack = FlinkStack(self, "FlinkStack",
            vpc=vpc,
            security_group=all_sg,
            bootstrap_brokers=msk_iam_bootstrap_brokers,
            
            cluster=msk_cluster
        )

        


    

        
        
