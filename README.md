
# Real-Time Streaming with Amazon Managed Streaming for Apache Kafka (Amazon MSK) and Amazon Managed Service for Apache Flink 

<!--BEGIN STABILITY BANNER-->
---

![Stability: Developer Preview](https://img.shields.io/badge/stability-Developer--Preview-important.svg?style=for-the-badge)

> **This is an experimental example. It may not build out of the box**
>
> This example is built on Construct Libraries marked "Developer Preview" and may not be updated for latest breaking changes.
>
> It may additionally requires infrastructure prerequisites that must be created before successful build.
>
> If build is unsuccessful, please create an [issue](https://github.com/aws-samples/aws-cdk-examples/issues/new) so that we may debug the problem 
---
<!--END STABILITY BANNER-->

## Overview
Based on the following AWS Blog: https://aws.amazon.com/blogs/big-data/build-a-real-time-streaming-application-using-apache-flink-python-api-with-amazon-kinesis-data-analytics/

![Architecture Diagram](https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2021/03/25/bdb1289-pyflink-kda-1-1.jpg
 "Resources created with CDK")

This repository provides two examples of running a Python-based Apache Flink application using Amazon Managed Service for Apache Flink with stateful processing. We use custom code to generate random telemetry data that includes sensor ID, temperature, and event time.
The first use case demonstrates sending a notification when the count of high temperature readings of a sensor exceeds a defined threshold within a window (for this post, 30 seconds).
The second use case calculates the average temperature of the sensors within a fixed window (30 seconds), and persists the results in Amazon Simple Storage Service (Amazon S3) partitioned by event time for efficient query processing.

The workflow includes the following steps:
1. An Amazon CloudWatch event triggers an AWS Lambda function every minute.
2. The Lambda function generates telemetry data and sends the data to Amazon Managed Streaming for Apache Kafka (Amazon MSK). With [IAM Access Control](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html). 
3. The data is processed by an Apache Flink Python application hosted on Amazon Managed Service for Apache Flink. With [IAM Access Control](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html). 
4. After processing, data with average temperature calculation is stored in Amazon S3 and data with anomaly results is sent to the output topic of the same MSK cluster.
5. The Lambda function monitors the output stream, and processes and sends data to the appropriate destination—for this use case, Amazon Simple Notification Service (Amazon SNS).

## Additions:
- IAM Access Control for authentication and authorization to the MSK cluster from Lambda and Managed Flink. 
- Enabled for Mutual TLS authentication
- Updated to Flink 1.13.6
- Enables MSK [Multi-VPC connectitvity](https://aws.amazon.com/blogs/big-data/connect-kafka-client-applications-securely-to-your-amazon-msk-cluster-from-different-vpcs-and-aws-accounts/)


## Prerequisites
- Maven 
- (optional) An AWS Private Certificate Authority 

## To Run: 
1. Install the required dependencies:
```
pip install -r requirements.txt
```

2. Build the jar file for the Flink SQL Connector with IAM Auth
```
cd JarPackaging
mvn clean package
cd ..
cp JarPackaging/target/aws-iam-sql-kafka-connector-1.jar PythonKafkaSink/lib
zip -r PythonKafkaSink.zip PythonKafkaSink/
```

3. Bootstrap AWS Account for CDK:
```
cdk bootstrap
```

4. Deploy the stack (30-40 mins):

```
cdk deploy
```

or if using TLS with a Private CA:
```
cdk deploy --parameters privateCaArn=arn:aws:acm-pca:<AWS_REGION>:<AWS_ACCOUNT_ID>:certificate-authority/XXXXXX-XXXX-XXXX-XXXXXX-XXXXXXXX
```


## Authentication and authorization
### IAM Access Control
Follow [instructions here](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html#configure-clients-for-iam-access-control)

### Mutual TLS Authentication
Follow [instructions here](https://docs.aws.amazon.com/msk/latest/developerguide/msk-authentication.html#msk-authentication-client).

For step 4 On Amazon Linux 2, truststore can be copied as: 
```
cp /usr/lib/jvm/java-11-amazon-corretto.x86_64/lib/security/cacerts kafka.client.truststore.jks
```