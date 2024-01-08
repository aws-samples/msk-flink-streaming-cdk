
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
2. The Lambda function generates telemetry data and sends the data to Amazon Managed Streaming for Apache Kafka (Amazon MSK).
3. The data is processed by an Apache Flink Python application hosted on Amazon Managed Service for Apache Flink.
4. After processing, data with average temperature calculation is stored in Amazon S3 and data with anomaly results is sent to the output topic of the same MSK cluster.
5. The Lambda function monitors the output stream, and processes and sends data to the appropriate destination—for this use case, Amazon Simple Notification Service (Amazon SNS).

## To Run: 
1. Install the required dependencies:
```
pip install -r requirements.txt
```

2. Download Apache Flink Connector for Kafka SQL ![(Source)](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-kafka_2.11/1.11.2)
```
curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.11.2/flink-sql-connector-kafka_2.11-1.11.2.jar --output PythonKafkaSink/lib/flink-sql-connector-kafka_2.11-1.11.2.jar

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

