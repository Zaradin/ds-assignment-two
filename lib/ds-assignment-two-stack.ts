import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3notify from "aws-cdk-lib/aws-s3-notifications";
import * as sns from "aws-cdk-lib/aws-sns";
import * as sns_subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as lambdaEventSources from "aws-cdk-lib/aws-lambda-event-sources";

export class DsAssignmentTwoStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        // S3 Bucket for uploading the images
        const imagesBucket = new s3.Bucket(this, "images", {
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            publicReadAccess: false,
        });

        const newImageTopic = new sns.Topic(this, "NewImageTopic", {
            displayName: "New Image topic",
        });

        // Configure S3 to send notifications to SNS when objects are created
        imagesBucket.addEventNotification(
            s3.EventType.OBJECT_CREATED,
            new s3notify.SnsDestination(newImageTopic)
        );

        // Dead Letter Queue for invalid image processing
        const dlq = new sqs.Queue(this, "InvalidImagesDLQ", {
            visibilityTimeout: cdk.Duration.seconds(300),

            retentionPeriod: cdk.Duration.days(14),
        });

        // SQS Queue for processing images
        const imageProcessQueue = new sqs.Queue(this, "ImageProcessQueue", {
            receiveMessageWaitTime: cdk.Duration.seconds(5),
            visibilityTimeout: cdk.Duration.seconds(30),
            deadLetterQueue: {
                queue: dlq,
                maxReceiveCount: 3, // After 3 failed attempts, send to DLQ
            },
        });

        // Subscribe the queue to the topic with a filter for image processing
        newImageTopic.addSubscription(
            new sns_subs.SqsSubscription(imageProcessQueue)
        );

        // DynamoDB table for storing image metadata
        const imageTable = new dynamodb.Table(this, "ImageTable", {
            partitionKey: { name: "id", type: dynamodb.AttributeType.STRING },
            billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
        });

        // Lambda functions
        const logImageFn = new lambdanode.NodejsFunction(
            this,
            "LogImageFunction",
            {
                runtime: lambda.Runtime.NODEJS_16_X,
                entry: `${__dirname}/../lambdas/logImage.ts`,
                timeout: cdk.Duration.seconds(15),
                memorySize: 128,
                environment: {
                    IMAGE_TABLE_NAME: imageTable.tableName,
                    BUCKET_NAME: imagesBucket.bucketName,
                },
            }
        );

        // Lambda function to remove invalid images
        const removeImageFn = new lambdanode.NodejsFunction(
            this,
            "RemoveImageFunction",
            {
                runtime: lambda.Runtime.NODEJS_16_X,
                entry: `${__dirname}/../lambdas/removeImage.ts`,
                timeout: cdk.Duration.seconds(15),
                memorySize: 128,
                environment: {
                    BUCKET_NAME: imagesBucket.bucketName,
                },
            }
        );

        // Add SQS as an event source for the Lambda
        logImageFn.addEventSource(
            new lambdaEventSources.SqsEventSource(imageProcessQueue, {
                batchSize: 5, // Process 5 messages at a time
            })
        );

        // Add DLQ as an event source for the Remove Image Lambda
        removeImageFn.addEventSource(
            new lambdaEventSources.SqsEventSource(dlq, {
                batchSize: 1, // Process 1 message at a time
            })
        );

        // Grant permissions to the Lambda function
        imagesBucket.grantRead(logImageFn);
        imageTable.grantWriteData(logImageFn);

        // Need write access to delete objects (dlq)
        imagesBucket.grantReadWrite(removeImageFn);

        // Output
        new cdk.CfnOutput(this, "BucketName", {
            value: imagesBucket.bucketName,
            description: "S3 bucket for storing images",
        });

        new cdk.CfnOutput(this, "TopicArn", {
            value: newImageTopic.topicArn,
            description: "SNS topic ARN for image notifications",
        });

        new cdk.CfnOutput(this, "TableName", {
            value: imageTable.tableName,
            description: "DynamoDB table for image metadata",
        });
    }
}
