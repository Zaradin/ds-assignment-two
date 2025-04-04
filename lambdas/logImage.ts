import { SQSHandler } from "aws-lambda";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";

const s3 = new S3Client();
const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);

// Function to validate image file extension
const isValidImageType = (filename: string): boolean => {
    const validExtensions = [".jpeg", ".png"];
    const extension = filename
        .toLowerCase()
        .substring(filename.lastIndexOf("."));
    return validExtensions.includes(extension);
};

export const handler: SQSHandler = async (event) => {
    console.log("Event ", JSON.stringify(event));

    const tableName = process.env.IMAGE_TABLE_NAME;
    if (!tableName) {
        throw new Error("IMAGE_TABLE_NAME environment variable is not set");
    }

    for (const record of event.Records) {
        try {
            const recordBody = JSON.parse(record.body); // Parse SQS message
            let snsMessage;

            // Handle different message structures
            if (recordBody.Message) {
                // Message from SNS via SQS
                snsMessage = JSON.parse(recordBody.Message);
            } else {
                // Direct SQS message
                snsMessage = recordBody;
            }

            if (snsMessage.Records) {
                console.log("Processing S3 event records");

                for (const messageRecord of snsMessage.Records) {
                    if (
                        messageRecord.eventSource === "aws:s3" &&
                        messageRecord.eventName.startsWith("ObjectCreated")
                    ) {
                        const s3e = messageRecord.s3;
                        const srcBucket = s3e.bucket.name;
                        // Object key may have spaces or unicode non-ASCII characters.
                        const srcKey = decodeURIComponent(
                            s3e.object.key.replace(/\+/g, " ")
                        );

                        console.log(
                            `Processing new image: ${srcKey} from bucket: ${srcBucket}`
                        );

                        // Validate the image file type
                        if (!isValidImageType(srcKey)) {
                            console.error(`Invalid image type: ${srcKey}`);
                            throw new Error(`Invalid image type: ${srcKey}`);
                        }

                        try {
                            // Download the image from the S3 source bucket to verify it exists
                            await s3.send(
                                new GetObjectCommand({
                                    Bucket: srcBucket,
                                    Key: srcKey,
                                })
                            );

                            // Log the valid image to DynamoDB
                            await docClient.send(
                                new PutCommand({
                                    TableName: tableName,
                                    Item: {
                                        id: srcKey,
                                        uploadTime: new Date().toISOString(),
                                        bucket: srcBucket,
                                    },
                                })
                            );

                            console.log(
                                `Successfully logged image ${srcKey} to DynamoDB`
                            );
                        } catch (error) {
                            console.error(
                                `Error processing image ${srcKey}:`,
                                error
                            );
                            throw error;
                        }
                    }
                }
            } else {
                console.log("Not an S3 event notification, skipping");
            }
        } catch (error) {
            console.error("Error processing record:", error);
            // Throwing the error will cause the message to be requeued
            // After max retries, it will go to DLQ if configured
            throw error;
        }
    }
};
