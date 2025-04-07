import { SNSEvent, SNSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
    DynamoDBDocumentClient,
    UpdateCommand,
    GetCommand,
} from "@aws-sdk/lib-dynamodb";

const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);

// Valid status values
const VALID_STATUSES = ["Pass", "Reject"];

export const handler: SNSHandler = async (event: SNSEvent) => {
    console.log("Processing status update event:", JSON.stringify(event));

    const tableName = process.env.IMAGE_TABLE_NAME;
    if (!tableName) {
        throw new Error("IMAGE_TABLE_NAME environment variable is not set");
    }

    for (const record of event.Records) {
        try {
            console.log("Processing SNS record:", record.Sns);

            // Parse the message body
            const messageBody = JSON.parse(record.Sns.Message);

            // Validate message structure
            if (!messageBody.id || !messageBody.date || !messageBody.update) {
                console.error(
                    "Invalid message format. Required fields: id, date, update"
                );
                continue;
            }

            const imageId = messageBody.id;
            const reviewDate = messageBody.date;
            const update = messageBody.update;

            // Further validate update structure
            if (!update.status || !update.reason) {
                console.error(
                    "Invalid update format. Required fields: status, reason"
                );
                continue;
            }

            // Validate status value
            if (!VALID_STATUSES.includes(update.status)) {
                console.error(
                    `Invalid status: ${
                        update.status
                    }. Valid statuses: ${VALID_STATUSES.join(", ")}`
                );
                continue;
            }

            // Check if the image exists in the table
            const getParams = {
                TableName: tableName,
                Key: {
                    id: imageId,
                },
            };

            const getResult = await docClient.send(new GetCommand(getParams));

            if (!getResult.Item) {
                console.error(`Image not found in database: ${imageId}`);
                continue;
            }

            // Update the image status in the database
            const updateParams = {
                TableName: tableName,
                Key: {
                    id: imageId,
                },
                UpdateExpression:
                    "SET #status = :status, #reason = :reason, #reviewDate = :reviewDate",
                ExpressionAttributeNames: {
                    "#status": "status",
                    "#reason": "reason",
                    "#reviewDate": "reviewDate",
                },
                ExpressionAttributeValues: {
                    ":status": update.status,
                    ":reason": update.reason,
                    ":reviewDate": reviewDate,
                },
            };

            console.log(
                `Updating status for image ${imageId} to: ${update.status} with reason: ${update.reason}`
            );

            const updateResult = await docClient.send(
                new UpdateCommand(updateParams)
            );

            console.log(`Successfully updated status for image ${imageId}`);
        } catch (error) {
            console.error("Error processing status update message:", error);
            // Continue processing other messages even if one fails
        }
    }
};
