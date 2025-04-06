import { SNSEvent, SNSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
    DynamoDBDocumentClient,
    UpdateCommand,
    GetCommand,
} from "@aws-sdk/lib-dynamodb";

const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);

// Valid metadata types
const VALID_METADATA_TYPES = ["Caption", "Date", "name"];

export const handler: SNSHandler = async (event: SNSEvent) => {
    console.log("Processing metadata event:", JSON.stringify(event));

    const tableName = process.env.IMAGE_TABLE_NAME;
    if (!tableName) {
        throw new Error("IMAGE_TABLE_NAME environment variable is not set");
    }

    for (const record of event.Records) {
        try {
            console.log("Processing SNS record:", record.Sns);

            // Extract message attributes and body
            const messageAttributes = record.Sns.MessageAttributes;
            const messageBody = JSON.parse(record.Sns.Message);

            // Validate message structure
            if (!messageBody.id || !messageBody.value) {
                console.error(
                    "Invalid message format. Required fields: id, value"
                );
                continue;
            }

            // Get the metadata type from message attributes
            const metadataTypeAttr = messageAttributes.metadata_type;
            if (!metadataTypeAttr || !metadataTypeAttr.Value) {
                console.error("Missing metadata_type attribute");
                continue;
            }

            // Get the metadata type and correctly handle the attribute name case
            const metadataType = metadataTypeAttr.Value;
            let attributeName;

            // Map each metadata type to the correct attribute name
            // This ensures consistent attribute naming
            switch (metadataType) {
                case "Caption":
                    attributeName = "caption";
                    break;
                case "Date":
                    attributeName = "date";
                    break;
                case "name":
                    attributeName = "name";
                    break;
                default:
                    console.error(`Invalid metadata type: ${metadataType}`);
                    continue;
            }

            // Validate metadata type
            if (!VALID_METADATA_TYPES.includes(metadataType)) {
                console.error(
                    `Invalid metadata type: ${metadataType}. Valid types: ${VALID_METADATA_TYPES.join(
                        ", "
                    )}`
                );
                continue;
            }

            // Check if the image exists in the table
            const getParams = {
                TableName: tableName,
                Key: {
                    id: messageBody.id,
                },
            };

            const getResult = await docClient.send(new GetCommand(getParams));

            if (!getResult.Item) {
                console.error(`Image not found in database: ${messageBody.id}`);
                continue;
            }

            // Using the proper type format for UpdateCommand
            const updateParams = {
                TableName: tableName,
                Key: {
                    id: messageBody.id,
                },
                UpdateExpression: `SET #attrName = :value`,
                ExpressionAttributeNames: {
                    "#attrName": attributeName,
                },
                ExpressionAttributeValues: {
                    ":value": messageBody.value,
                },
            };

            console.log(
                `Updating ${metadataType} (attribute: ${attributeName}) for image ${messageBody.id} to: ${messageBody.value}`
            );

            const updateResult = await docClient.send(
                new UpdateCommand(updateParams)
            );

            console.log(
                `Successfully updated metadata ${attributeName}. Result:`,
                updateResult
            );
        } catch (error) {
            console.error("Error processing metadata message:", error);
            // Continue processing other messages even if one fails
        }
    }
};
