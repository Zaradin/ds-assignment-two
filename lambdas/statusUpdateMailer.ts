import { DynamoDBStreamEvent, DynamoDBStreamHandler } from "aws-lambda";
import {
    SESClient,
    SendEmailCommand,
    SendEmailCommandInput,
} from "@aws-sdk/client-ses";
import { SES_REGION, SES_EMAIL_FROM, SES_EMAIL_TO } from "../env";

const sesClient = new SESClient({ region: SES_REGION });

export const handler: DynamoDBStreamHandler = async (
    event: DynamoDBStreamEvent
) => {
    console.log("Processing DynamoDB Stream event:", JSON.stringify(event));

    if (!SES_EMAIL_FROM || !SES_EMAIL_TO || !SES_REGION) {
        throw new Error(
            "Please add the SES_EMAIL_TO, SES_EMAIL_FROM and SES_REGION environment variables"
        );
    }

    for (const record of event.Records) {
        try {
            // Only process MODIFY events (when status is updated)
            if (record.eventName !== "MODIFY") {
                console.log(`Skipping ${record.eventName} event`);
                continue;
            }

            if (!record.dynamodb?.NewImage || !record.dynamodb?.OldImage) {
                console.log("Missing new or old image data");
                continue;
            }

            const newImage = record.dynamodb.NewImage;
            const oldImage = record.dynamodb.OldImage;
            const newStatus = newImage.status?.S;
            const oldStatus = oldImage.status?.S;

            // Only send emails when status changes
            if (newStatus && newStatus !== oldStatus) {
                const imageId = newImage.id?.S || "Unknown Image";
                const reason = newImage.reason?.S || "No reason provided";
                const reviewDate =
                    newImage.reviewDate?.S || new Date().toISOString();

                const statusText =
                    newStatus === "Pass" ? "approved" : "rejected";
                const subject = `Photo Status Update: ${imageId}`;

                const emailParams = {
                    Destination: {
                        ToAddresses: [SES_EMAIL_TO],
                    },
                    Message: {
                        Body: {
                            Html: {
                                Charset: "UTF-8",
                                Data: getHtmlContent({
                                    imageId: imageId,
                                    status: newStatus,
                                    reason: reason,
                                    reviewDate: reviewDate,
                                }),
                            },
                        },
                        Subject: {
                            Charset: "UTF-8",
                            Data: subject,
                        },
                    },
                    Source: SES_EMAIL_FROM,
                };

                console.log(
                    `Sending status notification email to ${SES_EMAIL_TO}`
                );

                await sesClient.send(new SendEmailCommand(emailParams));

                console.log(
                    `Successfully sent status notification email to ${SES_EMAIL_TO}`
                );
            } else {
                console.log(
                    "Status hasn't changed or no status present, no notification needed"
                );
            }
        } catch (error) {
            console.error("Error processing DynamoDB Stream record:", error);
        }
    }
};

interface EmailContent {
    imageId: string;
    status: string;
    reason: string;
    reviewDate: string;
}

function getHtmlContent({ imageId, status, reason, reviewDate }: EmailContent) {
    const statusText = status === "Pass" ? "approved" : "rejected";
    const statusColor = status === "Pass" ? "#28a745" : "#dc3545";

    return `
    <html>
      <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #eee; border-radius: 5px;">
          <h2>Photo Status Update</h2>
          <p>Hello,</p>
          <p>A photo has been reviewed with the following update:</p>
          
          <div style="margin: 20px 0; padding: 15px; border-left: 4px solid ${statusColor}; background-color: #f9f9f9;">
            <p><strong>Image:</strong> ${imageId}</p>
            <p><strong>Status:</strong> <span style="color: ${statusColor}; font-weight: bold;">${statusText.toUpperCase()}</span></p>
            <p><strong>Reason:</strong> ${reason}</p>
            <p><strong>Review Date:</strong> ${reviewDate}</p>
          </div>
        </div>
      </body>
    </html>
    `;
}
