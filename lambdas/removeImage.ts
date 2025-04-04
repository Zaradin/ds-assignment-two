import { SQSHandler } from "aws-lambda";
import { DeleteObjectCommand, S3Client } from "@aws-sdk/client-s3";

const s3 = new S3Client();

export const handler: SQSHandler = async (event) => {
    console.log("Processing DLQ event:", JSON.stringify(event));

    const bucketName = process.env.BUCKET_NAME;
    if (!bucketName) {
        throw new Error("BUCKET_NAME environment variable is not set");
    }

    for (const record of event.Records) {
        try {
            console.log("Processing DLQ message:", record.body);

            // Parse the SQS message
            const body = JSON.parse(record.body);

            // Extract image information from the message
            // The structure can vary depending on how the message reached the DLQ
            let imageKey: string | undefined;
            let sourceBucket: string | undefined;

            // Original message was from SNS via SQS
            if (body.Message) {
                try {
                    const snsMessage = JSON.parse(body.Message);

                    // If it's an S3 event notification
                    if (
                        snsMessage.Records &&
                        snsMessage.Records[0] &&
                        snsMessage.Records[0].eventSource === "aws:s3"
                    ) {
                        const s3Record = snsMessage.Records[0].s3;
                        sourceBucket = s3Record.bucket.name;
                        imageKey = decodeURIComponent(
                            s3Record.object.key.replace(/\+/g, " ")
                        );
                    }
                } catch (error) {
                    console.error("Error parsing SNS message:", error);
                }
            }

            // couldn't extract the image information, log and skip
            if (!imageKey || !sourceBucket) {
                console.error(
                    "Could not determine image key or bucket from message:",
                    body
                );
                continue;
            }

            // Use the extracted bucket or fall back to the environment variable
            const targetBucket = sourceBucket || bucketName;

            console.log(
                `Deleting invalid image: ${imageKey} from bucket: ${targetBucket}`
            );

            // Delete the invalid file from S3
            await s3.send(
                new DeleteObjectCommand({
                    Bucket: targetBucket,
                    Key: imageKey,
                })
            );

            console.log(
                `Successfully deleted invalid image: ${imageKey} from bucket: ${targetBucket}`
            );
        } catch (error) {
            console.error("Error processing DLQ message:", error);
            // We don't throw here to prevent the message from being returned to the DLQ
            // Continue processing other messages even if one fails
        }
    }
};
