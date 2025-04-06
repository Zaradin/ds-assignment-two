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
            // Following the diagram flow: S3 → SNS → SQS → DLQ
            if (body.Message) {
                try {
                    const snsMessage = JSON.parse(body.Message);

                    // Check if it's an S3 event notification
                    if (
                        snsMessage.Records &&
                        snsMessage.Records[0] &&
                        snsMessage.Records[0].eventSource === "aws:s3"
                    ) {
                        const s3Record = snsMessage.Records[0].s3;
                        const sourceBucket = s3Record.bucket.name;
                        const imageKey = decodeURIComponent(
                            s3Record.object.key.replace(/\+/g, " ")
                        );

                        console.log(
                            `Deleting invalid image: ${imageKey} from bucket: ${sourceBucket}`
                        );

                        // Delete the invalid file from S3
                        await s3.send(
                            new DeleteObjectCommand({
                                Bucket: sourceBucket,
                                Key: imageKey,
                            })
                        );

                        console.log(
                            `Successfully deleted invalid image: ${imageKey} from bucket: ${sourceBucket}`
                        );
                    } else {
                        console.log("Not an S3 event notification, skipping");
                    }
                } catch (error) {
                    console.error("Error parsing SNS message:", error);
                }
            } else {
                console.log("Not an SNS message, skipping");
            }
        } catch (error) {
            console.error("Error processing DLQ message:", error);
            // We don't throw here to prevent the message from being returned to the DLQ
            // Continue processing other messages even if one fails
        }
    }
};
