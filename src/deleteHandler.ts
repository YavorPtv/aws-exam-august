import { DynamoDBClient, DeleteItemCommand, GetItemCommand } from "@aws-sdk/client-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

const ddbClient = new DynamoDBClient({});
const snsClient = new SNSClient({});
const TABLE_NAME = process.env.TABLE_NAME;
const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN; // Set in CDK

export const handler = async (event: any) => {
    console.log("deleteHandler triggered with event:", JSON.stringify(event));

    // Case 1: DynamoDB Streams (TTL or manual deletion)
    if (event.Records) {
        for (const record of event.Records) {
            if (record.eventName === "REMOVE") {
                const pk = record.dynamodb.Keys.PK.S;
                const sk = record.dynamodb.Keys.SK.N;

                const createdAt = parseInt(sk, 10);
                const now = Math.floor(Date.now() / 1000);
                const retentionSeconds = now - createdAt;

                const message = `Item with PK=${pk}, SK=${sk} was removed (TTL or manual) after ${retentionSeconds} seconds.`;

                // Send notification
                await snsClient.send(new PublishCommand({
                    TopicArn: SNS_TOPIC_ARN,
                    Message: message,
                }));

                console.log("TTL/Remove event processed, message sent:", message);
            }
        }
        return;
    }

    // Case 2: EventBridge Scheduler (24h + 30 min mark)
    if (event.PK && event.SK) {
        const { PK, SK } = event;

        // Check if item exists
        const item = await ddbClient.send(new GetItemCommand({
            TableName: TABLE_NAME,
            Key: {
                PK: { S: PK },
                SK: { N: SK }
            }
        }));

        if (!item.Item) {
            console.log(`Item with PK=${PK}, SK=${SK} not found (already deleted)`);
        } else {
            // Delete the item
            await ddbClient.send(new DeleteItemCommand({
                TableName: TABLE_NAME,
                Key: {
                    PK: { S: PK },
                    SK: { N: SK }
                }
            }));

            console.log(`Item with PK=${PK}, SK=${SK} deleted by EventBridge Scheduler`);

            // Calculate retention time
            const createdAt = parseInt(SK, 10);
            const now = Math.floor(Date.now() / 1000);
            const retentionSeconds = now - createdAt;

            const message = `Item with PK=${PK}, SK=${SK} was deleted by Scheduler after ${retentionSeconds} seconds.`;

            // Send notification
            await snsClient.send(new PublishCommand({
                TopicArn: SNS_TOPIC_ARN,
                Message: message,
            }));

            console.log("Scheduler event processed, message sent:", message);
        }
    }
};
