import { DynamoDBClient, DeleteItemCommand, GetItemCommand } from "@aws-sdk/client-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

const ddbClient = new DynamoDBClient({});
const snsClient = new SNSClient({});
const TABLE_NAME = process.env.TABLE_NAME;
const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN;

export const handler = async (event: any) => {
    console.log(event);

    if (event.Records) {
        for (const record of event.Records) {
            if (record.eventName === "REMOVE") {
                const pk = record.dynamodb.Keys.PK.S;
                const sk = record.dynamodb.Keys.SK.N;

                const createdAt = parseInt(sk, 10);
                const now = Math.floor(Date.now() / 1000);
                const retentionSeconds = now - createdAt;

                const message = `Item with PK=${pk}, SK=${sk} was removed (TTL or manual) after ${retentionSeconds} seconds.`;

                await snsClient.send(new PublishCommand({
                    TopicArn: SNS_TOPIC_ARN,
                    Message: message,
                }));

            }
        }
        return;
    }

    if (event.PK && event.SK) {
        const { PK, SK } = event;

        const item = await ddbClient.send(new GetItemCommand({
            TableName: TABLE_NAME,
            Key: {
                PK: { S: PK },
                SK: { N: SK }
            }
        }));

        if (!item.Item) {
            console.log(`Item not found (already deleted)`);
        } else {
            await ddbClient.send(new DeleteItemCommand({
                TableName: TABLE_NAME,
                Key: {
                    PK: { S: PK },
                    SK: { N: SK }
                }
            }));

            console.log(`Item deleted by EventBridge Scheduler`);

            const createdAt = parseInt(SK, 10);
            const now = Math.floor(Date.now() / 1000);
            const retentionSeconds = now - createdAt;

            const message = `Item with PK=${PK} SK=${SK} was deleted by scheduler after ${retentionSeconds} seconds.`;

            await snsClient.send(new PublishCommand({
                TopicArn: SNS_TOPIC_ARN,
                Message: message,
            }));
        }
    }
};
