import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { SNSClient, CreateTopicCommand, SubscribeCommand, PublishCommand } from "@aws-sdk/client-sns";
import { DynamoDBClient, PutItemCommand, DeleteItemCommand } from "@aws-sdk/client-dynamodb";
import { SchedulerClient, CreateScheduleCommand } from "@aws-sdk/client-scheduler";

const snsClient = new SNSClient({});
const ddbClient = new DynamoDBClient({});
const schedulerClient = new SchedulerClient({});

const TABLE_NAME = process.env.TABLE_NAME;
const EMAIL = 'yavorpetrakiev@gmail.com';
const REGION = 'eu-central-1';
const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN;

export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
    console.log(event);

    try {
        const body = event.body ? JSON.parse(event.body) : null;

        if (!body) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: 'No body' }),
            };
        }

        if (body.valid === true) {
            // This is where you'll trigger SNS to send email for valid JSON
            console.log("Valid JSON received:", body);

            // Publish message
            await snsClient.send(new PublishCommand({
                TopicArn: SNS_TOPIC_ARN,
                Message: `Valid JSON received: ${JSON.stringify(body)}`
            }));

            // (Temporary response for now)
            return {
                statusCode: 200,
                body: JSON.stringify({ message: 'Valid JSON - email will be sent', data: body }),
            };
        } else {
            // This is where you'll add the DynamoDB + EventBridge logic for invalid JSON
            console.log("Invalid JSON received:", body);

            // Store in DynamoDB
            const timestamp = Math.floor(Date.now() / 1000); // seconds
            await ddbClient.send(new PutItemCommand({
                TableName: TABLE_NAME,
                Item: {
                    PK: { S: "InvalidEvents" },
                    SK: { N: `${timestamp}` },
                    body: { S: JSON.stringify(body) },
                    ttl: { N: `${timestamp + 24 * 3600}` }, // 24-hour TTL
                }
            }));

            console.log("Invalid item stored in DynamoDB with TTL set.");

            // Create EventBridge Scheduler for 24h + 30min deletion
            const targetArn = `arn:aws:lambda:${REGION}:${process.env.AWS_ACCOUNT_ID}:function:deleteHandler`;
            
            await schedulerClient.send(new CreateScheduleCommand({
                Name: `delete-${timestamp}`,
                FlexibleTimeWindow: { Mode: "OFF" },
                ScheduleExpression: `rate(1470 minutes)`, // 24h + 30 min
                Target: {
                    Arn: targetArn,
                    RoleArn: process.env.SCHEDULER_ROLE_ARN,
                    Input: JSON.stringify({
                        PK: "InvalidEvents",
                        SK: `${timestamp}`,
                        email: EMAIL
                    }),
                },
            }));

            console.log("EventBridge schedule created for deletion at 24h30min mark.");

            // (Temporary response for now)
            return {
                statusCode: 200,
                body: JSON.stringify({ message: 'Invalid JSON - will be stored for cleanup', data: body }),
            };
        }
    } catch (err) {
        console.error("Error parsing request:", err);
        return {
            statusCode: 400,
            body: JSON.stringify({ message: `Invalid JSON format. Err: ${err}` }),
        };
    }
};
