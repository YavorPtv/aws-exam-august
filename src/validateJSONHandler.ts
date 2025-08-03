import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { SNSClient, CreateTopicCommand, SubscribeCommand, PublishCommand } from "@aws-sdk/client-sns";
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
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

            await snsClient.send(new PublishCommand({
                TopicArn: SNS_TOPIC_ARN,
                Message: `Valid JSON received: ${JSON.stringify(body)}`
            }));

            return {
                statusCode: 200,
                body: JSON.stringify({ message: 'Valid JSON - email will be sent', data: body }),
            };
        } else {

            const timestamp = Math.floor(Date.now() / 1000);
            await ddbClient.send(new PutItemCommand({
                TableName: TABLE_NAME,
                Item: {
                    PK: { S: "InvalidEvents" },
                    SK: { N: `${timestamp}` },
                    body: { S: JSON.stringify(body) },
                    ttl: { N: `${timestamp + 24 * 3600}` }, // 24h 
                }
            }));

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
