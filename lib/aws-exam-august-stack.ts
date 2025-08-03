import * as cdk from 'aws-cdk-lib';
import { LambdaIntegration, RestApi } from 'aws-cdk-lib/aws-apigateway';
import { Runtime, StartingPosition } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as path from 'node:path';
import { Construct } from 'constructs';
import { AttributeType, BillingMode, StreamViewType, Table } from 'aws-cdk-lib/aws-dynamodb';
import { PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { EmailSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';

export class AwsExamAugustStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        const table = new Table(this, 'InvalidEventsTable', {
            partitionKey: {
                name: 'PK',
                type: AttributeType.STRING
            },
            sortKey: {
                name: 'SK',
                type: AttributeType.NUMBER
            },
            timeToLiveAttribute: 'ttl',
            billingMode: BillingMode.PAY_PER_REQUEST,
            stream: StreamViewType.NEW_IMAGE
        });

        const notificationTopic = new Topic(this, 'notificationTopic');

        notificationTopic.addSubscription(
            new EmailSubscription('yavorpetrakiev@gmail.com')
        );

        const schedulerRole = new Role(this, 'SchedulerRole', {
            assumedBy: new ServicePrincipal('scheduler.amazonaws.com'),
        });

        const validateJSONLambdaFunction = new NodejsFunction(this, 'validateJSONLambda', {
            handler: 'handler',
            runtime: Runtime.NODEJS_22_X,
            entry: path.join(__dirname, '../src/validateJSONHandler.ts'),
            environment: {
                TABLE_NAME: table.tableName,
                AWS_ACCOUNT_ID: cdk.Stack.of(this).account,
                SCHEDULER_ROLE_ARN: schedulerRole.roleArn,
                SNS_TOPIC_ARN: notificationTopic.topicArn
            }
        });

        validateJSONLambdaFunction.addToRolePolicy(new PolicyStatement({
            actions: [
                'scheduler:CreateSchedule',
            ],
            resources: ['*']
        }));

        validateJSONLambdaFunction.addToRolePolicy(new PolicyStatement({
            actions: ['iam:PassRole'],
            resources: [schedulerRole.roleArn]
        }));

        table.grantReadWriteData(validateJSONLambdaFunction);

        const deleteHandlerLambdaFunction = new NodejsFunction(this, 'deleteHandler', {
            handler: 'handler',
            runtime: Runtime.NODEJS_22_X,
            entry: path.join(__dirname, '../src/deleteHandler.ts'),
            environment: {
                TABLE_NAME: table.tableName,
                SNS_TOPIC_ARN: notificationTopic.topicArn
            }
        });

        table.grantReadWriteData(deleteHandlerLambdaFunction);
        notificationTopic.grantPublish(deleteHandlerLambdaFunction);

        schedulerRole.addToPolicy(new PolicyStatement({
            actions: ['lambda:InvokeFunction'],
            resources: [deleteHandlerLambdaFunction.functionArn],
        }));

        deleteHandlerLambdaFunction.addEventSource(new DynamoEventSource(table, {
            startingPosition: StartingPosition.LATEST,
            filters: [
                {
                    pattern: JSON.stringify({
                        eventName: ["REMOVE"]
                    })
                }
            ]
        }));

        table.grantReadWriteData(deleteHandlerLambdaFunction);
        notificationTopic.grantPublish(validateJSONLambdaFunction);

        const api = new RestApi(this, 'ValidateJSONApi');
        const resource = api.root.addResource('validate');
        resource.addMethod('POST', new LambdaIntegration(validateJSONLambdaFunction, {
            proxy: true,
        }))
    }
}
