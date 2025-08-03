import { App } from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { AwsExamAugustStack } from "../lib/aws-exam-august-stack";

test('Stack snapshot test', () => {
    const app = new App();
    const stack = new AwsExamAugustStack(app, 'TestStack');
    const template = Template.fromStack(stack);

    expect(template).toMatchSnapshot();
});
