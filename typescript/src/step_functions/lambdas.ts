import {aws_iam, aws_lambda, aws_logs, Duration} from "aws-cdk-lib";
import {Construct} from "constructs";

export interface StepFunctionsLambdasProps {
    readonly accountId: string
    readonly region: string
    readonly databricksUserParam?: string
    readonly databricksPassParam?: string
    readonly databricksAccountParam?: string
    readonly lambdaCode: aws_lambda.DockerImageCode
}

interface LambdaProps {
    functionName: string
    lambdaMethod: string
}


export class StepFunctionsLambdas extends Construct {

    readonly props: StepFunctionsLambdasProps;
    readonly submitJobLambda: aws_lambda.IFunction;
    readonly jobStatusLambda: aws_lambda.IFunction;
    readonly lambdaRole: aws_iam.IRole;

    constructor(scope: Construct, id: string, props: StepFunctionsLambdasProps) {
        super(scope, id);
        this.props = props;

        this.lambdaRole = new aws_iam.Role(this, "Role", {
            assumedBy: new aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managedPolicies: [
                aws_iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
            ]
        });
        this.lambdaRole.addToPrincipalPolicy(new aws_iam.PolicyStatement({
            effect: aws_iam.Effect.ALLOW,
            actions: ["ssm:GetParameter"],
            resources: [
                `arn:aws:ssm:${this.props.region}:${this.props.accountId}:parameter/databricks/*`,
            ]
        }));

        this.submitJobLambda = this.generateLambda({
            functionName: "DatabricksSubmitJob",
            lambdaMethod: "submit-job"
        });
        this.jobStatusLambda = this.generateLambda({
            functionName: "DatabricksJobStatus",
            lambdaMethod: "job-status"
        });
    }

    private generateLambda(lambdaProps: LambdaProps) {
        return new aws_lambda.DockerImageFunction(this, "Lambda", {
            functionName: lambdaProps.functionName,
            code: this.props.lambdaCode,
            timeout: Duration.seconds(15),
            role: this.lambdaRole,
            environment: {
                LAMBDA_METHOD: lambdaProps.lambdaMethod,
                USER_PARAM: this.props.databricksUserParam || "/databricks/deploy/user",
                PASS_PARAM: this.props.databricksPassParam || "/databricks/deploy/password",
                ACCOUNT_PARAM: this.props.databricksAccountParam || "/databricks/account-id"
            },
            logRetention: aws_logs.RetentionDays.THREE_MONTHS,
        });
    }
}
