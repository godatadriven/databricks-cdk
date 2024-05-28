import {Construct} from "constructs";
import {
    CustomResource,
    custom_resources as cr,
} from "aws-cdk-lib";
import { IFunction } from "aws-cdk-lib/aws-lambda";

export interface TokenProperties {
    workspaceUrl: string
    tokenName: string
    comment?: string
    lifetimeSeconds?: number
}

export interface TokenProps extends TokenProperties {
    readonly databricksDeployLambda: IFunction
}

export class Token extends Construct {
    readonly customResource: CustomResource;
    readonly tokenSecretArn: string;

    constructor(scope: Construct, id: string, props: TokenProps) {
        super(scope, id);

        const cr_provider = new cr.Provider(this, 'Provider',
        {
            onEventHandler: props.databricksDeployLambda,
        });

        this.customResource = new CustomResource(this, 'Resource', {
            serviceToken: cr_provider.serviceToken,
            properties: {
                action: "token",
                token_name: props.tokenName,
                workspace_url: props.workspaceUrl,
                comment: props.comment,
                lifetime_seconds: props.lifetimeSeconds,
            }
        });

        this.tokenSecretArn = this.customResource.getAttString("token_secrets_arn");
    }
}
