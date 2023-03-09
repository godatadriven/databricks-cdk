import {Construct} from "constructs";
import {CustomResource} from "aws-cdk-lib";

export interface TokenProperties {
    workspaceUrl: string
    tokenName: string
    comment?: string
    lifetimeSeconds?: number
}

export interface TokenProps extends TokenProperties {
    readonly serviceToken: string
}

export class Token extends CustomResource {
    constructor(scope: Construct, id: string, props: TokenProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "token",
                token_name: props.tokenName,
                workspace_url: props.workspaceUrl,
                comment: props.comment,
                lifetime_seconds: props.lifetimeSeconds,
            }
        });
    }
    public tokenSecretArn(): string {
        /**
      * Returns the secrets manager ARN. Can only be returned
      * when creating the token
      */
        return this.getAttString("token_secrets_arn");
    }
}
