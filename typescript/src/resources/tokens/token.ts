import {Construct} from "constructs";
import {CustomResource} from "aws-cdk-lib";

export interface TokenProperties {
    workspaceUrl: string
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
                workspace_url: props.workspaceUrl,
                comment: props.comment,
                lifetime_seconds: props.lifetimeSeconds,
            }
        });
    }

    public tokenValue(): string {
        /**
         * Returns the secret value of the created token. Can only be returned
         * when creating the token
         */
        return this.getAttString("token_value");
    }
}
