import {Construct} from "constructs";
import {CustomResource} from "aws-cdk-lib";


export interface SecretProperties {
    workspaceUrl: string
    scope: string
    key: string
    stringValue: string
}

export interface SecretProps extends SecretProperties {
    readonly serviceToken: string
}

export class Secret extends CustomResource {
    constructor(scope: Construct, id: string, props: SecretProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "secret",
                workspace_url: props.workspaceUrl,
                scope: props.scope,
                key: props.key,
                string_value: props.stringValue
            }
        });
    }
}
