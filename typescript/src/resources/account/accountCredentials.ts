import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


export interface AccountCredentialsProperties {
    readonly credentialsName: string
    readonly roleArn: string
}

export interface AccountCredentialsProps extends AccountCredentialsProperties {
    readonly serviceToken: string
}

export class AccountCredentials extends CustomResource {
    constructor(scope: Construct, id: string, props: AccountCredentialsProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "credentials",
                credentials_name: props.credentialsName,
                role_arn: props.roleArn,
            }
        });
    }

    public credentialsId(): string {
        return this.getAttString("credentials_id");
    }
}
