import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


export interface CredentialsProperties {
    readonly credentialsName: string
    readonly roleArn: string
}

export interface CredentialsProps extends CredentialsProperties {
    readonly serviceToken: string
}

export class Credentials extends CustomResource {
    constructor(scope: Construct, id: string, props: CredentialsProps) {
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
