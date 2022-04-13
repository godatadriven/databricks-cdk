import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


export interface UserProperties {
    workspaceUrl: string
    userName: string
}

export interface UserProps extends UserProperties {
    readonly serviceToken: string
}

export class User extends CustomResource {
    constructor(scope: Construct, id: string, props: UserProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "user",
                workspace_url: props.workspaceUrl,
                user_name: props.userName
            }
        });
    }

    public userId(): string {
        return this.getAttString("user_id");
    }
}
