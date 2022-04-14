import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


export interface ScimUserProperties {
    workspaceUrl: string
    userName: string
}

export interface ScimUserProps extends ScimUserProperties {
    readonly serviceToken: string
}

export class ScimUser extends CustomResource {
    constructor(scope: Construct, id: string, props: ScimUserProps) {
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
