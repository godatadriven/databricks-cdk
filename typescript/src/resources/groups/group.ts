import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


export interface GroupUserMember {
    user_name: string
}

export interface GroupGroupMember {
    group_name: string
}


export interface GroupProperties {
    workspaceUrl: string
    groupName: string
    members: Array<GroupUserMember | GroupGroupMember>
}

export interface GroupProps extends GroupProperties {
    readonly serviceToken: string
}

export class Group extends CustomResource {
    constructor(scope: Construct, id: string, props: GroupProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "group",
                workspace_url: props.workspaceUrl,
                group_name: props.groupName,
                members: props.members,
            }
        });
    }
}
