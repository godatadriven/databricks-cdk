import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";
import {User, Group, ServicePrincipal} from "./models";


export enum ExperimentPermissionLevel {
    "CAN_READ",
    "CAN_EDIT",
    "CAN_MANAGE"
}

export interface UserExperimentPermission extends User {
    permission_level: ExperimentPermissionLevel
}

export interface GroupExperimentPermission extends Group {
    permission_level: ExperimentPermissionLevel
}

export interface ServicePrinicpalExperimentPermission extends ServicePrincipal {
    permission_level: ExperimentPermissionLevel
}


export interface ExperimentPermissionsProperties {
    workspaceUrl: string
    experimentId: string
    accessControlList: Array<UserExperimentPermission | GroupExperimentPermission | ServicePrinicpalExperimentPermission>
}

export interface ExperimentPermissionsProps extends ExperimentPermissionsProperties {
    readonly serviceToken: string
}

export class ExperimentPermissions extends CustomResource {
    constructor(scope: Construct, id: string, props: ExperimentPermissionsProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "experiment-permission",
                workspace_url: props.workspaceUrl,
                experiment_id: props.experimentId,
                access_control_list: props.accessControlList,
            }
        });
    }
}
