import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


interface JobPermission {
    permission_level: string
}

export interface JobPermissionUser extends JobPermission {
    user_name: string
}

export interface JobPermissionGroup extends JobPermission {
    group_name: string
}

export interface JobPermissionServicePrincipal extends JobPermission {
    service_principal: string
}

export interface JobPermissionsProperties {
    workspaceUrl: string
    jobId: string
    accessControlList: Array<JobPermissionUser | JobPermissionGroup | JobPermissionServicePrincipal>
    ownerPermission: JobPermissionUser | JobPermissionGroup | JobPermissionServicePrincipal
}

export interface JobPermissionsProps extends JobPermissionsProperties {
    readonly serviceToken: string
}

export class JobPermissions extends CustomResource {
    constructor(scope: Construct, id: string, props: JobPermissionsProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "job-permissions",
                workspace_url: props.workspaceUrl,
                cluster_id: props.jobId,
                access_control_list: props.accessControlList,
                owner_permission: props.ownerPermission
            }
        });
    }
}
