import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";
import {User, ServicePrincipal, UserPermission, GroupPermission, ServicePrincipalPermission} from "./models";


export interface JobPermissionsProperties {
    workspaceUrl: string
    jobId: string
    accessControlList: Array<UserPermission | GroupPermission | ServicePrincipalPermission>
    owner: User | ServicePrincipal
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
                job_id: props.jobId,
                access_control_list: props.accessControlList,
                owner: props.owner
            }
        });
    }
}
