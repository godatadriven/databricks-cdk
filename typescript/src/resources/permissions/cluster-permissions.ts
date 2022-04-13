import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


interface Permission {
    permission_level: string
}

export interface UserPermission extends Permission {
    user_name: string
}

export interface GroupPermission extends Permission {
    group_name: string
}

export interface ServicePrincipalPermission extends Permission {
    service_principal: string
}

export interface ClusterPermissionsProperties {
    workspaceUrl: string
    clusterId: string
    accessControlList: Array<UserPermission | GroupPermission | ServicePrincipalPermission>
}

export interface ClusterPermissionsProps extends ClusterPermissionsProperties {
    readonly serviceToken: string
}

export class ClusterPermissions extends CustomResource {
    constructor(scope: Construct, id: string, props: ClusterPermissionsProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "cluster-permissions",
                workspace_url: props.workspaceUrl,
                cluster_id: props.clusterId,
                access_control_list: props.accessControlList,
            }
        });
    }
}
