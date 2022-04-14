import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


interface ClusterPermission {
    permission_level: string
}

export interface ClusterPermissionUser extends ClusterPermission {
    user_name: string
}

export interface ClusterPermissionGroup extends ClusterPermission {
    group_name: string
}

export interface ClusterPermissionServicePrincipal extends ClusterPermission {
    service_principal: string
}

export interface ClusterPermissionsProperties {
    workspaceUrl: string
    clusterId: string
    accessControlList: Array<ClusterPermissionUser | ClusterPermissionGroup | ClusterPermissionServicePrincipal>
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
