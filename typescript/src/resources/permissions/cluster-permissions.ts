import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";
import {UserPermission, GroupPermission, ServicePrinicpalPermission} from "./models";


export interface ClusterPermissionsProperties {
    workspaceUrl: string
    clusterId: string
    accessControlList: Array<UserPermission | GroupPermission | ServicePrinicpalPermission>
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
