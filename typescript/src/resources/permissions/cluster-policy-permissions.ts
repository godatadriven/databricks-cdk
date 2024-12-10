import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";
import {UserPermission, GroupPermission, ServicePrincipalPermission} from "./models";


export interface ClusterPolicyPermissionsProperties {
    workspaceUrl: string
    clusterPolicyId: string
    accessControlList: Array<UserPermission | GroupPermission | ServicePrincipalPermission>
}

export interface ClusterPolicyPermissionsProps extends ClusterPolicyPermissionsProperties {
    readonly serviceToken: string
}

export class ClusterPolicyPermissions extends CustomResource {
    constructor(scope: Construct, id: string, props: ClusterPolicyPermissionsProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "cluster-policy-permissions",
                workspace_url: props.workspaceUrl,
                cluster_policy_id: props.clusterPolicyId,
                access_control_list: props.accessControlList,
            }
        });
    }
}
