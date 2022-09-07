import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


interface WarehousePermission {
    permission_level: string
}

export interface WarehousePermissionUser extends WarehousePermission {
    user_name: string
}

export interface WarehousePermissionGroup extends WarehousePermission {
    group_name: string
}

export interface WarehousePermissionServicePrincipal extends WarehousePermission {
    service_principal: string
}

export interface WarehousePermissionsProperties {
    workspaceUrl: string
    endpointId: string
    accessControlList: Array<WarehousePermissionUser | WarehousePermissionGroup | WarehousePermissionServicePrincipal>
}

export interface WarehousePermissionsProps extends WarehousePermissionsProperties {
    readonly serviceToken: string
}

export class WarehousePermissions extends CustomResource {
    constructor(scope: Construct, id: string, props: WarehousePermissionsProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "warehouse-permissions",
                workspace_url: props.workspaceUrl,
                endpoint_id: props.endpointId,
                access_control_list: props.accessControlList,
            }
        });
    }
}
