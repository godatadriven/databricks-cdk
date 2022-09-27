import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";
import {UserPermission, GroupPermission, ServicePrinicpalPermission} from "./models";

export interface WarehousePermissionsProperties {
    workspaceUrl: string
    endpointId: string
    accessControlList: Array<UserPermission | GroupPermission | ServicePrinicpalPermission>
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
