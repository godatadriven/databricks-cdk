import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

export interface UnityCatalogPermissions {
    principal: string
    privileges: Array<string>

}

export interface UnityCatalogPermissionsList {
    privilege_assignments: Array<UnityCatalogPermissions>
}

export interface UnityCatalogPermissionProperties {
    workspace_url: string
    sec_type: string
    sec_id: string
    permissions: UnityCatalogPermissionsList
}

export interface UnityCatalogPermissionProps extends UnityCatalogPermissionProperties {
    readonly serviceToken: string
}

export class UnityCatalogPermission extends CustomResource {
    constructor(scope: Construct, id: string, props: UnityCatalogPermissionProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "catalog-permission",
                workspace_url: props.workspace_url,
                permissions: props.permissions,
            }
        });
    }
}
