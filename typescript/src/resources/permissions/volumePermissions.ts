import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

export enum PrivilegeVolume {
    APPLY_TAG = "APPLY_TAG",
    READ_VOLUME = "READ_VOLUME",
    WRITE_VOLUME = "WRITE_VOLUME",
    ALL_PRIVILEGES = "ALL_PRIVILEGES"
}

export interface PrivilegeAssignmentVolume {
    principal: string
    privileges: Array<PrivilegeVolume>
}

export interface VolumePermissionsProperties {
    workspaceUrl: string
    privilege_assignments: Array<PrivilegeAssignmentVolume>
    volume_name: string
}

export interface VolumePermissionsProps extends VolumePermissionsProperties {
    readonly serviceToken: string
}

export class VolumePermissions extends CustomResource {
    constructor(scope: Construct, id: string, props: VolumePermissionsProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "volume-permissions",
                workspace_url: props.workspaceUrl,
                volume_name: props.volume_name,
                privilege_assignments: props.privilege_assignments,
            }
        });
    }
}
