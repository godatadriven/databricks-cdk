import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

export interface UnityCatalogMetastoreAssignmentProperties {
    workspace_url: string
    workspace_id: string
    metastore_id: string
    default_catalog_name: string
}

export interface UnityCatalogMetastoreAssignmentProps extends UnityCatalogMetastoreAssignmentProperties {
    readonly serviceToken: string
}

export class UnityCatalogMetastoreAssignment extends CustomResource {
    constructor(scope: Construct, id: string, props: UnityCatalogMetastoreAssignmentProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "metastore-assignment",
                workspace_url: props.workspace_url,
                workspace_id: props.workspace_id,
                metastore_id: props.metastore_id,
                default_catalog_name: props.default_catalog_name,
            }
        });
    }
}
