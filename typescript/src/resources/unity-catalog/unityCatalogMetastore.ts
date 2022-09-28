import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

export interface UnityCatalogMetastoreSettings {
    name: string
    storage_root: string
    owner?: string
}

export interface UnityCatalogMetastoreProperties {
    workspace_url: string
    metastore: UnityCatalogMetastoreSettings
    iam_role: string
}

export interface UnityCatalogMetastoreProps extends UnityCatalogMetastoreProperties {
    readonly serviceToken: string
}

export class UnityCatalogMetastore extends CustomResource {
    constructor(scope: Construct, id: string, props: UnityCatalogMetastoreProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "unity-metastore",
                workspace_url: props.workspace_url,
                metastore: props.metastore,
                iam_role: props.iam_role,
            }
        });
    }

    public metastoreId(): string {
        return this.getAttString("metastore_id");
    }
}
