import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

export interface UnityCatalogCatalogSettings {
    name: string
    comment?: string
    properties?: Map<string, string>
    owner?: string
}

export interface UnityCatalogCatalogProperties {
    workspace_url: string
    catalog: UnityCatalogCatalogSettings
}

export interface UnityCatalogCatalogProps extends UnityCatalogCatalogProperties {
    readonly serviceToken: string
}

export class UnityCatalogCatalog extends CustomResource {
    constructor(scope: Construct, id: string, props: UnityCatalogCatalogProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "catalog",
                workspace_url: props.workspace_url,
                catalog: props.catalog,
            }
        });
    }
}
