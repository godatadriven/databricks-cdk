import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

export interface UnityCatalogSchemaSettings {
    name: string
    catalog_name: string
    comment?: string
    properties?: Map<string, string>
    owner?: string
}

export interface UnityCatalogSchemaProperties {
    workspace_url: string
    schema: UnityCatalogSchemaSettings
}

export interface UnityCatalogSchemaProps extends UnityCatalogSchemaProperties {
    readonly serviceToken: string
}

export class UnityCatalogSchema extends CustomResource {
    constructor(scope: Construct, id: string, props: UnityCatalogSchemaProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "unity-schema",
                workspace_url: props.workspace_url,
                schema: props.schema,
            }
        });
    }
}
