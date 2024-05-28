import {
    CustomResource,
    custom_resources as cr,
} from "aws-cdk-lib";
import { IFunction } from "aws-cdk-lib/aws-lambda";
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
    readonly databricksDeployLambda: IFunction
}

export class UnityCatalogSchema extends Construct {
    readonly customResource: CustomResource;
    readonly catalogName: string;
    readonly name: string;

    constructor(scope: Construct, id: string, props: UnityCatalogSchemaProps) {
        super(scope, id);
        this.catalogName = props.schema.catalog_name;
        this.name = props.schema.name;


        const cr_provider = new cr.Provider(this, 'Provider',
        {
            onEventHandler: props.databricksDeployLambda,
        });

        this.customResource = new CustomResource(this, 'Resource', {
            serviceToken: cr_provider.serviceToken,
            properties: {
                workspace_url: props.workspace_url,
                schema: props.schema
            }
        });

    }
}
