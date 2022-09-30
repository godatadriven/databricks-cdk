import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

export interface UnityCatalogExternalLocationSettings {
    name: string
    comment?: string
    url: string
    credential_name: string
    read_only?: boolean
}

export interface UnityCatalogExternalLocationProperties {
    workspace_url: string
    external_location: UnityCatalogExternalLocationSettings
}

export interface UnityCatalogExternalLocationProps extends UnityCatalogExternalLocationProperties {
    readonly serviceToken: string
}

export class UnityCatalogExternalLocation extends CustomResource {
    constructor(scope: Construct, id: string, props: UnityCatalogExternalLocationProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "unity-external-location",
                workspace_url: props.workspace_url,
                external_location: props.external_location,
            }
        });
    }
}
