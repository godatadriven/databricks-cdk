import { CustomResource } from "aws-cdk-lib";
import { Construct } from "constructs";

export interface ComplexValue {
    // TODO: Add more fields according to the actual schema
    value?: string,
}

export interface ServicePrincipalSettings {
    active?: boolean
    application_id?: string
    display_name?: string
    entitlements?: ComplexValue[]
    external_id?: string
    groups?: ComplexValue[]
    id?: string
    roles?: ComplexValue[]
    // TODO: Add support for a "schemas" field
}

export interface ServicePrincipalProperties {
    workspace_url: string
    service_principal_settings: ServicePrincipalSettings
}

export interface ServicePrincipalProps extends ServicePrincipalProperties {
    readonly serviceToken: string
}

export class ServicePrincipal extends CustomResource {
    constructor(scope: Construct, id: string, props: ServicePrincipalProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "service-principal",
                workspace_url: props.workspace_url,
                service_principal: props.service_principal_settings,
            }
        });
    }
}
