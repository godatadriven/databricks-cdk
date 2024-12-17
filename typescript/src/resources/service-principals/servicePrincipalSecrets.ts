import { CustomResource } from "aws-cdk-lib";
import { Construct } from "constructs";


export interface ServicePrincipalSecretsProperties {
    service_principal_id: number
}

export interface ServicePrincipalSecretsProps extends ServicePrincipalSecretsProperties {
    readonly serviceToken: string
}

export class ServicePrincipalSecrets extends CustomResource {
    constructor(scope: Construct, id: string, props: ServicePrincipalSecretsProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "service-principal-secrets",
                service_principal_id: props.service_principal_id,
            }
        });
    }
}
