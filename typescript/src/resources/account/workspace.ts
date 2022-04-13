import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


export interface WorkspaceProperties {
    readonly workspaceName: string
    readonly deploymentName?: string
    readonly awsRegion: string
    readonly credentialsId: string
    readonly storageConfigurationId: string
    readonly networkId?: string
    readonly managedServicesCustomerManaged_keyId?: string
    readonly pricingTier?: string
    readonly storageCustomerManagedKeyId?: string
}

export interface WorkspaceProps extends WorkspaceProperties {
    readonly serviceToken: string
}

export class Workspace extends CustomResource {
    constructor(scope: Construct, id: string, props: WorkspaceProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "workspaces",
                workspace_name: props.workspaceName,
                deployment_name: props.deploymentName,
                aws_region: props.awsRegion,
                credentials_id: props.credentialsId,
                storage_configuration_id: props.storageConfigurationId,
                network_id: props.networkId,
                managed_services_customer_managed_key_id: props.managedServicesCustomerManaged_keyId,
                pricing_tier: props.pricingTier,
                storage_customer_managed_key_id: props.storageCustomerManagedKeyId,
            }
        });
    }

    public workspaceId(): string {
        return this.getAttString("workspace_id");
    }

    public workspaceUrl(): string {
        return this.getAttString("workspace_url");
    }
}
