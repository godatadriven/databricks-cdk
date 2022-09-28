import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

export interface UnityCatalogStorageCredential {
    name: string
    comment?: string
}

export interface UnityCatalogAwsIamRole {
    role_arn: string
}

export interface StorageCredentialAws extends UnityCatalogStorageCredential {
    aws_iam_role: UnityCatalogAwsIamRole
}

export interface UnityCatalogAzureServicePrincipal {
    directory_id: string
    application_id: string
    client_secret: string
}

export interface StorageCredentialAzure extends UnityCatalogStorageCredential {
    azure_service_principal: UnityCatalogAzureServicePrincipal
}

export interface UnityCatalogGcpServiceAccountKey {
    email: string
    private_key_id: string
    private_key: string
}

export interface StorageCredentialGcp extends UnityCatalogStorageCredential {
    gcp_service_account_key: UnityCatalogGcpServiceAccountKey
}

export interface UnityCatalogStorageCredentialProperties {
    workspace_url: string
    storage_credential: StorageCredentialAws | StorageCredentialAzure | StorageCredentialGcp
}

export interface UnityCatalogStorageCredentialProps extends UnityCatalogStorageCredentialProperties {
    readonly serviceToken: string
}

export class UnityCatalogStorageCredential extends CustomResource {
    constructor(scope: Construct, id: string, props: UnityCatalogStorageCredentialProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "unity-storage-credentials",
                workspace_url: props.workspace_url,
                storage_credential: props.storage_credential,
            }
        });
    }
}
