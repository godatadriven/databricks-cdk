import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


export interface AccountStorageConfigProperties {
    readonly storageConfigName: string
    readonly bucketName: string
}

export interface AccountStorageConfigProps extends AccountStorageConfigProperties {
    readonly serviceToken: string
}

export class AccountStorageConfig extends CustomResource {
    constructor(scope: Construct, id: string, props: AccountStorageConfigProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "storage-configurations",
                storage_configuration_name: props.storageConfigName,
                bucket_name: props.bucketName,
            }
        });
    }

    public storageConfigId(): string {
        return this.getAttString("storage_configuration_id");
    }
}
