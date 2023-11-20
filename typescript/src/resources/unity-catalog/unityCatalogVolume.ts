import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

enum volumeType {
    EXTERNAL = "EXTERNAL",
    MANAGED = "MANAGED",
}


export interface UnityCatalogVolumeSettings {
    name: string
    schema_name: string
    catalog_name: string
    volume_type?: volumeType
    comment?: string
    storage_location?: string

}

export interface UnityCatalogVolumeProperties {
    workspace_url: string
    volume: UnityCatalogVolumeSettings
}

export interface UnityCatalogVolumeProps extends UnityCatalogVolumeProperties {
    readonly serviceToken: string
}

export class UnityCatalogVolume extends CustomResource {
    constructor(scope: Construct, id: string, props: UnityCatalogVolumeProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "volume",
                workspace_url: props.workspace_url,
                catalog: props.volume,
            }
        });
    }
}
