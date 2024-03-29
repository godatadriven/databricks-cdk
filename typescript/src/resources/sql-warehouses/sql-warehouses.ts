import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

export interface WarehouseTagPairs {
    key: string
    value: string
}

export interface WarehouseTags {
    custom_tags: Array<WarehouseTagPairs>
}

export interface Channel {
    name: string
}

export interface DatabricksWarehouse {
    name: string
    cluster_size: string
    min_num_clusters?: number
    max_num_clusters: number
    auto_stop_mins?: number
    tags?: WarehouseTags
    spot_instance_policy?: string
    enable_photon?: boolean
    enable_serverless_compute?: boolean
    channel?: Channel
}

export interface WarehouseProperties {
    workspace_url: string
    warehouse: DatabricksWarehouse
}

export interface WarehouseProps extends WarehouseProperties {
    readonly serviceToken: string
}

export class Warehouse extends CustomResource {
    constructor(scope: Construct, id: string, props: WarehouseProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "warehouse",
                workspace_url: props.workspace_url,
                warehouse: props.warehouse,
            }
        });
    }

    public warehouseId(): string {
        return this.getAttString("physical_resource_id");
    }
}
