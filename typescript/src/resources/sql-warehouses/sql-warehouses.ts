import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

export interface WarehouseTags {
    key: string
    value: string
}

export const SpotInstancePolicy = {
    COST_OPTIMIZED: "COST_OPTIMIZED",
    RELIABILITY_OPTIMIZED: "RELIABILITY_OPTIMIZED"
};

export interface DatabricksWarehouse {
    name: string
    cluster_size: string
    min_num_clusters?: number
    max_num_clusters: number
    auto_stop_mins?: number
    tags?: Array<WarehouseTags>
    spot_instance_policy?: SpotInstancePolicy
    enable_photon?: boolean
    enable_serverless_compute?: boolean
    channel?: string
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
