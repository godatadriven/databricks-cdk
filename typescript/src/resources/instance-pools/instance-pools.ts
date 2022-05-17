import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

export interface InstancePoolAwsAttributes {
    spot_bid_price_percent?: number
    availability?: string
    zone_id?: string
}

export interface InstancePoolDockerBasicAuth {
    username: string
    password: string
}

export interface InstancePoolDockerImage {
    url: string
    basic_auth?: InstancePoolDockerBasicAuth
}

export interface DatabricksInstancePool {
    instance_pool_name: string
    min_idle_instances?: number
    max_capacity?: number
    aws_attributes?: InstancePoolAwsAttributes
    node_type_id: string
    custom_tags?: Record<string, string>
    idle_instance_autotermination_minutes?: number
    enable_elastic_disk?: boolean
    disk_spec?: Record<string, unknown>
    preloaded_spark_versions: Array<string>
    preloaded_docker_images?: Array<InstancePoolDockerImage>
}

export interface InstancePoolProperties {
    workspace_url: string
    instance_pool: DatabricksInstancePool
}

export interface InstancePoolProps extends InstancePoolProperties {
    readonly serviceToken: string
}

export class InstancePool extends CustomResource {
    constructor(scope: Construct, id: string, props: InstancePoolProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "instance-pool",
                workspace_url: props.workspace_url,
                instance_pool: props.instance_pool,
            }
        });
    }

    public instancePoolId(): string {
        return this.getAttString("physical_resource_id");
    }
}
