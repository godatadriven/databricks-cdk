import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";

export interface AutoScale {
    min_workers: number
    max_workers: number
}

export interface AwsAttributes {
    first_on_demand?: number
    availability?: string
    zone_id: string
    instance_profile_arn?: string
    spot_bid_price_percent?: number
    ebs_volume_type?: string
    ebs_volume_count?: number
    ebs_volume_size?: number
    ebs_volume_iops?: number
    ebs_volume_throughput?: number
}

export interface StorageInfo {
    destination: string
}

export interface StorageInfoS3 extends StorageInfo{
    region: string
    endpoint?: string
    enable_encryption?: boolean
    encryption_type?: string
    kms_key?: string
    canned_acl?: string
}

export interface InitScriptInfoDbfs {
    dbfs: StorageInfo
}

export interface InitScriptInfoFile {
    file: StorageInfo
}

export interface InitScriptInfoS3 {
    s3: StorageInfoS3
}

export interface DockerBasicAuth {
    username: string
    password: string
}

export interface DockerImage {
    url: string
    basic_auth?: DockerBasicAuth
}

export interface DatabricksCluster {
    num_workers?: number
    autoscale?: AutoScale
    cluster_name: string
    spark_version: string
    docker_image?: DockerImage
    spark_conf?: Record<string, string>
    aws_attributes: AwsAttributes
    node_type_id?: string
    driver_node_type_id?: string
    ssh_public_keys?: Array<string>
    custom_tags?: Record<string, unknown>
    cluster_log_conf?: string
    init_scripts?: Array<InitScriptInfoDbfs | InitScriptInfoFile | InitScriptInfoS3>
    spark_env_vars?: Record<string, unknown>
    autotermination_minutes?: number
    enable_elastic_disk?: boolean
    driver_instance_pool_id?: string
    instance_pool_id?: string
    idempotency_token?: string
    apply_policy_default_values?: boolean
    enable_local_disk_encryption?: boolean
}

export interface ClusterProperties {
    workspaceUrl: string
    cluster: DatabricksCluster
}

export interface ClusterProps extends ClusterProperties {
    readonly serviceToken: string
}

export class Cluster extends CustomResource {
    constructor(scope: Construct, id: string, props: ClusterProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "cluster",
                workspace_url: props.workspaceUrl,
                cluster: props.cluster
            }
        });
    }

    public clusterId(): string {
        return this.getAttString("physical_resource_id");
    }
}
