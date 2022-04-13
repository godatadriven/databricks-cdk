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

export interface NewCluster {
    num_workers?: number
    autoscale?: AutoScale
    spark_version: string
    spark_conf?: Record<string, string>
    aws_attributes: AwsAttributes
    node_type_id?: string
    driver_node_type_id?: string
    ssh_public_keys?: Array<string>
    custom_tags?: Record<string, unknown>
    cluster_log_conf?: string
    init_scripts?: Array<InitScriptInfoDbfs | InitScriptInfoFile | InitScriptInfoS3>
    docker_image?: DockerImage
    spark_env_vars?: Record<string, unknown>
    autotermination_minutes?: number
    enable_elastic_disk?: boolean
    driver_instance_pool_id?: string
    instance_pool_id?: string
}

export interface JobCluster {
    job_cluster_key: string
    new_cluster: NewCluster
}

export interface NotebookTask {
    notebook_path: string
    base_parameters?: Record<string, unknown>
}

export interface SparkJarTask {
    main_class_name: string
    parameters?: Array<string>
}

export interface SparkPythonTask {
    python_file: string
    parameters?: Array<string>
}

export interface SparkSubmitTask {
    parameters: Array<string>
}

export interface PipelineTask {
    pipeline_id: string
}

export interface PythonWheelTask {
    package_name: string
    entry_point?: string
    parameters?: Array<string>
}

export interface Library {
    jar?: string
    egg?: string
    whl?: string
    pypi?: string
    maven?: string
    cran?: string
}

export interface JobEmailNotifications {
    on_start?: Array<string>
    on_success?: Array<string>
    on_failure?: Array<string>
    no_alert_for_skipped_runs?: boolean
}

export interface JobTaskSettings {
    task_key: string
    description?: string
    depends_on?: Array<string>
    existing_cluster_id?: string
    new_cluster?: NewCluster
    job_cluster_key?: string
    notebook_task?: NotebookTask
    spark_jar_task?: SparkJarTask
    spark_python_task?: SparkPythonTask
    spark_submit_task?: SparkSubmitTask
    pipeline_task?: PipelineTask
    python_wheel_task?: PythonWheelTask
    libraries? : Array<Library>
    email_notifications?: JobEmailNotifications
    timeout_seconds?: number
    max_retries?: number
    min_retry_interval_millis?: number
    retry_on_timeout?: boolean
}

export interface CronSchedule {
    quartz_cron_expression?: string
    timezone_id?: string
    pause_status?: string
}

export interface AccessControlRequestForUser {
    user_name: string
    permission_level: string
}

export interface AccessControlRequestForGroup {
    group_name: string
    permission_level: string
}

export interface JobSettings {
    name: string
    tasks: Array<JobTaskSettings>
    job_clusters?: Array<JobCluster>
    email_notifications?: JobEmailNotifications
    timeout_seconds?: number
    schedule?: CronSchedule
    max_concurrent_runs?: number
    format?: string
    access_control_list?: Array<AccessControlRequestForUser | AccessControlRequestForGroup>
}

export interface JobProperties {
    workspaceUrl: string
    job: JobSettings
}

export interface JobProps extends JobProperties {
    readonly serviceToken: string
}

export class Job extends CustomResource {
    constructor(scope: Construct, id: string, props: JobProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "job",
                workspace_url: props.workspaceUrl,
                job: props.job,
            }
        });
    }
}
