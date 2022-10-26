import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


export interface ClusterPolicySettings {
    name: string
    description?: string
    definition: string | Record<string, Record<string, number | boolean | string | Array<string>>>
}

export interface ClusterPolicyProperties {
    workspaceUrl: string
    clusterPolicy: ClusterPolicySettings
}

export interface ClusterPolicyProps extends ClusterPolicyProperties {
    serviceToken: string
}


export class ClusterPolicy extends CustomResource {
    constructor(scope: Construct, id: string, props: ClusterPolicyProps) {
        props.clusterPolicy.definition = JSON.stringify(props.clusterPolicy.definition);
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "cluster-policy",
                workspace_url: props.workspaceUrl,
                cluster_policy: props.clusterPolicy
            }
        });
    }

    public policyId(): string {
        return this.getAttString("physical_resource_id");
    }
}

