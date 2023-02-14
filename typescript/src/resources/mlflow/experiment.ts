import {Construct} from "constructs";
import {CustomResource} from "aws-cdk-lib";

export interface ExperimentProperties {
    workspaceUrl: string
    artifactLocation?: string
    name: string
    description?: string
}

export interface ExperimentProps extends ExperimentProperties {
    readonly serviceToken: string
}

export class Experiment extends CustomResource {
    constructor(scope: Construct, id: string, props: ExperimentProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "mlflow-experiment",
                workspace_url: props.workspaceUrl,
                artifact_location: props.artifactLocation,
                name: props.name,
                description: props.description
            }
        });
    }
}
