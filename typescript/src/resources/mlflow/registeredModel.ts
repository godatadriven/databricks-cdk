import {Construct} from "constructs";
import {CustomResource} from "aws-cdk-lib";
export interface RegisteredModelTag {
    key: string
    value: string
}
export interface RegisteredModelProperties {
    workspaceUrl: string
    name: string
    description?: string
    tags?: Array<RegisteredModelTag>
}

export interface RegisteredModelProps extends RegisteredModelProperties {
    readonly serviceToken: string
}

export class RegisteredModel extends CustomResource {
    constructor(scope: Construct, id: string, props: RegisteredModelProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "mlflow-registered-model",
                workspace_url: props.workspaceUrl,
                name: props.name,
                description: props.description,
                tags: props.tags
            }
        });
    }
}
