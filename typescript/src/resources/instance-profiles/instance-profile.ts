import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


export interface InstanceProfileProperties {
    workspaceUrl: string
    instanceProfileArn: string
    isMetaInstanceProfile?: boolean
    skipValidation?: boolean
}

export interface InstanceProfileProps extends InstanceProfileProperties {
    readonly serviceToken: string
}

export class InstanceProfile extends CustomResource {
    constructor(scope: Construct, id: string, props: InstanceProfileProps) {
        let skipValidation = true;
        // Sometimes the validation randoms fails, this mainly done for the UI
        if (props.skipValidation) {
            skipValidation = props.skipValidation;
        }
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "instance-profile",
                workspace_url: props.workspaceUrl,
                instance_profile_arn: props.instanceProfileArn,
                is_meta_instance_profile: props.isMetaInstanceProfile,
                skip_validation: skipValidation,
            }
        });
    }
}
