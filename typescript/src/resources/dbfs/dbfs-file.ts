import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


export interface DbfsFileProperties {
    workspaceUrl: string
    path: string
    base64Bytes: string
}

export interface DbfsFileProps extends DbfsFileProperties {
    readonly serviceToken: string
}

export class DbfsFile extends CustomResource {
    constructor(scope: Construct, id: string, props: DbfsFileProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "dbfs-file",
                workspace_url: props.workspaceUrl,
                path: props.path,
                base64_bytes: props.base64Bytes,
            }
        });
    }
}
