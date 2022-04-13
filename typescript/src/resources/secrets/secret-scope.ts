import {Construct} from "constructs";
import {CustomResource} from "aws-cdk-lib";
import {Secret} from "./secret";


export interface SecretScopeProperties {
    workspaceUrl: string
    scope: string
    initialManagePrincipal: string
}

export interface SecretScopeProps extends SecretScopeProperties {
    readonly serviceToken: string
}

export class SecretScope extends CustomResource {
    props: SecretScopeProps

    constructor(scope: Construct, id: string, props: SecretScopeProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "secret-scope",
                workspace_url: props.workspaceUrl,
                scope: props.scope,
                initial_manage_principal: props.initialManagePrincipal,
            }
        });
        this.props = props;
    }

    public createSecret(scope: Construct, id: string, key: string, stringValue: string): Secret {
        const s = new Secret(scope, id, {
            workspaceUrl: this.props.workspaceUrl,
            scope: this.props.scope,
            key: key,
            stringValue: stringValue,
            serviceToken: this.props.serviceToken
        });
        s.node.addDependency(this);
        return s;
    }
}
