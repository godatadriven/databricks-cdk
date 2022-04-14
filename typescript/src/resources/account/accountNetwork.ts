import {CustomResource} from "aws-cdk-lib";
import {Construct} from "constructs";


export interface AccountNetworkProperties {
    readonly networkName: string
    readonly vpcId: string
    readonly subnet_ids: Array<string>
    readonly security_group_ids: Array<string>
}

export interface AccountNetworkProps extends AccountNetworkProperties {
    readonly serviceToken: string
}

export class AccountNetwork extends CustomResource {
    constructor(scope: Construct, id: string, props: AccountNetworkProps) {
        super(scope, id, {
            serviceToken: props.serviceToken,
            properties: {
                action: "networks",
                network_name: props.networkName,
                vpc_id: props.vpcId,
                subnet_ids: props.subnet_ids,
                security_group_ids: props.security_group_ids,
            }
        });
    }

    public networkId(): string {
        return this.getAttString("network_id");
    }
}
