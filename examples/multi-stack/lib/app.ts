import {App} from "aws-cdk-lib";
import {RootStack} from "./root-stack/stack";
import {ApplicationStack} from "./application-stack/stack";

export class DeployApp extends App {

    public async setup(): Promise<void> {
        new RootStack(this, "root-stack", {});
        new ApplicationStack(this, "application-stack", {});
    }
}
