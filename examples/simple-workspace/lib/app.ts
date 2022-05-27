import {App} from "aws-cdk-lib";
import {SimpleStack} from "./simple/stack";

export class DeployApp extends App {

    public async setup(): Promise<void> {
        new SimpleStack(this, "simple", {});
    }
}
