#!/usr/bin/env node

/*
 * Entrypoint for the CDK, here we construct an App with one or more Stacks.
 *
 * Because we would like to use the aws-sdk, we need to call some code
 * asynchronously.
 */

import { DeployApp } from "../lib/app";
import { App } from "aws-cdk-lib";

async function buildApplication(): Promise<App> {
    /*
     * Because you can not do async stuff in the constructor, we add an async
     * setup method to classes that need to perform some asynchronous
     * initialization.
     */
    const app = new DeployApp();
    await app.setup();
    return app;
}

/*
 * Because you can only do await in an async function, we manually 'await'
 * the Promise returned by the async function.
 */
buildApplication().then(
    (app) => {
        app.synth();
    },
    (reason) => {
        console.error(reason);
        process.exit(1);
    },
);
