import {Duration, CfnOutput} from "aws-cdk-lib";
import {aws_lambda, aws_ssm, aws_logs, aws_iam} from "aws-cdk-lib";
import path from "path";
import * as fs from "fs";
import {Credentials, CredentialsProperties} from "../resources/account/credentials";
import {StorageConfig, StorageConfigProperties} from "../resources/account/storage-config";
import {Network, NetworkProperties} from "../resources/account/network";
import {Workspace, WorkspaceProperties} from "../resources/account/workspace";
import {InstanceProfile, InstanceProfileProperties} from "../resources/instance-profiles/instance-profile";
import {Cluster, ClusterProperties} from "../resources/clusters/cluster";
import {ClusterPermissions, ClusterPermissionsProperties} from "../resources/permissions/cluster-permissions";
import {DbfsFile, DbfsFileProperties} from "../resources/dbfs/dbfs-file";
import {SecretScope, SecretScopeProperties} from "../resources/secrets/secret-scope";
import {Job, JobProperties} from "../resources/jobs/job";
import {Group, GroupProperties} from "../resources/groups/group";
import {User, UserProperties} from "../resources/scim/user";
import {randomUUID} from "crypto";
import {Construct} from "constructs";
import * as os from "os";


interface CustomDeployLambdaProps {
    readonly accountId: string
    readonly region: string
    readonly lambdaVersion?: string
}

abstract class IDatabricksDeployLambda extends Construct {
    protected serviceToken: string

    public createCredential(scope: Construct, id: string, props: CredentialsProperties): Credentials {
        return new Credentials(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createStorageConfig(scope: Construct, id: string, props: StorageConfigProperties): StorageConfig {
        return new StorageConfig(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createNetwork(scope: Construct, id: string, props: NetworkProperties): Network {
        return new Network(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createWorkspace(scope: Construct, id: string, props: WorkspaceProperties): Workspace {
        return new Workspace(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createInstanceProfile(scope: Construct, id: string, props: InstanceProfileProperties): InstanceProfile {
        return new InstanceProfile(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createCluster(scope: Construct, id: string, props: ClusterProperties): Cluster {
        return new Cluster(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createUser(scope: Construct, id: string, props: UserProperties): User {
        return new User(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createClusterPermissions(scope: Construct, id: string, props: ClusterPermissionsProperties): ClusterPermissions {
        return new ClusterPermissions(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createGroup(scope: Construct, id: string, props: GroupProperties): Group {
        return new Group(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createDbfsFile(scope: Construct, id: string, props: DbfsFileProperties): DbfsFile {
        return new DbfsFile(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createSecretScope(scope: Construct, id: string, props: SecretScopeProperties): SecretScope {
        return new SecretScope(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createJob(scope: Construct, id: string, props: JobProperties): Job {
        return new Job(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }
}

class DatabricksDeployLambdaImport extends IDatabricksDeployLambda {
    constructor(scope: Construct, id: string, serviceToken: string) {
        super(scope, id);
        this.serviceToken = serviceToken;
    }
}

export class DatabricksDeployLambda extends IDatabricksDeployLambda {

    readonly props: CustomDeployLambdaProps
    readonly lambda: aws_lambda.IFunction
    readonly lambdaRole: aws_iam.IRole

    constructor(scope: Construct, id: string, props: CustomDeployLambdaProps) {
        super(scope, id);
        this.props = props;

        const dockerTempDir = fs.mkdtempSync(path.join(os.tmpdir(), "databricks-cdk-lambda-"));
        const dockerFile = path.join(dockerTempDir, "Dockerfile");
        const lambdaVersion = props.lambdaVersion || "dev"; // TODO: get from package version
        fs.writeFileSync(dockerFile, `FROM ffinfo/databricks-cdk-deploy-lambda:${lambdaVersion}`);

        // Random hash will trigger a rebuild always, only need if dev is used
        const randomHash = (lambdaVersion == "dev") ? randomUUID() : "";

        const dockerImageCode = aws_lambda.DockerImageCode.fromImageAsset(
            dockerTempDir, {
                file: "Dockerfile",
                buildArgs: {
                    version: lambdaVersion,
                    randomHash: randomHash,
                }
            }
        );

        this.lambdaRole = new aws_iam.Role(this, "Role", {
            assumedBy: new aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managedPolicies: [
                aws_iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
            ]
        });
        this.lambdaRole.addToPrincipalPolicy(new aws_iam.PolicyStatement({
            effect: aws_iam.Effect.ALLOW,
            actions: ["ssm:GetParameter"],
            resources: [
                `arn:aws:ssm:${this.props.region}:${this.props.accountId}:parameter/databricks/*`,
            ]
        }));

        this.lambda = new aws_lambda.DockerImageFunction(this, "Lambda", {
            functionName: "DatabricksDeploy",
            code: dockerImageCode,
            timeout: Duration.seconds(300),
            role: this.lambdaRole,
            memorySize: 512,
            environment: {
                LAMBDA_METHOD: "databricks-deploy",
                SENTRY_DSN: aws_ssm.StringParameter.valueForStringParameter(
                    this, "/datascience-resources/sentry_dsn"),
            },
            logRetention: aws_logs.RetentionDays.THREE_MONTHS,
        });
        this.serviceToken = this.lambda.functionArn;

        new CfnOutput(this, "OutputLambdaArn", {
            exportName: "databricks-deploy-lambda-arn",
            value: this.lambda.functionArn
        });
    }

    public static fromServiceToken(scope: Construct, id: string, serviceToken: string): IDatabricksDeployLambda {
        return new DatabricksDeployLambdaImport(scope, id, serviceToken);
    }
}
