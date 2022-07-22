import {Duration} from "aws-cdk-lib";
import {aws_lambda, aws_logs, aws_iam} from "aws-cdk-lib";
import {AccountCredentials, AccountCredentialsProperties} from "./account/accountCredentials";
import {AccountStorageConfig, AccountStorageConfigProperties} from "./account/account-storage-config";
import {AccountNetwork, AccountNetworkProperties} from "./account/accountNetwork";
import {Workspace, WorkspaceProperties} from "./account/workspace";
import {InstanceProfile, InstanceProfileProperties} from "./instance-profiles/instance-profile";
import {Cluster, ClusterProperties} from "./clusters/cluster";
import {ClusterPermissions, ClusterPermissionsProperties} from "./permissions/cluster-permissions";
import {DbfsFile, DbfsFileProperties} from "./dbfs/dbfs-file";
import {SecretScope, SecretScopeProperties} from "./secrets/secret-scope";
import {Job, JobProperties} from "./jobs/job";
import {Group, GroupProperties} from "./groups/group";
import {ScimUser, ScimUserProperties} from "./scim/scimUser";
import {InstancePool, InstancePoolProperties} from "./instance-pools/instance-pools";
import {Construct} from "constructs";
import {DockerImage} from "../docker-image";


interface CustomDeployLambdaProps {
    readonly accountId: string
    readonly region: string
    readonly lambdaVersion?: string
    readonly databricksUserParam?: string
    readonly databricksPassParam?: string
    readonly databricksAccountParam?: string
    readonly lambdaCode?: aws_lambda.DockerImageCode
}

export abstract class IDatabricksDeployLambda extends Construct {
    serviceToken = "";

    public createCredential(scope: Construct, id: string, props: AccountCredentialsProperties): AccountCredentials {
        return new AccountCredentials(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createStorageConfig(scope: Construct, id: string, props: AccountStorageConfigProperties): AccountStorageConfig {
        return new AccountStorageConfig(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }

    public createNetwork(scope: Construct, id: string, props: AccountNetworkProperties): AccountNetwork {
        return new AccountNetwork(scope, id, {
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

    public createUser(scope: Construct, id: string, props: ScimUserProperties): ScimUser {
        return new ScimUser(scope, id, {
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

    public createInstancePool(scope: Construct, id: string, props: InstancePoolProperties): InstancePool {
        return new InstancePool(scope, id, {
            ...props,
            serviceToken: this.serviceToken
        });
    }
}

export class DatabricksDeployLambdaImport extends IDatabricksDeployLambda {
    constructor(scope: Construct, id: string, serviceToken: string) {
        super(scope, id);
        this.serviceToken = serviceToken;
    }
}

export class DatabricksDeployLambda extends IDatabricksDeployLambda {

    readonly props: CustomDeployLambdaProps;
    readonly lambda: aws_lambda.IFunction;
    readonly lambdaRole: aws_iam.IRole;

    constructor(scope: Construct, id: string, props: CustomDeployLambdaProps) {
        super(scope, id);
        this.props = props;

        const dockerImageCode = this.props.lambdaCode || DockerImage.generate(this.props.lambdaVersion);

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
                LAMBDA_METHOD: "cfn-deploy",
                USER_PARAM: props.databricksUserParam || "/databricks/deploy/user",
                PASS_PARAM: props.databricksPassParam || "/databricks/deploy/password",
                ACCOUNT_PARAM: props.databricksAccountParam || "/databricks/account-id"
            },
            logRetention: aws_logs.RetentionDays.THREE_MONTHS,
        });
        this.serviceToken = this.lambda.functionArn;
    }

    public static fromServiceToken(scope: Construct, id: string, serviceToken: string): IDatabricksDeployLambda {
        return new DatabricksDeployLambdaImport(scope, id, serviceToken);
    }
}
