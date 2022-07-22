# flake8: noqa 401
def test_cluster():
    from databricks_cdk.resources.clusters import cluster


def test_cluster_permissions():
    from databricks_cdk.resources.permissions import cluster_permissions


def test_credentials():
    from databricks_cdk.resources.account import credentials


def test_dbfs_file():
    from databricks_cdk.resources.dbfs import dbfs_file


def test_group():
    from databricks_cdk.resources.groups import group


def test_cnf_handler():
    from databricks_cdk.resources.handler import handler


def test_root_handler():
    from databricks_cdk.handler import handler


def test_instance_profile():
    from databricks_cdk.resources.instance_profiles import instance_profile


def test_job():
    from databricks_cdk.resources.jobs import job


def test_networks():
    from databricks_cdk.resources.account import networks


def test_secret():
    from databricks_cdk.resources.secrets import secret


def test_secret_scope():
    from databricks_cdk.resources.secrets import secret_scope


def test_storage_config():
    from databricks_cdk.resources.account import storage_config


def test_user():
    from databricks_cdk.resources.scim import user


def test_utils():
    from databricks_cdk import utils


def test_workspace():
    from databricks_cdk.resources.account import workspace


def test_instance_pools():
    from databricks_cdk.resources.instance_pools import instance_pools
