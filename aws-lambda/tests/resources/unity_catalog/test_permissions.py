from databricks_cdk.resources.unity_catalog.permissions import (
    PermissionDiff,
    Permissions,
    PermissionsDiff,
    PermissionsList,
    create_diff,
)


def test_create_diff_empty():
    current = PermissionsList(privilege_assignments=[])
    new = PermissionsList(privilege_assignments=[])
    result = create_diff(current, new)
    assert len(result.changes) == 0


def test_create_diff_same():
    current = PermissionsList(privilege_assignments=[Permissions(principal="test", privileges=["SELECT"])])
    new = PermissionsList(privilege_assignments=[Permissions(principal="test", privileges=["SELECT"])])
    result = create_diff(current, new)
    assert len(result.changes) == 0


def test_create_diff():
    current = PermissionsList(privilege_assignments=[Permissions(principal="test", privileges=["SELECT", "USAGE"])])
    new = PermissionsList(privilege_assignments=[Permissions(principal="test", privileges=["SELECT", "INSERT"])])
    result = create_diff(current, new)

    assert result == PermissionsDiff(changes=[PermissionDiff(principal="test", add=["INSERT"], remove=["USAGE"])])


def test_create_diff_remove():
    current = PermissionsList(privilege_assignments=[Permissions(principal="test", privileges=["SELECT", "USAGE"])])
    new = PermissionsList(privilege_assignments=[])
    result = create_diff(current, new)

    assert result == PermissionsDiff(changes=[PermissionDiff(principal="test", remove=["SELECT", "USAGE"])])


def test_create_diff_add():
    current = PermissionsList(privilege_assignments=[])
    new = PermissionsList(privilege_assignments=[Permissions(principal="test", privileges=["SELECT", "USAGE"])])
    result = create_diff(current, new)

    assert result == PermissionsDiff(changes=[PermissionDiff(principal="test", add=["SELECT", "USAGE"])])
