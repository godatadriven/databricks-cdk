from pydantic import BaseModel


class Permission(BaseModel):
    permission_level: str


class UserPermission(Permission):
    user_name: str


class GroupPermission(Permission):
    group_name: str


class ServicePrincipalPermission(Permission):
    service_principal_name: str
