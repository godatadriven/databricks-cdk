from pydantic import BaseModel


class User(BaseModel):
    user_name: str


class Group(BaseModel):
    group_name: str


class ServicePrincipal(BaseModel):
    service_principal_name: str


class UserPermission(User):
    permission_level: str


class GroupPermission(Group):
    permission_level: str


class ServicePrincipalPermission(ServicePrincipal):
    permission_level: str
