"""
Permission utility functions for RBAC
"""
from typing import List, Set, Optional, Dict
from models import User, Role


async def get_user_permissions(user: User) -> Dict:
    """
    Calculate user's effective permissions by combining role permissions
    and user-specific overrides.

    Returns:
        dict with keys: is_admin, can_manage_datasets, can_run_query,
                       accessible_datasets, all_datasets
    """
    # Start with role-based permissions
    is_admin = False
    can_manage_datasets = False
    can_run_query = False
    accessible_datasets: Set[str] = set()
    all_datasets = False

    # Aggregate permissions from all assigned roles
    if user.role_ids:
        from bson import ObjectId
        role_object_ids = [ObjectId(rid) for rid in user.role_ids]
        roles = await Role.find({"_id": {"$in": role_object_ids}}).to_list()

        for role in roles:
            if role.is_admin:
                is_admin = True
            if role.can_manage_datasets:
                can_manage_datasets = True
            if role.can_run_query:
                can_run_query = True
            if role.all_datasets:
                all_datasets = True

            # Collect dataset permissions
            accessible_datasets.update(role.dataset_permissions)

    # Apply user-specific overrides (custom permissions)
    # Only admin flag and dataset access are user-level overrides
    # Feature permissions (can_manage_datasets, can_run_query) come from roles only
    if user.is_admin:
        is_admin = True
    if user.all_datasets:
        all_datasets = True

    # Admin users should have access to all datasets automatically
    if is_admin:
        all_datasets = True

    # User-specific dataset access
    accessible_datasets.update(user.dataset_access)

    return {
        "is_admin": is_admin,
        "can_manage_datasets": can_manage_datasets,
        "can_run_query": can_run_query,
        "all_datasets": all_datasets,
        "accessible_datasets": list(accessible_datasets)
    }


async def can_access_dataset(user: User, dataset_id: str) -> bool:
    """Check if user can access a specific dataset"""
    perms = await get_user_permissions(user)
    
    # Admin or all_datasets flag
    if perms["is_admin"] or perms["all_datasets"]:
        return True
    
    # Check specific dataset access
    return dataset_id in perms["accessible_datasets"]


async def has_permission(user: User, permission: str) -> bool:
    """
    Check if user has a specific permission
    
    Args:
        user: User object
        permission: Permission name (e.g., 'can_run_query', 'can_manage_datasets')
    
    Returns:
        bool: True if user has the permission
    """
    perms = await get_user_permissions(user)
    
    # Admin has all permissions
    if perms["is_admin"]:
        return True
    
    # Check specific permission
    return perms.get(permission, False)
