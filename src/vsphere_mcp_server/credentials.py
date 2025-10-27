"""Credential management for vSphere MCP server - Docker/Environment version."""

import os
from typing import Tuple, Optional, NamedTuple


class VCenterCredentials(NamedTuple):
    """Modello per le credenziali vCenter."""
    hostname: str
    username: str
    password: str
    insecure: bool = False


def extract_domain(hostname: str) -> str:
    """Extract domain from FQDN."""
    parts = hostname.split(".")
    if len(parts) > 2:
        return ".".join(parts[1:])
    return hostname


def get_credentials(hostname: str) -> Tuple[str, str]:
    """Get credentials for vSphere host from environment variables."""
    # Try to get credentials from environment variables first
    env_creds = get_vcenter_credentials(hostname)
    if env_creds:
        return env_creds.username, env_creds.password
    
    # Fallback: prompt for credentials (for non-Docker environments)
    return _prompt_for_credentials(hostname)


def get_vcenter_credentials(hostname: str) -> Optional[VCenterCredentials]:
    """
    Recupera le credenziali vCenter dalle variabili d'ambiente.
    """
    # Read environment variables defined in the .env file
    host = os.environ.get('VCENTER_HOST')
    user = os.environ.get('VCENTER_USER')
    password = os.environ.get('VCENTER_PASSWORD')
    # Read the INSECURE variable and convert it to boolean
    insecure = os.environ.get('INSECURE', 'False').lower() in ('true', '1', 't')

    if host and user and password:
        return VCenterCredentials(
            hostname=host,
            username=user,
            password=password,
            insecure=insecure
        )
    
    return None


def _prompt_for_credentials(hostname: str) -> Tuple[str, str]:
    """Prompt for credentials using input() - fallback for non-Docker environments."""
    print(f"Credenziali non trovate nelle variabili d'ambiente per {hostname}")
    username = input("Username: ")
    password = input("Password: ")
    return username, password


def clear_credentials(hostname: str) -> bool:
    """Clear stored credentials for domain - placeholder for Docker environment."""
    # In Docker environment, credentials are managed via environment variables
    # This function is kept for compatibility but doesn't do anything
    return True
