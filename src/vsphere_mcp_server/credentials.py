"""Credential management for vSphere MCP server - Docker/Environment version."""

import os
from typing import List, Tuple, Optional, NamedTuple


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


def get_vcenter_hosts() -> List[str]:
    """Return the list of configured vCenter hostnames.

    Reads ``VCENTER_HOSTS`` (comma-separated, first entry is the primary/default).
    Falls back to the single-host ``VCENTER_HOST`` for backward compatibility.
    Returns an empty list if neither is set.

    Credentials (``VCENTER_USER`` / ``VCENTER_PASSWORD``) are shared across all
    listed hosts, so multiple vCenters that use the same login need only be
    added here.
    """
    hosts_raw = os.environ.get('VCENTER_HOSTS')
    if hosts_raw:
        hosts = [h.strip() for h in hosts_raw.split(',') if h.strip()]
        if hosts:
            return hosts

    single = os.environ.get('VCENTER_HOST')
    if single and single.strip():
        return [single.strip()]

    return []


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

    Username/password/INSECURE are shared across all configured vCenters, so the
    returned credentials are bound to whichever ``hostname`` is asked for rather
    than to a single ``VCENTER_HOST``. This is what lets the same login target
    more than one vCenter (see :func:`get_vcenter_hosts`).
    """
    user = os.environ.get('VCENTER_USER')
    password = os.environ.get('VCENTER_PASSWORD')
    # Read the INSECURE variable and convert it to boolean
    insecure = os.environ.get('INSECURE', 'False').lower() in ('true', '1', 't')

    # Fall back to the first configured host if the caller didn't specify one.
    if not hostname:
        hosts = get_vcenter_hosts()
        hostname = hosts[0] if hosts else None

    if hostname and user and password:
        return VCenterCredentials(
            hostname=hostname,
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
