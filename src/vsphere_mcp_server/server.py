"""vSphere MCP Server - Main server implementation - Docker/Environment version.

Multi-vCenter aware: configure one or more vCenters via ``VCENTER_HOSTS``
(comma-separated) sharing a single ``VCENTER_USER`` / ``VCENTER_PASSWORD``.
Inventory/list tools aggregate across every configured vCenter; VM-targeted
tools automatically locate the VM on whichever vCenter owns it. Passing an
explicit ``hostname`` to any tool restricts it to that single vCenter.
"""

import os
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional, Tuple

from mcp.server.fastmcp import FastMCP

from .credentials import get_vcenter_hosts
from .vsphere_client import VSphereClient

# Initialize MCP server
mcp = FastMCP("vSphere MCP Server")

_NO_VCENTER_MSG = (
    "Error: No vCenter configured. Set VCENTER_HOSTS (comma-separated) or "
    "VCENTER_HOST in the environment."
)


def _handle_error(e: Exception, operation: str) -> str:
    """Handle errors consistently across all tools."""
    error_msg = str(e)

    if "Authentication failed" in error_msg:
        return (
            f"Authentication failed for {operation}. "
            "Check your environment variables (VCENTER_HOSTS, VCENTER_USER, VCENTER_PASSWORD)."
        )
    if "Connection" in error_msg or "timeout" in error_msg.lower():
        return f"Connection failed for {operation}. Check network connectivity and hostname."
    return f"Error in {operation}: {error_msg}"


# ---------------------------------------------------------------------------
# Multi-vCenter helpers
# ---------------------------------------------------------------------------

def _resolve_hosts(hostname: Optional[str]) -> List[str]:
    """Return the hosts to act on: the explicit one if given, else all configured."""
    if hostname:
        return [hostname]
    return get_vcenter_hosts()


def _run_per_host(host_workers: Dict[str, Callable[[VSphereClient], object]]) -> Dict[str, Tuple[object, Optional[Exception]]]:
    """Run each host's worker concurrently with its own client.

    ``host_workers`` maps hostname -> ``worker(client)``. Returns a dict of
    hostname -> ``(result, error)``. Each client is opened and closed per call;
    failures are isolated per host (one vCenter being down doesn't fail others).
    """
    if not host_workers:
        return {}

    def run(host: str, worker: Callable[[VSphereClient], object]) -> object:
        client = VSphereClient(host)
        try:
            return worker(client)
        finally:
            client.close()

    collected: Dict[str, Tuple[object, Optional[Exception]]] = {}
    with ThreadPoolExecutor(max_workers=min(len(host_workers), 8)) as executor:
        futures = {executor.submit(run, host, worker): host for host, worker in host_workers.items()}
        for future in as_completed(futures):
            host = futures[future]
            try:
                collected[host] = (future.result(), None)
            except Exception as exc:  # noqa: BLE001 - surfaced via _handle_error
                collected[host] = (None, exc)
    return collected


def _for_each_host(hosts: List[str], worker: Callable[[VSphereClient], object]) -> List[Tuple[str, object, Optional[Exception]]]:
    """Run the same worker across every host in parallel, preserving host order.

    Returns a list of ``(host, result, error)`` tuples in the original order.
    """
    collected = _run_per_host({host: worker for host in hosts})
    return [(host, collected[host][0], collected[host][1]) for host in hosts]


def _run_aggregate(hostname: Optional[str], worker: Callable[[VSphereClient], str], operation: str) -> str:
    """Run a read/inventory worker across the resolved vCenters and merge output.

    Per-host section headers (``=== host ===``) are added only when more than one
    vCenter is in play, keeping single-vCenter output clean.
    """
    hosts = _resolve_hosts(hostname)
    if not hosts:
        return _NO_VCENTER_MSG

    results = _for_each_host(hosts, worker)
    multi = len(hosts) > 1
    parts: List[str] = []
    for host, res, err in results:
        body = _handle_error(err, operation) if err is not None else (res if res else "")
        parts.append(f"=== {host} ===\n{body}" if multi else str(body))
    return "\n\n".join(p for p in parts if p)


def _run_locate(
    hostname: Optional[str],
    worker: Callable[[VSphereClient], Optional[str]],
    operation: str,
    not_found_msg: str,
) -> str:
    """Find an object (by id) on whichever vCenter has it.

    ``worker(client)`` should return a formatted string when the object exists on
    that host, or ``None`` when it doesn't. The first host that returns a value
    wins; if none do, ``not_found_msg`` is returned (or the error if every host
    failed).
    """
    hosts = _resolve_hosts(hostname)
    if not hosts:
        return _NO_VCENTER_MSG

    results = _for_each_host(hosts, worker)
    multi = len(hosts) > 1
    errors: List[Exception] = []
    for host, res, err in results:
        if err is not None:
            errors.append(err)
            continue
        if res:
            return f"[{host}]\n{res}" if multi else res
    if errors and len(errors) == len(hosts):
        return _handle_error(errors[0], operation)
    return not_found_msg


def _resolve_vm(vm_id: str, hostname: Optional[str] = None) -> Tuple[str, str]:
    """Locate a VM across configured vCenters.

    Accepts either a ``vm-...`` id or a VM name (case-insensitive) and returns
    ``(host, vm_id)`` for the owning vCenter. Raises ``ValueError`` if not found.
    """
    hosts = _resolve_hosts(hostname)
    if not hosts:
        raise ValueError("No vCenter configured. Set VCENTER_HOSTS (or VCENTER_HOST).")

    is_id = vm_id.startswith("vm-")
    # Explicit host + real id: nothing to look up.
    if hostname and is_id:
        return hostname, vm_id

    inventories = _for_each_host(hosts, lambda c: c.get("vcenter/vm").get("value", []))
    errors: List[Exception] = []
    for host, vms, err in inventories:
        if err is not None:
            errors.append(err)
            continue
        for vm in vms or []:
            if is_id and vm.get("vm") == vm_id:
                return host, vm_id
            if not is_id and vm.get("name", "").lower() == vm_id.lower():
                return host, vm.get("vm")

    if errors and len(errors) == len(hosts):
        raise errors[0]
    raise ValueError(f"Virtual machine '{vm_id}' not found on any configured vCenter")


def _vm_index(hosts: List[str]) -> Tuple[Dict[str, Tuple[str, str]], Dict[str, str]]:
    """Build lookup maps across all hosts for bulk operations.

    Returns ``(by_name, by_id)`` where ``by_name`` maps lowercased VM name ->
    ``(host, vm_id)`` and ``by_id`` maps ``vm-...`` id -> host.
    """
    inventories = _for_each_host(hosts, lambda c: c.get("vcenter/vm").get("value", []))
    by_name: Dict[str, Tuple[str, str]] = {}
    by_id: Dict[str, str] = {}
    for host, vms, err in inventories:
        if err is not None:
            continue
        for vm in vms or []:
            name = vm.get("name", "")
            vid = vm.get("vm")
            if name and name.lower() not in by_name:
                by_name[name.lower()] = (host, vid)
            if vid:
                by_id[vid] = host
    return by_name, by_id


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

@mcp.tool()
def list_vcenters() -> str:
    """List the configured vCenter servers.

    Inventory tools query all of these by default; VM-targeted tools auto-locate
    the VM across them. Pass a specific one as the ``hostname`` argument to any
    tool to restrict it to that vCenter.
    """
    hosts = get_vcenter_hosts()
    if not hosts:
        return _NO_VCENTER_MSG
    lines = [f"Configured vCenters ({len(hosts)}):", ""]
    for i, host in enumerate(hosts):
        marker = " (primary/default)" if i == 0 else ""
        lines.append(f"• {host}{marker}")
    return "\n".join(lines)


# VM Management Tools
@mcp.tool()
def list_vms(hostname: str = None) -> str:
    """List all virtual machines with basic information.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        response = client.get("vcenter/vm")
        vms = response.get("value", [])

        if not vms:
            return "No virtual machines found"

        result = f"Found {len(vms)} virtual machines:\n\n"
        for vm in vms:
            result += f"• {vm.get('name', 'Unknown')} (ID: {vm.get('vm')})\n"
            result += f"  Power State: {vm.get('power_state', 'Unknown')}\n"
            result += f"  CPU Count: {vm.get('cpu_count', 'Unknown')}\n"
            result += f"  Memory: {vm.get('memory_size_MiB', 'Unknown')} MiB\n\n"

        return result.strip()

    return _run_aggregate(hostname, worker, "listing VMs")


@mcp.tool()
def get_vm_details(vm_id: str, hostname: str = None) -> str:
    """Get detailed information about a specific virtual machine.

    Args:
        vm_id: Virtual machine ID or name
        hostname: vSphere hostname (optional; auto-locates the VM across configured vCenters)
    """
    try:
        host, resolved_id = _resolve_vm(vm_id, hostname)
    except (ConnectionError, ValueError, KeyError) as e:
        return _handle_error(e, f"getting VM {vm_id} details")

    def worker(client: VSphereClient) -> str:
        response = client.get(f"vcenter/vm/{resolved_id}")
        vm = response.get("value", {})

        if not vm:
            return f"Virtual machine {resolved_id} not found"

        result = f"VM Details: {vm.get('name', 'Unknown')}\n"
        result += f"ID: {resolved_id}\n"
        result += f"Power State: {vm.get('power_state', 'Unknown')}\n"
        result += f"CPU Count: {vm.get('cpu', {}).get('count', 'Unknown')}\n"
        result += f"Memory: {vm.get('memory', {}).get('size_MiB', 'Unknown')} MiB\n"
        result += f"Guest OS: {vm.get('guest_OS', 'Unknown')}\n"
        result += (
            f"Hardware Version: {vm.get('hardware', {}).get('version', 'Unknown')}\n"
        )

        # Network info
        nics = vm.get("nics", [])
        if nics:
            result += "\nNetwork Interfaces:\n"
            for i, nic in enumerate(nics):
                network_name = "Unknown"
                if isinstance(nic, dict):
                    backing = nic.get("backing", {})
                    if isinstance(backing, dict):
                        network_name = backing.get("network_name", "Unknown")
                result += f"  NIC {i}: {network_name}\n"

        return result

    return _run_aggregate(host, worker, f"getting VM {vm_id} details")


@mcp.tool()
def power_on_vm(vm_id: str, hostname: str = None) -> str:
    """Power on a virtual machine.

    Args:
        vm_id: Virtual machine ID or name
        hostname: vSphere hostname (optional; auto-locates the VM across configured vCenters)
    """
    try:
        host, resolved_id = _resolve_vm(vm_id, hostname)
    except (ConnectionError, ValueError, KeyError) as e:
        return _handle_error(e, f"powering on VM {vm_id}")

    def worker(client: VSphereClient) -> str:
        client.post(f"vcenter/vm/{resolved_id}/power/start")
        return f"Power on initiated for VM {resolved_id} on {host}"

    return _run_aggregate(host, worker, f"powering on VM {vm_id}")


@mcp.tool()
def power_off_vm(vm_id: str, hostname: str = None) -> str:
    """Power off a virtual machine.

    Args:
        vm_id: Virtual machine ID or name
        hostname: vSphere hostname (optional; auto-locates the VM across configured vCenters)
    """
    try:
        host, resolved_id = _resolve_vm(vm_id, hostname)
    except (ConnectionError, ValueError, KeyError) as e:
        return _handle_error(e, f"powering off VM {vm_id}")

    def worker(client: VSphereClient) -> str:
        client.post(f"vcenter/vm/{resolved_id}/power/stop")
        return f"Power off initiated for VM {resolved_id} on {host}"

    return _run_aggregate(host, worker, f"powering off VM {vm_id}")


# Infrastructure Tools
@mcp.tool()
def list_hosts(hostname: str = None) -> str:
    """List all ESXi hosts.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        response = client.get("vcenter/host")
        hosts = response.get("value", [])

        if not hosts:
            return "No ESXi hosts found"

        result = f"Found {len(hosts)} ESXi hosts:\n\n"
        for host in hosts:
            result += f"• {host.get('name', 'Unknown')} (ID: {host.get('host')})\n"
            result += f"  Connection State: {host.get('connection_state', 'Unknown')}\n"
            result += f"  Power State: {host.get('power_state', 'Unknown')}\n\n"

        return result.strip()

    return _run_aggregate(hostname, worker, "listing hosts")


@mcp.tool()
def get_host_details(host_id: str, hostname: str = None) -> str:
    """Get detailed information about an ESXi host.

    Args:
        host_id: ESXi host ID
        hostname: vSphere hostname (optional; locates the host across configured vCenters)
    """
    def worker(client: VSphereClient) -> Optional[str]:
        response = client.get(f"vcenter/host/{host_id}")
        host = response.get("value", {})
        if not host:
            return None

        result = f"Host Details: {host.get('name', 'Unknown')}\n"
        result += f"ID: {host_id}\n"
        result += f"Connection State: {host.get('connection_state', 'Unknown')}\n"
        result += f"Power State: {host.get('power_state', 'Unknown')}\n"
        return result

    return _run_locate(
        hostname, worker, f"getting host {host_id} details", f"ESXi host {host_id} not found"
    )


@mcp.tool()
def list_datacenters(hostname: str = None) -> str:
    """List all datacenters.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        response = client.get("vcenter/datacenter")
        datacenters = response.get("value", [])

        if not datacenters:
            return "No datacenters found"

        result = f"Found {len(datacenters)} datacenters:\n\n"
        for dc in datacenters:
            result += f"• {dc.get('name', 'Unknown')} (ID: {dc.get('datacenter')})\n"

        return result.strip()

    return _run_aggregate(hostname, worker, "listing datacenters")


@mcp.tool()
def get_datacenter_details(datacenter_id: str, hostname: str = None) -> str:
    """Get detailed information about a datacenter.

    Args:
        datacenter_id: Datacenter ID
        hostname: vSphere hostname (optional; locates the datacenter across configured vCenters)
    """
    def worker(client: VSphereClient) -> Optional[str]:
        response = client.get(f"vcenter/datacenter/{datacenter_id}")
        dc = response.get("value", {})
        if not dc:
            return None

        result = f"Datacenter Details: {dc.get('name', 'Unknown')}\n"
        result += f"ID: {datacenter_id}\n"
        return result

    return _run_locate(
        hostname,
        worker,
        f"getting datacenter {datacenter_id} details",
        f"Datacenter {datacenter_id} not found",
    )


@mcp.tool()
def list_datastores(hostname: str = None) -> str:
    """List all datastores with capacity information.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        response = client.get("vcenter/datastore")
        datastores = response.get("value", [])

        if not datastores:
            return "No datastores found"

        result = f"Found {len(datastores)} datastores:\n\n"
        for ds in datastores:
            capacity = ds.get("capacity", 0)
            free_space = ds.get("free_space", 0)
            used_space = capacity - free_space
            used_pct = (used_space / capacity * 100) if capacity > 0 else 0

            result += f"• {ds.get('name', 'Unknown')} (ID: {ds.get('datastore')})\n"
            result += f"  Type: {ds.get('type', 'Unknown')}\n"
            result += f"  Capacity: {capacity / (1024**3):.1f} GB\n"
            result += f"  Used: {used_space / (1024**3):.1f} GB ({used_pct:.1f}%)\n"
            result += f"  Free: {free_space / (1024**3):.1f} GB\n\n"

        return result.strip()

    return _run_aggregate(hostname, worker, "listing datastores")


@mcp.tool()
def get_datastore_details(datastore_id: str, hostname: str = None) -> str:
    """Get detailed information about a datastore.

    Args:
        datastore_id: Datastore ID
        hostname: vSphere hostname (optional; locates the datastore across configured vCenters)
    """
    def worker(client: VSphereClient) -> Optional[str]:
        response = client.get(f"vcenter/datastore/{datastore_id}")
        ds = response.get("value", {})
        if not ds:
            return None

        capacity = ds.get("capacity", 0) or 0
        free_space = ds.get("free_space", 0) or 0

        # Ensure values are positive
        if capacity <= 0 or free_space < 0:
            result = f"Datastore Details: {ds.get('name', 'Unknown')}\n"
            result += f"ID: {datastore_id}\n"
            result += f"Type: {ds.get('type', 'Unknown')}\n"
            result += "Capacity information not available or invalid\n"
            return result

        used_space = capacity - free_space
        used_pct = (used_space / capacity * 100) if capacity > 0 else 0

        result = f"Datastore Details: {ds.get('name', 'Unknown')}\n"
        result += f"ID: {datastore_id}\n"
        result += f"Type: {ds.get('type', 'Unknown')}\n"
        result += f"Capacity: {capacity / (1024**3):.1f} GB\n"
        result += f"Used: {used_space / (1024**3):.1f} GB ({used_pct:.1f}%)\n"
        result += f"Free: {free_space / (1024**3):.1f} GB\n"
        return result

    return _run_locate(
        hostname,
        worker,
        f"getting datastore {datastore_id} details",
        f"Datastore {datastore_id} not found",
    )


# Organization Tools
@mcp.tool()
def list_folders(folder_type: str = "VIRTUAL_MACHINE", hostname: str = None) -> str:
    """List folders by type.

    Args:
        folder_type: Folder type (VIRTUAL_MACHINE, HOST, DATACENTER, DATASTORE, NETWORK)
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        response = client.get(f"vcenter/folder?filter.type={folder_type}")
        folders = response.get("value", [])

        if not folders:
            return f"No {folder_type} folders found"

        result = f"Found {len(folders)} {folder_type} folders:\n\n"
        for folder in folders:
            result += (
                f"• {folder.get('name', 'Unknown')} (ID: {folder.get('folder')})\n"
            )

        return result.strip()

    return _run_aggregate(hostname, worker, f"listing {folder_type} folders")


@mcp.tool()
def get_folder_details(folder_id: str, hostname: str = None) -> str:
    """Get detailed information about a folder.

    Args:
        folder_id: Folder ID
        hostname: vSphere hostname (optional; locates the folder across configured vCenters)
    """
    def worker(client: VSphereClient) -> Optional[str]:
        response = client.get(f"vcenter/folder/{folder_id}")
        folder = response.get("value", {})
        if not folder:
            return None

        result = f"Folder Details: {folder.get('name', 'Unknown')}\n"
        result += f"ID: {folder_id}\n"
        result += f"Type: {folder.get('type', 'Unknown')}\n"
        return result

    return _run_locate(
        hostname,
        worker,
        f"getting folder {folder_id} details",
        f"Folder {folder_id} not found or access denied (may be a system folder)",
    )


# Network Tools
@mcp.tool()
def list_networks(hostname: str = None) -> str:
    """List all networks with VLAN information.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        response = client.get("vcenter/network")
        networks = response.get("value", [])

        if not networks:
            return "No networks found"

        result = f"Found {len(networks)} networks:\n\n"
        for network in networks:
            name = network.get("name", "Unknown")
            result += f"• {name} (ID: {network.get('network')})\n"
            result += f"  Type: {network.get('type', 'Unknown')}\n"

            # Extract VLAN info from name
            vlan_match = re.search(r"v(\d+)-|VLAN(\d+)", name)
            if vlan_match:
                vlan_id = vlan_match.group(1) or vlan_match.group(2)
                result += f"  VLAN ID: {vlan_id}\n"

            result += "\n"

        return result.strip()

    return _run_aggregate(hostname, worker, "listing networks")


@mcp.tool()
def get_network_details(network_id: str, hostname: str = None) -> str:
    """Get detailed information about a network.

    Args:
        network_id: Network ID
        hostname: vSphere hostname (optional; locates the network across configured vCenters)
    """
    def worker(client: VSphereClient) -> Optional[str]:
        response = client.get(f"vcenter/network/{network_id}")
        network = response.get("value", {})
        if not network:
            return None

        name = network.get("name", "Unknown")
        result = f"Network Details: {name}\n"
        result += f"ID: {network_id}\n"
        result += f"Type: {network.get('type', 'Unknown')}\n"

        # Extract VLAN info from name
        vlan_match = re.search(r"v(\d+)-|VLAN(\d+)", name)
        if vlan_match:
            vlan_id = vlan_match.group(1) or vlan_match.group(2)
            result += f"VLAN ID: {vlan_id}\n"

        return result

    return _run_locate(
        hostname,
        worker,
        f"getting network {network_id} details",
        (
            f"Network {network_id} not found or is a distributed portgroup "
            "(not accessible via this API)"
        ),
    )


@mcp.tool()
def get_vlan_info(vlan_query: str, hostname: str = None) -> str:
    """Get information about a VLAN by name or VLAN ID.

    Args:
        vlan_query: VLAN name (e.g., v1306-MEL03-Secure-Management) or VLAN ID (e.g., 1306)
        hostname: vSphere hostname (optional; searches all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        response = client.get("vcenter/network")
        networks = response.get("value", [])

        if not networks:
            return "No networks found"

        matches = []

        # Search by name (partial match, case-insensitive)
        for network in networks:
            name = network.get("name", "")
            if vlan_query.lower() in name.lower():
                matches.append(network)

        # If no name matches and query is numeric, search by VLAN ID
        if not matches and vlan_query.isdigit():
            vlan_id = vlan_query
            for network in networks:
                name = network.get("name", "")
                vlan_match = re.search(r"v(\d+)-|VLAN(\d+)", name)
                if vlan_match:
                    extracted_vlan = vlan_match.group(1) or vlan_match.group(2)
                    if extracted_vlan == vlan_id:
                        matches.append(network)

        if not matches:
            return f"No VLAN found matching '{vlan_query}'"

        result = f"VLAN Search Results for '{vlan_query}':\n\n"

        for network in matches:
            name = network.get("name", "Unknown")
            result += f"• {name}\n"
            result += f"  Network ID: {network.get('network', 'Unknown')}\n"
            result += f"  Type: {network.get('type', 'Unknown')}\n"

            # Extract VLAN ID from name
            vlan_match = re.search(r"v(\d+)-|VLAN(\d+)", name)
            if vlan_match:
                vlan_id = vlan_match.group(1) or vlan_match.group(2)
                result += f"  VLAN ID: {vlan_id}\n"

            result += "\n"

        result += f"Found {len(matches)} matching network(s)"
        return result

    return _run_aggregate(hostname, worker, f"searching for VLAN '{vlan_query}'")


@mcp.tool()
def list_vlans(hostname: str = None) -> str:
    """Extract and list VLAN information from network names.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        response = client.get("vcenter/network")
        networks = response.get("value", [])

        if not networks:
            return "No networks found"

        vlans: Dict[str, List[str]] = {}
        for network in networks:
            name = network.get("name", "Unknown")
            vlan_match = re.search(r"v(\d+)-|VLAN(\d+)", name)
            if vlan_match:
                vlan_id = vlan_match.group(1) or vlan_match.group(2)
                if vlan_id not in vlans:
                    vlans[vlan_id] = []
                vlans[vlan_id].append(name)

        if not vlans:
            return "No VLAN information found in network names"

        result = f"Found {len(vlans)} VLANs:\n\n"
        for vlan_id in sorted(vlans.keys(), key=int):
            result += f"VLAN {vlan_id}:\n"
            for network_name in vlans[vlan_id]:
                result += f"  • {network_name}\n"
            result += "\n"

        return result.strip()

    return _run_aggregate(hostname, worker, "extracting VLAN information")


@mcp.tool()
def get_vm_disk_usage(hostname: str = None) -> str:
    """Get disk usage information for all VMs.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        # Get all VMs
        response = client.get("vcenter/vm")
        vms = response.get("value", [])

        if not vms:
            return "No virtual machines found"

        result = f"Disk Usage Report for {len(vms)} VMs:\n\n"

        for vm in vms:
            vm_id = vm.get('vm')
            vm_name = vm.get('name', 'Unknown')

            try:
                # Get detailed VM information including disks
                vm_details = client.get(f"vcenter/vm/{vm_id}")
                vm_data = vm_details.get("value", {})

                # Get disk information
                disks = vm_data.get("disks", [])

                result += f"• {vm_name} (ID: {vm_id})\n"

                if disks:
                    for i, disk in enumerate(disks):
                        capacity = disk.get("capacity", 0)
                        if capacity > 0:
                            # Convert bytes to GB
                            capacity_gb = capacity / (1024**3)
                            result += f"  Disk {i}: {capacity_gb:.1f} GB\n"
                        else:
                            result += f"  Disk {i}: Capacity not available\n"
                else:
                    result += "  No disk information available\n"

                result += "\n"

            except Exception as e:
                result += f"• {vm_name} (ID: {vm_id}) - Error getting disk info: {str(e)}\n\n"

        return result.strip()

    return _run_aggregate(hostname, worker, "getting VM disk usage")


@mcp.tool()
def get_vm_storage_info(hostname: str = None) -> str:
    """Get detailed storage information for all VMs including datastore usage.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        # Get all VMs
        response = client.get("vcenter/vm")
        vms = response.get("value", [])

        if not vms:
            return "No virtual machines found"

        result = f"Storage Information for {len(vms)} VMs:\n\n"

        for vm in vms:
            vm_id = vm.get('vm')
            vm_name = vm.get('name', 'Unknown')

            try:
                # Get detailed VM information
                vm_details = client.get(f"vcenter/vm/{vm_id}")
                vm_data = vm_details.get("value", {})

                result += f"• {vm_name} (ID: {vm_id})\n"

                # Get disk information
                disks = vm_data.get("disks", [])
                if disks:
                    for i, disk in enumerate(disks):
                        capacity = disk.get("capacity", 0)
                        if capacity > 0:
                            capacity_gb = capacity / (1024**3)
                            result += f"  Disk {i}: {capacity_gb:.1f} GB allocated\n"
                        else:
                            result += f"  Disk {i}: Size not available\n"
                else:
                    result += "  No disk information available\n"

                # Get datastore information
                datastores = vm_data.get("datastores", [])
                if datastores:
                    result += "  Datastores:\n"
                    for ds in datastores:
                        result += f"    - {ds}\n"

                result += "\n"

            except Exception as e:
                result += f"• {vm_name} (ID: {vm_id}) - Error: {str(e)}\n\n"

        result += "\nNote: For actual disk usage percentage, VMware Tools must be installed in VMs and vRealize Operations or similar tools are needed.\n"
        result += "This report shows allocated disk space, not actual usage."

        return result.strip()

    return _run_aggregate(hostname, worker, "getting VM storage information")


@mcp.tool()
def get_datastore_usage(hostname: str = None) -> str:
    """Get datastore usage information to identify potential storage issues.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        # Get all datastores
        response = client.get("vcenter/datastore")
        datastores = response.get("value", [])

        if not datastores:
            return "No datastores found"

        result = f"Datastore Usage Report:\n\n"
        high_usage_ds = []

        for ds in datastores:
            ds_id = ds.get('datastore')
            ds_name = ds.get('name', 'Unknown')
            capacity = ds.get("capacity", 0)
            free_space = ds.get("free_space", 0)

            if capacity > 0 and free_space >= 0:
                used_space = capacity - free_space
                used_pct = (used_space / capacity * 100) if capacity > 0 else 0

                result += f"• {ds_name}\n"
                result += f"  Capacity: {capacity / (1024**3):.1f} GB\n"
                result += f"  Used: {used_space / (1024**3):.1f} GB ({used_pct:.1f}%)\n"
                result += f"  Free: {free_space / (1024**3):.1f} GB\n"

                if used_pct > 90:
                    high_usage_ds.append(f"{ds_name} ({used_pct:.1f}%)")

                result += "\n"

        if high_usage_ds:
            result += f"⚠️  Datastores with >90% usage:\n"
            for ds in high_usage_ds:
                result += f"  - {ds}\n"
            result += "\n"

        result += "Note: This shows datastore usage, not individual VM disk usage."

        return result.strip()

    return _run_aggregate(hostname, worker, "getting datastore usage")


@mcp.tool()
def get_vm_performance_info(hostname: str = None) -> str:
    """Get performance information for all VMs including CPU, RAM, and network.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        # Get all VMs
        response = client.get("vcenter/vm")
        vms = response.get("value", [])

        if not vms:
            return "No virtual machines found"

        result = f"Performance Information for {len(vms)} VMs:\n\n"

        for vm in vms:
            vm_id = vm.get('vm')
            vm_name = vm.get('name', 'Unknown')
            power_state = vm.get('power_state', 'Unknown')

            try:
                # Get detailed VM information
                vm_details = client.get(f"vcenter/vm/{vm_id}")
                vm_data = vm_details.get("value", {})

                result += f"• {vm_name} (ID: {vm_id})\n"
                result += f"  Power State: {power_state}\n"

                if power_state == "POWERED_ON":
                    # CPU Information
                    cpu_info = vm_data.get("cpu", {})
                    if cpu_info:
                        cpu_count = cpu_info.get("count", "Unknown")
                        result += f"  CPU: {cpu_count} vCPUs\n"

                    # Memory Information
                    memory_info = vm_data.get("memory", {})
                    if memory_info:
                        memory_mb = memory_info.get("size_MiB", "Unknown")
                        if memory_mb != "Unknown":
                            memory_gb = memory_mb / 1024
                            result += f"  Memory: {memory_gb:.1f} GB ({memory_mb} MB)\n"
                        else:
                            result += f"  Memory: {memory_mb}\n"

                    # Network Information
                    nics = vm_data.get("nics", [])
                    if nics:
                        result += f"  Network Interfaces: {len(nics)}\n"
                        for i, nic in enumerate(nics):
                            if isinstance(nic, dict):
                                backing = nic.get("backing", {})
                                if isinstance(backing, dict):
                                    network_name = backing.get("network_name", "Unknown")
                                    result += f"    NIC {i}: {network_name}\n"

                    # Guest OS Information
                    guest_os = vm_data.get("guest_OS", "Unknown")
                    result += f"  Guest OS: {guest_os}\n"

                else:
                    result += f"  VM is {power_state.lower()} - performance data not available\n"

                result += "\n"

            except Exception as e:
                result += f"• {vm_name} (ID: {vm_id}) - Error: {str(e)}\n\n"

        result += "\nNote: This shows allocated resources, not actual usage. For real-time performance metrics, vRealize Operations or similar tools are needed."

        return result.strip()

    return _run_aggregate(hostname, worker, "getting VM performance information")


@mcp.tool()
def get_host_performance_info(hostname: str = None) -> str:
    """Get performance information for all ESXi hosts.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        # Get all hosts
        response = client.get("vcenter/host")
        hosts = response.get("value", [])

        if not hosts:
            return "No ESXi hosts found"

        result = f"Host Performance Information for {len(hosts)} hosts:\n\n"

        for host in hosts:
            host_id = host.get('host')
            host_name = host.get('name', 'Unknown')
            connection_state = host.get('connection_state', 'Unknown')
            power_state = host.get('power_state', 'Unknown')

            result += f"• {host_name} (ID: {host_id})\n"
            result += f"  Connection State: {connection_state}\n"
            result += f"  Power State: {power_state}\n"

            if connection_state == "CONNECTED":
                try:
                    # Get detailed host information
                    host_details = client.get(f"vcenter/host/{host_id}")
                    host_data = host_details.get("value", {})

                    # CPU Information
                    cpu_info = host_data.get("cpu", {})
                    if cpu_info:
                        cpu_count = cpu_info.get("count", "Unknown")
                        result += f"  CPU: {cpu_count} physical CPUs\n"

                    # Memory Information
                    memory_info = host_data.get("memory", {})
                    if memory_info:
                        memory_mb = memory_info.get("size_MiB", "Unknown")
                        if memory_mb != "Unknown":
                            memory_gb = memory_mb / 1024
                            result += f"  Memory: {memory_gb:.1f} GB ({memory_mb} MB)\n"
                        else:
                            result += f"  Memory: {memory_mb}\n"

                    # Network Information
                    nics = host_data.get("nics", [])
                    if nics:
                        result += f"  Network Interfaces: {len(nics)}\n"
                        for i, nic in enumerate(nics):
                            if isinstance(nic, dict):
                                nic_name = nic.get("device", "Unknown")
                                result += f"    NIC {i}: {nic_name}\n"

                except Exception as e:
                    result += f"  Error getting detailed info: {str(e)}\n"
            else:
                result += f"  Host is {connection_state.lower()} - detailed info not available\n"

            result += "\n"

        result += "\nNote: This shows host hardware configuration, not real-time performance metrics."

        return result.strip()

    return _run_aggregate(hostname, worker, "getting host performance information")


@mcp.tool()
def get_vms_with_high_resource_usage(hostname: str = None) -> str:
    """Get VMs that might have high resource usage based on configuration.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        # Get all VMs
        response = client.get("vcenter/vm")
        vms = response.get("value", [])

        if not vms:
            return "No virtual machines found"

        result = f"VMs with High Resource Configuration:\n\n"
        high_cpu_vms = []
        high_memory_vms = []

        for vm in vms:
            vm_id = vm.get('vm')
            vm_name = vm.get('name', 'Unknown')
            power_state = vm.get('power_state', 'Unknown')

            if power_state == "POWERED_ON":
                try:
                    # Get detailed VM information
                    vm_details = client.get(f"vcenter/vm/{vm_id}")
                    vm_data = vm_details.get("value", {})

                    # Check CPU
                    cpu_info = vm_data.get("cpu", {})
                    if cpu_info:
                        cpu_count = cpu_info.get("count", 0)
                        if cpu_count >= 8:  # VMs with 8+ vCPUs
                            high_cpu_vms.append(f"{vm_name} ({cpu_count} vCPUs)")

                    # Check Memory
                    memory_info = vm_data.get("memory", {})
                    if memory_info:
                        memory_mb = memory_info.get("size_MiB", 0)
                        if memory_mb >= 16384:  # VMs with 16GB+ RAM
                            memory_gb = memory_mb / 1024
                            high_memory_vms.append(f"{vm_name} ({memory_gb:.1f} GB)")

                except Exception:
                    continue

        if high_cpu_vms:
            result += "🔴 VMs with High CPU Configuration (8+ vCPUs):\n"
            for vm in high_cpu_vms:
                result += f"  - {vm}\n"
            result += "\n"

        if high_memory_vms:
            result += "🔴 VMs with High Memory Configuration (16GB+ RAM):\n"
            for vm in high_memory_vms:
                result += f"  - {vm}\n"
            result += "\n"

        if not high_cpu_vms and not high_memory_vms:
            result += "✅ No VMs found with high resource configuration.\n"

        result += "\nNote: This shows resource allocation, not actual usage. High allocation doesn't necessarily mean high usage."

        return result.strip()

    return _run_aggregate(hostname, worker, "getting VMs with high resource usage")


# Snapshot Management Tools
@mcp.tool()
def list_vm_snapshots(vm_id: str, hostname: str = None) -> str:
    """List all snapshots for a specific VM.

    Args:
        vm_id: Virtual machine ID or name
        hostname: vSphere hostname (optional; auto-locates the VM across configured vCenters)
    """
    try:
        host, resolved_id = _resolve_vm(vm_id, hostname)
    except (ConnectionError, ValueError, KeyError) as e:
        return _handle_error(e, f"listing snapshots for VM {vm_id}")

    def worker(client: VSphereClient) -> str:
        response = client.get(f"vcenter/vm/{resolved_id}/snapshot")
        snapshots = response.get("value", [])

        if not snapshots:
            return f"No snapshots found for VM {resolved_id}"

        result = f"Snapshots for VM {resolved_id}:\n\n"
        for snapshot in snapshots:
            result += f"• {snapshot.get('name', 'Unknown')} (ID: {snapshot.get('snapshot')})\n"
            result += f"  Created: {snapshot.get('create_time', 'Unknown')}\n"
            result += f"  State: {snapshot.get('state', 'Unknown')}\n\n"

        return result.strip()

    return _run_aggregate(host, worker, f"listing snapshots for VM {vm_id}")


@mcp.tool()
def create_vm_snapshot(vm_id: str, snapshot_name: str, description: str = "", hostname: str = None) -> str:
    """Create a snapshot for a specific VM.

    Args:
        vm_id: Virtual machine ID or name
        snapshot_name: Name for the snapshot
        description: Description for the snapshot (optional)
        hostname: vSphere hostname (optional; auto-locates the VM across configured vCenters)
    """
    try:
        host, resolved_id = _resolve_vm(vm_id, hostname)
    except (ConnectionError, ValueError, KeyError) as e:
        return _handle_error(e, f"creating snapshot for VM {vm_id}")

    def worker(client: VSphereClient) -> str:
        snapshot_data = {
            "name": snapshot_name,
            "description": description,
            "memory": True,
            "quiesce": True
        }
        client.post(f"vcenter/vm/{resolved_id}/snapshot", snapshot_data)
        return f"Snapshot '{snapshot_name}' created successfully for VM {resolved_id} on {host}"

    return _run_aggregate(host, worker, f"creating snapshot for VM {vm_id}")


# Template Management Tools
@mcp.tool()
def list_templates(hostname: str = None) -> str:
    """List all VM templates.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        # Get all VMs and filter for templates
        response = client.get("vcenter/vm")
        vms = response.get("value", [])

        templates = []
        for vm in vms:
            # Check if VM is a template (this might need adjustment based on your vSphere setup)
            vm_name = vm.get('name', '')
            if 'template' in vm_name.lower() or vm.get('template', False):
                templates.append(vm)

        if not templates:
            return "No templates found"

        result = f"Found {len(templates)} templates:\n\n"
        for template in templates:
            result += f"• {template.get('name', 'Unknown')} (ID: {template.get('vm')})\n"
            result += f"  Power State: {template.get('power_state', 'Unknown')}\n"
            result += f"  Guest OS: {template.get('guest_OS', 'Unknown')}\n\n"

        return result.strip()

    return _run_aggregate(hostname, worker, "listing templates")


# Advanced Monitoring Tools
@mcp.tool()
def get_vm_events(vm_id: str, hostname: str = None) -> str:
    """Get recent events for a specific VM.

    Args:
        vm_id: Virtual machine ID or name
        hostname: vSphere hostname (optional; auto-locates the VM across configured vCenters)
    """
    try:
        host, resolved_id = _resolve_vm(vm_id, hostname)
    except (ConnectionError, ValueError, KeyError) as e:
        return _handle_error(e, f"getting events for VM {vm_id}")

    def worker(client: VSphereClient) -> str:
        try:
            response = client.get(f"vcenter/vm/{resolved_id}/events")
            events = response.get("value", [])

            if not events:
                return f"No recent events found for VM {resolved_id}"

            result = f"Recent Events for VM {resolved_id}:\n\n"
            for event in events[:10]:  # Show last 10 events
                result += f"• {event.get('event_type', 'Unknown')}\n"
                result += f"  Time: {event.get('time', 'Unknown')}\n"
                result += f"  Description: {event.get('description', 'No description')}\n\n"

            return result.strip()

        except Exception:
            return f"Events endpoint not available for VM {resolved_id}. This feature requires specific vSphere API permissions."

    return _run_aggregate(host, worker, f"getting events for VM {vm_id}")


@mcp.tool()
def get_alarms(hostname: str = None) -> str:
    """Get active alarms in the vSphere environment.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        try:
            response = client.get("vcenter/alarm")
            alarms = response.get("value", [])

            if not alarms:
                return "No active alarms found"

            result = f"Active Alarms ({len(alarms)}):\n\n"
            for alarm in alarms:
                result += f"• {alarm.get('name', 'Unknown')}\n"
                result += f"  Status: {alarm.get('status', 'Unknown')}\n"
                result += f"  Severity: {alarm.get('severity', 'Unknown')}\n"
                result += f"  Description: {alarm.get('description', 'No description')}\n\n"

            return result.strip()

        except Exception:
            return "Alarms endpoint not available. This feature requires specific vSphere API permissions."

    return _run_aggregate(hostname, worker, "getting alarms")


# Network Management Tools
@mcp.tool()
def get_port_groups(hostname: str = None) -> str:
    """Get all port groups in the vSphere environment.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        # Get port groups (this might be available through network endpoints)
        response = client.get("vcenter/network")
        networks = response.get("value", [])

        if not networks:
            return "No networks found"

        result = f"Network Port Groups ({len(networks)}):\n\n"
        for network in networks:
            result += f"• {network.get('name', 'Unknown')}\n"
            result += f"  Type: {network.get('type', 'Unknown')}\n"
            result += f"  ID: {network.get('network', 'Unknown')}\n\n"

        return result.strip()

    return _run_aggregate(hostname, worker, "getting port groups")


# Reporting and Analytics Tools
@mcp.tool()
def generate_vm_report(hostname: str = None) -> str:
    """Generate a comprehensive report of all VMs.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        # Get all VMs
        response = client.get("vcenter/vm")
        vms = response.get("value", [])

        if not vms:
            return "No virtual machines found"

        # Get datastores for storage info
        ds_response = client.get("vcenter/datastore")
        datastores = ds_response.get("value", [])

        # Get hosts for host info
        host_response = client.get("vcenter/host")
        hosts = host_response.get("value", [])

        result = f"=== vSphere VM Report ===\n"
        result += f"Generated: {os.popen('date').read().strip()}\n"
        result += f"Total VMs: {len(vms)}\n"
        result += f"Total Hosts: {len(hosts)}\n"
        result += f"Total Datastores: {len(datastores)}\n\n"

        # VM Summary
        powered_on = sum(1 for vm in vms if vm.get('power_state') == 'POWERED_ON')
        powered_off = len(vms) - powered_on

        result += f"=== VM Summary ===\n"
        result += f"Powered On: {powered_on}\n"
        result += f"Powered Off: {powered_off}\n\n"

        # Detailed VM List
        result += f"=== Detailed VM List ===\n"
        for vm in vms:
            vm_name = vm.get('name', 'Unknown')
            power_state = vm.get('power_state', 'Unknown')
            result += f"• {vm_name} - {power_state}\n"

        result += f"\n=== Datastore Summary ===\n"
        for ds in datastores:
            ds_name = ds.get('name', 'Unknown')
            capacity = ds.get("capacity", 0)
            free_space = ds.get("free_space", 0)
            if capacity > 0:
                used_pct = ((capacity - free_space) / capacity * 100)
                result += f"• {ds_name}: {used_pct:.1f}% used\n"

        return result.strip()

    return _run_aggregate(hostname, worker, "generating VM report")


@mcp.tool()
def get_resource_utilization_summary(hostname: str = None) -> str:
    """Get a summary of resource utilization across the vSphere environment.

    Args:
        hostname: vSphere hostname (optional; queries all configured vCenters if not provided)
    """
    def worker(client: VSphereClient) -> str:
        # Get all VMs
        vm_response = client.get("vcenter/vm")
        vms = vm_response.get("value", [])

        # Get all hosts
        host_response = client.get("vcenter/host")
        hosts = host_response.get("value", [])

        # Get all datastores
        ds_response = client.get("vcenter/datastore")
        datastores = ds_response.get("value", [])

        result = f"=== Resource Utilization Summary ===\n\n"

        # CPU Summary
        total_vcpus = 0
        total_physical_cpus = 0

        for vm in vms:
            if vm.get('power_state') == 'POWERED_ON':
                try:
                    vm_details = client.get(f"vcenter/vm/{vm.get('vm')}")
                    vm_data = vm_details.get("value", {})
                    cpu_info = vm_data.get("cpu", {})
                    if cpu_info:
                        total_vcpus += cpu_info.get("count", 0)
                except:
                    continue

        for host in hosts:
            if host.get('connection_state') == 'CONNECTED':
                try:
                    host_details = client.get(f"vcenter/host/{host.get('host')}")
                    host_data = host_details.get("value", {})
                    cpu_info = host_data.get("cpu", {})
                    if cpu_info:
                        total_physical_cpus += cpu_info.get("count", 0)
                except:
                    continue

        result += f"CPU Utilization:\n"
        result += f"  Total vCPUs allocated: {total_vcpus}\n"
        result += f"  Total physical CPUs: {total_physical_cpus}\n"
        if total_physical_cpus > 0:
            cpu_ratio = total_vcpus / total_physical_cpus
            result += f"  vCPU to Physical CPU ratio: {cpu_ratio:.2f}:1\n"
        result += "\n"

        # Memory Summary
        total_vm_memory = 0
        total_host_memory = 0

        for vm in vms:
            if vm.get('power_state') == 'POWERED_ON':
                try:
                    vm_details = client.get(f"vcenter/vm/{vm.get('vm')}")
                    vm_data = vm_details.get("value", {})
                    memory_info = vm_data.get("memory", {})
                    if memory_info:
                        total_vm_memory += memory_info.get("size_MiB", 0)
                except:
                    continue

        for host in hosts:
            if host.get('connection_state') == 'CONNECTED':
                try:
                    host_details = client.get(f"vcenter/host/{host.get('host')}")
                    host_data = host_details.get("value", {})
                    memory_info = host_data.get("memory", {})
                    if memory_info:
                        total_host_memory += memory_info.get("size_MiB", 0)
                except:
                    continue

        result += f"Memory Utilization:\n"
        result += f"  Total VM memory allocated: {total_vm_memory / 1024:.1f} GB\n"
        result += f"  Total host memory: {total_host_memory / 1024:.1f} GB\n"
        if total_host_memory > 0:
            memory_ratio = (total_vm_memory / total_host_memory) * 100
            result += f"  Memory overcommitment: {memory_ratio:.1f}%\n"
        result += "\n"

        # Storage Summary
        total_capacity = 0
        total_free = 0

        for ds in datastores:
            total_capacity += ds.get("capacity", 0)
            total_free += ds.get("free_space", 0)

        total_used = total_capacity - total_free
        used_percentage = (total_used / total_capacity * 100) if total_capacity > 0 else 0

        result += f"Storage Utilization:\n"
        result += f"  Total capacity: {total_capacity / (1024**3):.1f} GB\n"
        result += f"  Total used: {total_used / (1024**3):.1f} GB ({used_percentage:.1f}%)\n"
        result += f"  Total free: {total_free / (1024**3):.1f} GB\n"

        return result.strip()

    return _run_aggregate(hostname, worker, "getting resource utilization summary")


# Automation Tools
@mcp.tool()
def bulk_power_operations(operation: str, vm_list: str, hostname: str = None) -> str:
    """Perform bulk power operations on multiple VMs.

    VMs are located across every configured vCenter (unless a hostname is given),
    so the list may mix VMs from different vCenters.

    Args:
        operation: Power operation ('on', 'off', 'restart')
        vm_list: Comma-separated list of VM names or IDs
        hostname: vSphere hostname (optional; searches all configured vCenters if not provided)
    """
    if operation not in ['on', 'off', 'restart']:
        return "Error: Operation must be 'on', 'off', or 'restart'"

    action = {'on': 'start', 'off': 'stop', 'restart': 'reset'}[operation]
    verb = {'on': 'Power on', 'off': 'Power off', 'restart': 'Restart'}[operation]

    hosts = _resolve_hosts(hostname)
    if not hosts:
        return _NO_VCENTER_MSG

    by_name, by_id = _vm_index(hosts)
    requested = [name.strip() for name in vm_list.split(',') if name.strip()]

    per_host: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    lines: List[str] = []
    for label in requested:
        if label.startswith("vm-") and label in by_id:
            per_host[by_id[label]].append((label, label))
        elif label.lower() in by_name:
            host, vid = by_name[label.lower()]
            per_host[host].append((label, vid))
        else:
            lines.append(f"❌ {label}: VM not found on any configured vCenter")

    def make_worker(items: List[Tuple[str, str]]) -> Callable[[VSphereClient], List[str]]:
        def worker(client: VSphereClient) -> List[str]:
            out = []
            for label, vid in items:
                try:
                    client.post(f"vcenter/vm/{vid}/power/{action}")
                    out.append(f"✅ {label}: {verb} initiated")
                except Exception as e:
                    out.append(f"❌ {label}: Error - {str(e)}")
            return out
        return worker

    host_results = _run_per_host({h: make_worker(items) for h, items in per_host.items()})
    for host in hosts:
        if host not in host_results:
            continue
        res, err = host_results[host]
        if err is not None:
            for label, _ in per_host[host]:
                lines.append(f"❌ {label}: Error - {str(err)}")
        else:
            lines.extend(res)

    result = f"Bulk {operation.upper()} Operation Results:\n\n"
    result += "\n".join(lines)
    return result.strip()


# Destructive Operations with Confirmation
@mcp.tool()
def delete_vm_snapshot(vm_id: str, snapshot_id: str, confirm: bool = False, hostname: str = None) -> str:
    """Delete a snapshot for a specific VM. REQUIRES CONFIRMATION.

    Args:
        vm_id: Virtual machine ID or name
        snapshot_id: Snapshot ID to delete
        confirm: Must be True to proceed with deletion
        hostname: vSphere hostname (optional; auto-locates the VM across configured vCenters)
    """
    if not confirm:
        return f"⚠️  DESTRUCTIVE OPERATION: Delete snapshot {snapshot_id} for VM {vm_id}\n\nThis operation cannot be undone!\n\nTo proceed, call this function again with confirm=True"

    try:
        host, resolved_id = _resolve_vm(vm_id, hostname)
    except (ConnectionError, ValueError, KeyError) as e:
        return _handle_error(e, f"deleting snapshot {snapshot_id} for VM {vm_id}")

    def worker(client: VSphereClient) -> str:
        client.delete(f"vcenter/vm/{resolved_id}/snapshot/{snapshot_id}")
        return f"✅ Snapshot {snapshot_id} deleted successfully for VM {resolved_id} on {host}"

    return _run_aggregate(host, worker, f"deleting snapshot {snapshot_id} for VM {vm_id}")


@mcp.tool()
def delete_vm(vm_id: str, confirm: bool = False, hostname: str = None) -> str:
    """Delete a virtual machine. REQUIRES CONFIRMATION.

    Args:
        vm_id: Virtual machine ID or name
        confirm: Must be True to proceed with deletion
        hostname: vSphere hostname (optional; auto-locates the VM across configured vCenters)
    """
    if not confirm:
        return f"⚠️  DESTRUCTIVE OPERATION: Delete VM {vm_id}\n\nThis operation will permanently delete the virtual machine and all its data!\nThis operation cannot be undone!\n\nTo proceed, call this function again with confirm=True"

    try:
        host, resolved_id = _resolve_vm(vm_id, hostname)
    except (ConnectionError, ValueError, KeyError) as e:
        return _handle_error(e, f"deleting VM {vm_id}")

    def worker(client: VSphereClient) -> str:
        client.delete(f"vcenter/vm/{resolved_id}")
        return f"✅ VM {resolved_id} deleted successfully on {host}"

    return _run_aggregate(host, worker, f"deleting VM {vm_id}")


@mcp.tool()
def modify_vm_resources(vm_id: str, cpu_count: int = None, memory_gb: int = None, confirm: bool = False, hostname: str = None) -> str:
    """Modify VM resources (CPU and/or Memory). REQUIRES CONFIRMATION.

    Args:
        vm_id: Virtual machine ID or name
        cpu_count: New CPU count (optional)
        memory_gb: New memory in GB (optional)
        confirm: Must be True to proceed with modification
        hostname: vSphere hostname (optional; auto-locates the VM across configured vCenters)
    """
    if not confirm:
        changes = []
        if cpu_count is not None:
            changes.append(f"CPU: {cpu_count} vCPUs")
        if memory_gb is not None:
            changes.append(f"Memory: {memory_gb} GB")

        return f"⚠️  DESTRUCTIVE OPERATION: Modify VM {vm_id}\n\nProposed changes:\n" + "\n".join(f"  - {change}" for change in changes) + "\n\nThis operation will modify the VM configuration!\n\nTo proceed, call this function again with confirm=True"

    if cpu_count is None and memory_gb is None:
        return "Error: At least one resource (CPU or Memory) must be specified"

    try:
        host, resolved_id = _resolve_vm(vm_id, hostname)
    except (ConnectionError, ValueError, KeyError) as e:
        return _handle_error(e, f"modifying VM {vm_id}")

    def worker(client: VSphereClient) -> str:
        # Prepare modification data
        modification_data = {}

        if cpu_count is not None:
            modification_data["cpu"] = {"count": cpu_count}

        if memory_gb is not None:
            modification_data["memory"] = {"size_MiB": memory_gb * 1024}

        # Apply modifications
        client.patch(f"vcenter/vm/{resolved_id}", modification_data)

        changes = []
        if cpu_count is not None:
            changes.append(f"CPU: {cpu_count} vCPUs")
        if memory_gb is not None:
            changes.append(f"Memory: {memory_gb} GB")

        return f"✅ VM {resolved_id} modified successfully on {host}:\n" + "\n".join(f"  - {change}" for change in changes)

    return _run_aggregate(host, worker, f"modifying VM {vm_id}")


@mcp.tool()
def bulk_delete_vms(vm_list: str, confirm: bool = False, hostname: str = None) -> str:
    """Delete multiple VMs. REQUIRES CONFIRMATION.

    VMs are located across every configured vCenter (unless a hostname is given),
    so the list may mix VMs from different vCenters.

    Args:
        vm_list: Comma-separated list of VM names or IDs
        confirm: Must be True to proceed with deletion
        hostname: vSphere hostname (optional; searches all configured vCenters if not provided)
    """
    if not confirm:
        vm_names = [name.strip() for name in vm_list.split(',')]
        return f"⚠️  DESTRUCTIVE OPERATION: Delete Multiple VMs\n\nVMs to be deleted:\n" + "\n".join(f"  - {name}" for name in vm_names) + "\n\nThis operation will permanently delete all specified VMs and all their data!\nThis operation cannot be undone!\n\nTo proceed, call this function again with confirm=True"

    hosts = _resolve_hosts(hostname)
    if not hosts:
        return _NO_VCENTER_MSG

    by_name, by_id = _vm_index(hosts)
    requested = [name.strip() for name in vm_list.split(',') if name.strip()]

    per_host: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    lines: List[str] = []
    for label in requested:
        if label.startswith("vm-") and label in by_id:
            per_host[by_id[label]].append((label, label))
        elif label.lower() in by_name:
            host, vid = by_name[label.lower()]
            per_host[host].append((label, vid))
        else:
            lines.append(f"❌ {label}: VM not found on any configured vCenter")

    def make_worker(items: List[Tuple[str, str]]) -> Callable[[VSphereClient], List[str]]:
        def worker(client: VSphereClient) -> List[str]:
            out = []
            for label, vid in items:
                try:
                    client.delete(f"vcenter/vm/{vid}")
                    out.append(f"✅ {label}: Deleted successfully")
                except Exception as e:
                    out.append(f"❌ {label}: Error - {str(e)}")
            return out
        return worker

    host_results = _run_per_host({h: make_worker(items) for h, items in per_host.items()})
    for host in hosts:
        if host not in host_results:
            continue
        res, err = host_results[host]
        if err is not None:
            for label, _ in per_host[host]:
                lines.append(f"❌ {label}: Error - {str(err)}")
        else:
            lines.extend(res)

    result = f"Bulk Delete Operation Results:\n\n"
    result += "\n".join(lines)
    return result.strip()


@mcp.tool()
def force_power_off_vm(vm_id: str, confirm: bool = False, hostname: str = None) -> str:
    """Force power off a virtual machine (equivalent to pulling the power cord). REQUIRES CONFIRMATION.

    Args:
        vm_id: Virtual machine ID or name
        confirm: Must be True to proceed with force power off
        hostname: vSphere hostname (optional; auto-locates the VM across configured vCenters)
    """
    if not confirm:
        return f"⚠️  DESTRUCTIVE OPERATION: Force Power Off VM {vm_id}\n\nThis operation will immediately power off the VM without graceful shutdown!\nThis is equivalent to pulling the power cord and may cause data loss!\n\nTo proceed, call this function again with confirm=True"

    try:
        host, resolved_id = _resolve_vm(vm_id, hostname)
    except (ConnectionError, ValueError, KeyError) as e:
        return _handle_error(e, f"force powering off VM {vm_id}")

    def worker(client: VSphereClient) -> str:
        client.post(f"vcenter/vm/{resolved_id}/power/stop")
        return f"⚠️  VM {resolved_id} force powered off on {host} (equivalent to pulling power cord)"

    return _run_aggregate(host, worker, f"force powering off VM {vm_id}")


def main() -> None:
    """Main entry point for the MCP server."""
    import os
    from dotenv import load_dotenv

    # Load environment variables
    load_dotenv()

    # Configure FastMCP settings for streamable HTTP transport
    mcp.settings.host = os.getenv("SERVER_HOST", "0.0.0.0")
    mcp.settings.port = int(os.getenv("SERVER_PORT", "8000"))
    mcp.settings.stateless_http = True  # Enable stateless mode

    # Run with streamable HTTP transport
    mcp.run(transport="streamable-http")


# Export the Starlette/FastAPI app for testing and external use
app = mcp.streamable_http_app()


if __name__ == "__main__":
    main()
