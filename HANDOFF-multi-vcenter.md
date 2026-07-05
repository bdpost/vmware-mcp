# Handoff: multi-vCenter support + `.gitignore`

Deploy/verify notes for the Docker host. Code was written and unit-tested on a
dev machine **without Docker**; the live build + smoke test happen here.

## What changed

1. **`.gitignore` added** (was missing). Ignores `.env`, `.env.*` (keeps
   `env.example`), `__pycache__/`, venvs, editor/OS cruft. No secrets were ever
   tracked; this prevents a future `.env` from being committed.

2. **Multiple vCenters, shared credentials.** Configure a comma-separated
   `VCENTER_HOSTS` (first = primary/default). One shared `VCENTER_USER` /
   `VCENTER_PASSWORD` covers them all.
   - Inventory/list tools (`list_vms`, `list_hosts`, `list_datastores`, …) query
     **every** configured vCenter in parallel and label results per host; one
     vCenter being down does not fail the others.
   - VM-targeted tools (`get_vm_details`, `power_on_vm`, snapshots, deletes,
     bulk ops, …) **auto-locate** the VM by id or name across all vCenters.
   - Passing an explicit `hostname` to any tool restricts it to that vCenter.
   - New `list_vcenters` tool lists what's configured.
   - `VCENTER_HOST` (singular) still works as a fallback if `VCENTER_HOSTS` unset.

### Files touched
| File | Change |
|------|--------|
| `.gitignore` | new — ignores `.env` etc. |
| `src/vsphere_mcp_server/credentials.py` | `get_vcenter_hosts()`; credentials now host-agnostic (bind to requested host, shared user/pass) |
| `src/vsphere_mcp_server/vsphere_client.py` | fixed inverted/inconsistent `INSECURE` default in the SSL-verify fallback |
| `src/vsphere_mcp_server/server.py` | multi-host helpers (`_run_aggregate`, `_for_each_host`, `_run_per_host`, `_run_locate`, `_resolve_vm`, `_vm_index`); all ~30 tools refactored; added `list_vcenters`; some detail-tool signatures changed so `hostname` is an optional trailing arg |
| `env.example`, `docker-compose.yml`, `README.md` | documented `VCENTER_HOSTS` |

> **Signature note (breaking for explicit-positional callers):** a few tools that
> took `hostname` as the *first* positional arg now take it *last* and optional:
> `get_host_details(host_id, hostname=None)`, `get_datacenter_details(...)`,
> `get_datastore_details(...)`, `get_folder_details(...)`, `get_network_details(...)`,
> `list_folders(folder_type=..., hostname=None)`, `list_datacenters(hostname=None)`,
> `list_vlans(hostname=None)`, `get_vlan_info(vlan_query, hostname=None)`. If any
> external caller passed these positionally they'll need updating; MCP/LLM callers
> using named args are unaffected.

## Already verified on the dev box (no Docker)
- `python3 -m py_compile` on all sources — clean.
- `get_vcenter_hosts()` parsing (comma list, spaces/blank trimming, `VCENTER_HOST`
  fallback, precedence, empty).
- Host-agnostic credential resolution (binds to requested host; defaults to
  primary; returns `None` when password missing).
- Full server logic against a **mocked** two-vCenter client: aggregation with
  per-host headers, cross-vCenter VM find by name, targeting the correct owning
  vCenter, bulk routing per host, missing-VM reporting, single-host clean output,
  and per-host error isolation.

What could **not** be tested without Docker/live vCenters: real REST auth against
your two vCenters and end-to-end MCP transport. That's the smoke test below.

## Deploy on the Docker host

```bash
# 1. Get the code (pull the branch/commit with these changes)
cd /path/to/vmware-mcp
git pull   # or checkout the relevant branch

# 2. Configure .env  (chmod 600 — holds the shared vCenter password)
cp env.example .env
$EDITOR .env
chmod 600 .env
```

`.env` should look like:
```env
VCENTER_HOSTS=vcenter-a.your.domain,vcenter-b.your.domain
VCENTER_USER=svc-account@vsphere.local
VCENTER_PASSWORD=********
INSECURE=True          # lab certs; set False if both vCenters have trusted certs
SERVER_HOST=0.0.0.0
SERVER_PORT=8000
```

```bash
# 3. Build + run
docker compose up -d --build
docker compose logs -f --tail=50    # watch it come up; Ctrl-C to stop tailing
```

> Note: `docker-compose.yml` joins an **external** network `llm-rag-mcp_backend`
> (for AnythingLLM). If that network doesn't exist on this host the stack won't
> start — create it or drop the `backend` external network from the compose file.

## Smoke test (against the real vCenters)

The server speaks MCP over streamable-HTTP at `http://<host>:8000/mcp`. Easiest
path is to point your MCP client (AnythingLLM per the README) at it and ask:

1. **"List the configured vCenters"** → `list_vcenters` should show both, primary
   marked. Confirms env parsing.
2. **"List all VMs"** (no host named) → VMs from **both** vCenters, each under a
   `=== vcenter-x ===` header. Confirms parallel aggregation + auth to both.
3. **"Get details for `<a VM that only exists on the second vCenter>`"** → returns
   from the correct vCenter without you naming it. Confirms cross-vCenter find.
4. **"List VMs on `vcenter-a.your.domain`"** (explicit host) → only that vCenter,
   no headers. Confirms explicit targeting still works.
5. **Failure isolation (optional):** temporarily point one host at a bad name in
   `VCENTER_HOSTS`, redeploy, run "list all VMs" → the good vCenter still returns,
   the bad one shows an inline connection error. Revert after.

### No MCP client handy? Quick container-side sanity check
Auth against each vCenter directly from inside the container to prove creds +
reachability (uses the same REST endpoint the client uses):

```bash
docker compose exec vsphere-mcp-server python3 - <<'PY'
import os
from vsphere_mcp_server.vsphere_client import VSphereClient
from vsphere_mcp_server.credentials import get_vcenter_hosts
for h in get_vcenter_hosts():
    c = VSphereClient(h)
    try:
        vms = c.get("vcenter/vm").get("value", [])
        print(f"OK   {h}: {len(vms)} VMs")
    except Exception as e:
        print(f"FAIL {h}: {e}")
    finally:
        c.close()
PY
```
Expect one `OK <host>: N VMs` line per vCenter.

## Rollback
```bash
git checkout -- .          # discard working changes, or
git revert <commit>        # if already committed
docker compose up -d --build
```
`.env` is untouched by git, so rollback won't disturb your credentials.
