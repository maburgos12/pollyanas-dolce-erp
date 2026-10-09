"""READ adapters. Identity comes from the server run context, never model arguments."""
import json
import os
import subprocess
from pathlib import Path
from agno.run import RunContext

HERE = Path(__file__).resolve().parent
DJANGO_PYTHON = "/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/.venv/bin/python"
LOCAL_DATABASE = "postgresql://postgres:postgres@127.0.0.1:56673/pastelerias_erp"
MAYA_TOOLS = {"catalog", "branches", "get_sale_price", "check_pickup_availability", "get_branch_info"}


def erp_call(actor, tool, arguments):
    try:
        result = subprocess.run([DJANGO_PYTHON, str(HERE / "erp_bridge.py")],
            input=json.dumps({"actor": actor, "tool": tool, "arguments": arguments}), text=True,
            capture_output=True, timeout=20, env={**os.environ, "DATABASE_URL": LOCAL_DATABASE,
                "APP_ENV": "development", "ALLOW_INSECURE_LOCAL_SECRET_KEY": "1"})
        if result.returncode:
            return {"status": "error", "error": "erp_bridge_failed"}
        return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, ValueError):
        return {"status": "error", "error": "erp_bridge_timeout_or_invalid_response"}


def search_assets(run_context: RunContext, q: str = "", sucursal_id: int | None = None) -> dict:
    """Search authorized equipment. Empty q lists equipment. Results are synthetic laboratory fixtures."""
    arguments = {"q": q, "limit": 10}
    if sucursal_id is not None:
        arguments["sucursal_id"] = sucursal_id
    return erp_call(run_context.user_id, "erp.search_assets", arguments)


def get_asset_context(run_context: RunContext, activo_id: int) -> dict:
    """Get current equipment details and maintenance history by an ID found through search_assets."""
    return erp_call(run_context.user_id, "erp.get_asset_context", {"activo_id": activo_id})


def get_pending_maintenance(run_context: RunContext) -> dict:
    """List authorized preventive maintenance plans. Empty result means no recorded plans, not no maintenance need."""
    return erp_call(run_context.user_id, "erp.get_pending_maintenance", {"limit": 10})


def maya_call(actor, name, arguments):
    if actor != "lab-manager" or name not in MAYA_TOOLS:
        return {"status": "denied", "error": "permission_denied"}
    # shortcut: SSH reuses existing public READ services for evaluation; replace with a scoped API before production.
    script = '''import asyncio,json
from app.ai.tools.customer_read_tools import CustomerReadTools,_active_catalog
request=REQUEST
async def main():
 if request["tool"]=="catalog":
  rows=await _active_catalog()
  result={"status":"ok","products":rows[:20],"truncated":len(rows)>20,"total":len(rows)}
 elif request["tool"]=="branches":
  rows=await CustomerReadTools().branches.get_all_active()
  result={"status":"ok","branches":[{"id":r["id"],"name":r["name"]} for r in rows[:20]],"truncated":len(rows)>20}
 else:
  result=await CustomerReadTools().execute(request["tool"],request["arguments"])
 print(json.dumps(result,ensure_ascii=False,default=str))
asyncio.run(main())
'''.replace("REQUEST", repr({"tool": name, "arguments": arguments}))
    try:
        result = subprocess.run(["ssh", "-i", "/Users/mauricioburgos/.ssh/agente_dg_ops", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
            "root@68.183.165.47", "cd /opt/pollyana-omnichannel && venv/bin/python -"],
            input=script, text=True, capture_output=True, timeout=30)
        if result.returncode:
            return {"status": "error", "error": "maya_service_unavailable"}
        payload = json.loads(result.stdout)
        payload["environment"] = "LIVE_PUBLIC_READ_VIA_EXISTING_MAYA_SERVICES"
        return payload
    except (subprocess.TimeoutExpired, ValueError):
        return {"status": "desconocido", "error": "maya_service_timeout_or_invalid_response"}


def catalog(run_context: RunContext) -> dict:
    """List active public product names from Maya's existing catalog; no customer records."""
    return maya_call(run_context.user_id, "catalog", {})


def branches(run_context: RunContext) -> dict:
    """Discover canonical active branch names in Maya. Missing branch means not configured in this source, not closed."""
    return maya_call(run_context.user_id, "branches", {})


def get_sale_price(run_context: RunContext, product_name: str, size: str) -> dict:
    """Get verified current Point price. Size: Individual, Chico, Mediano or Grande. Unknown is not zero."""
    return maya_call(run_context.user_id, "get_sale_price", {"product_name": product_name, "size": size})


def check_pickup_availability(run_context: RunContext, product_name: str, size: str, branch_name: str) -> dict:
    """Check verified stock of a canonical product/size at a branch. Unknown is not out of stock."""
    return maya_call(run_context.user_id, "check_pickup_availability", {"product_name": product_name, "size": size, "branch_name": branch_name})


def get_branch_info(run_context: RunContext, branch_name: str) -> dict:
    """Read public branch address, contact details and hours through the existing Maya service."""
    return maya_call(run_context.user_id, "get_branch_info", {"branch_name": branch_name})
