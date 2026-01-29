import asyncio
import logging
import os
import json
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from vantiqconnectorsdk import (
    VantiqConnectorSet,
    VantiqConnector,
    setup_logging,
)

# ------------ Settings ------------
CONNECTOR_VERSION = "1.1.0"
log = logging.getLogger("ApiSourceConnector")

# Global runtime config (loaded on connect)
api_config: Dict[str, Any] = {
    "url": "http://192.168.10.162:8080/test/scaleItems",
    "token": None,
    "batch_size": 500,
    # If csvConfig.schema exists, we keep only those keys
    "schema_fields": [],
    # Multi-store mapping: WH### -> HierarchyId
    "hierarchy_map": {},
    # Optional: include master-data (Departments/PluGroups) only in first batch per hierarchy
    "include_master_data_in_first_batch": True,
    # Optional: local hierarchy json file path (fallback if hierarchyMap not provided in config)
    "hierarchy_json_path": None,
    # Fallback hierarchy if item has no whs_code
    "default_hierarchy_id": None,
}


# ------------ Helpers ------------
def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(str(v).strip())
    except Exception:
        return None


def build_hierarchy_map_from_list(
    hierarchy_list: List[Dict[str, Any]],
) -> Dict[str, str]:
    """
    Builds WH### -> HierarchyId based on the convention:
      - Level==2 are branches/warehouses
      - Hierarchy.Code is numeric-like ("01", "7", "18")
      - API whs_code is "WH" + 3-digit code ("WH007", "WH018")
    """
    out: Dict[str, str] = {}
    for h in hierarchy_list:
        if not isinstance(h, dict):
            continue
        if h.get("Level") != 2:
            continue
        if h.get("Active") is False:
            continue

        code_int = _safe_int(h.get("Code"))
        hid = h.get("Id")
        if code_int is None or not hid:
            continue

        whs_code = f"WH{code_int:03d}"
        out[whs_code] = str(hid)

    return out


def load_hierarchy_map_from_file(path: str) -> Dict[str, str]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, list):
        raise ValueError("Hierarchy JSON must be a list")
    return build_hierarchy_map_from_list(payload)


def apply_schema_filter(record: Dict[str, Any]) -> Dict[str, Any]:
    schema_fields = api_config.get("schema_fields") or []
    if not schema_fields:
        return record
    return {k: v for k, v in record.items() if k in schema_fields}


def map_api_item_to_vantiq_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Maps API item -> Items-like payload.
    GUARANTEE: PluGroup is ALWAYS set.
    """

    # ---- Resolve PluGroup (MANDATORY) ----
    raw_group = raw.get("group_code")
    if raw_group is None or str(raw_group).strip() == "":
        log.warning(
            "Missing group_code for barcode=%s, falling back to department_code",
            raw.get("barcode"),
        )
        raw_group = raw.get("department_code")

    if raw_group is None or str(raw_group).strip() == "":
        # This should never happen in a sane feed
        raise ValueError(
            f"Item {raw.get('item_code')} has no group_code and no department_code"
        )

    plu_group = str(raw_group).strip()

    record: Dict[str, Any] = {
        # Use barcode as the primary Code and do NOT send item_code
        "Code": str(raw.get("barcode") or "").strip(),
        "Name": str(raw.get("item_name_ar") or raw.get("item_name_en") or "").strip(),
        "Price": str(raw.get("sell_price", "0")),
        "Weighted": "1" if _truthy(raw.get("is_weightable")) else "0",
        # ---- STRUCTURAL FIELDS ----
        "Department": str(raw.get("department_code") or "").strip(),
        "PluGroup": plu_group,  # 🔥 ALWAYS PRESENT, STRING
        # ---- Optional enrichments ----
        # EAN set from barcode only
        "EAN": str(raw.get("barcode") or "").strip(),
        "PicturePath": raw.get("image_name"),
        # ---- Debug / trace ----
        "ExtendedInfo": {
            "whs_code": raw.get("whs_code"),
            "group_code": raw.get("group_code"),
            "department_code": raw.get("department_code"),
            "category": raw.get("category"),
            "org_sell_price": raw.get("org_sell_price"),
            "image_name": raw.get("image_name"),
        },
        # ---- Backward compatibility ----
        "Cost": "0",
        "Tara": "0",
    }

    return apply_schema_filter(record)


def extract_master_data(
    raw_items: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Build unique Departments + PluGroups definitions from the raw items list.

    We do NOT set PluGroups.DepartmentId here (we don't know the Department _id).
    Instead we send DepartmentCode and let VAIL resolve DepartmentId per hierarchy.
    """
    dept_by_code: Dict[str, Dict[str, Any]] = {}
    group_by_code: Dict[str, Dict[str, Any]] = {}

    for it in raw_items:
        dept_code = str(it.get("department_code") or "").strip()
        group_code = str(it.get("group_code") or "").strip()
        category = str(it.get("category") or "").strip()

        if dept_code:
            # Department schema requires Active, Code, Name
            # Name: we only have "category" which might not be department name; still better than empty.
            dept_by_code.setdefault(
                dept_code,
                {"Code": dept_code, "Name": category or dept_code, "Active": True},
            )

        if group_code:
            existing = group_by_code.get(group_code)
            if (
                existing
                and existing.get("DepartmentCode")
                and dept_code
                and existing["DepartmentCode"] != dept_code
            ):
                log.warning(
                    "Conflicting DepartmentCode for PluGroup %s: %s vs %s. Keeping first.",
                    group_code,
                    existing["DepartmentCode"],
                    dept_code,
                )
            else:
                group_by_code.setdefault(
                    group_code,
                    {
                        "Code": group_code,
                        "Name": category or group_code,
                        "Active": True,
                        "DepartmentCode": dept_code or None,
                    },
                )

    return list(dept_by_code.values()), list(group_by_code.values())


# ------------ Main fetch & fan-out pipeline ------------
async def run_fetch_pipeline(conn: VantiqConnector):
    if not api_config.get("token"):
        log.error("Missing API Token. Skipping fetch.")
        return

    hierarchy_map: Dict[str, str] = api_config.get("hierarchy_map") or {}
    if not hierarchy_map:
        log.error(
            "Missing hierarchy_map (WH### -> HierarchyId). Cannot fan-out by store."
        )
        return

    log.info("Starting fetch from %s", api_config["url"])

    try:
        headers = {"Authorization": api_config["token"]}

        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None, lambda: requests.get(api_config["url"], headers=headers, timeout=300)
        )

        if response.status_code != 200:
            log.error("API Error %s: %s", response.status_code, response.text)
            return

        raw_items = response.json()
        if not isinstance(raw_items, list):
            log.error("API response is not a list.")
            return

        log.info("Fetched %s raw items. Grouping by whs_code...", len(raw_items))

        # Group items by store / hierarchy
        by_whs: Dict[str, List[Dict[str, Any]]] = {}
        missing_whs = 0

        for it in raw_items:
            whs_code = it.get("whs_code") or ""
            whs_code = str(whs_code).strip()
            if not whs_code:
                missing_whs += 1
                continue
            by_whs.setdefault(whs_code, []).append(it)

        if missing_whs:
            log.warning("Items with missing whs_code: %s (skipped)", missing_whs)

        # Fan-out per store
        for whs_code, store_items in by_whs.items():
            hierarchy_id = hierarchy_map.get(whs_code)
            if not hierarchy_id:
                log.warning(
                    "Unknown whs_code=%s (no mapping to HierarchyId). Skipping %s items.",
                    whs_code,
                    len(store_items),
                )
                continue

            # Build hierarchical master-data (Departments + PluGroups) for this store/hierarchy
            # Departments and PluGroups are linked via `departmentId` on the group object
            unique_departments: Dict[str, Dict[str, Any]] = {}
            unique_groups: Dict[str, Dict[str, Any]] = {}

            for it in store_items:
                d_code = it.get("department_code")
                g_code = it.get("group_code")
                category = str(it.get("category") or "").strip()

                if d_code and str(d_code).strip() not in unique_departments:
                    dc = str(d_code).strip()
                    unique_departments[dc] = {
                        "id": dc,
                        "name": f"Dept {dc}",
                        "hierarchyId": hierarchy_id,
                    }

                if g_code and str(g_code).strip() not in unique_groups:
                    gc = str(g_code).strip()
                    # Use the item's category for the PluGroup name when available
                    group_name = category if category else f"Group {gc}"
                    unique_groups[gc] = {
                        "id": gc,
                        "name": group_name,
                        "departmentId": str(d_code).strip() if d_code else None,
                        "hierarchyId": hierarchy_id,
                    }

            departments = list(unique_departments.values())
            plu_groups = list(unique_groups.values())

            print(f"Resolved hierarchyId for whs_code={whs_code}: {hierarchy_id}")
            log.info(
                "Resolved mapping: whs_code=%s -> hierarchyId=%s",
                whs_code,
                hierarchy_id,
            )
            vantiq_items = [map_api_item_to_vantiq_item(it) for it in store_items]

            total = len(vantiq_items)
            batch_size = int(api_config.get("batch_size") or 500)
            total_batches = (total + batch_size - 1) // batch_size if total else 0

            log.info(
                "Store %s -> HierarchyId=%s: %s items, %s batches",
                whs_code,
                hierarchy_id,
                total,
                total_batches,
            )

            for i in range(0, total, batch_size):
                batch_items = vantiq_items[i : i + batch_size]
                current_batch_num = (i // batch_size) + 1
                is_last = (i + batch_size) >= total

                include_master = (
                    api_config.get("include_master_data_in_first_batch", True)
                    and current_batch_num == 1
                )

                event: Dict[str, Any] = {
                    "hierarchyId": hierarchy_id,
                    "targetHierarchyId": hierarchy_id,
                    "whs_code": whs_code,
                    "lines": batch_items,  # keep backward-compatible key
                    "EOF": is_last,
                    "timestamp": datetime.now().isoformat(),
                }

                if include_master:
                    event["departments"] = departments
                    event["pluGroups"] = plu_groups
                    event["includeMasterData"] = True

                await conn.send_notification(event)

                log.info(
                    "Sent store=%s batch %s/%s (%s items). master=%s last=%s",
                    whs_code,
                    current_batch_num,
                    total_batches,
                    len(batch_items),
                    include_master,
                    is_last,
                )

                # Wait 1 second between each batch send to avoid overwhelming the receiver
                await asyncio.sleep(1)

    except Exception as e:
        log.exception("Pipeline failed: %s", e)


# ------------ Vantiq Handlers ------------
async def handle_connect(ctx: dict, config: dict):
    source_name = ctx.get(VantiqConnector.SOURCE_NAME)
    log.info("Connected to source: %s", source_name)

    # Load config
    source_cfg = config.get("config", config)

    api_config["url"] = source_cfg.get("apiUrl", api_config["url"])
    api_config["token"] = source_cfg.get("apiToken")
    api_config["batch_size"] = int(
        source_cfg.get("batchSize", api_config["batch_size"])
    )
    api_config["include_master_data_in_first_batch"] = bool(
        source_cfg.get("includeMasterDataInFirstBatch", True)
    )

    # Optional: a fallback hierarchy id (if ever needed)
    api_config["default_hierarchy_id"] = source_cfg.get("defaultHierarchyId")

    # Schema
    schema = source_cfg.get("csvConfig", {}).get("schema", {})
    if schema:
        api_config["schema_fields"] = list(schema.keys())
    else:
        # Default: include the new fields too
        api_config["schema_fields"] = [
            "Code",
            "Name",
            "Price",
            "Weighted",
            "Department",
            "PluGroup",
            "EAN",
            "PicturePath",
            "ExtendedInfo",
            "Cost",
            "Tara",
        ]

    # Hierarchy map
    hierarchy_map = source_cfg.get("hierarchyMap")
    if isinstance(hierarchy_map, dict) and hierarchy_map:
        api_config["hierarchy_map"] = {str(k): str(v) for k, v in hierarchy_map.items()}
        log.info(
            "Loaded hierarchy_map from config (%s entries).",
            len(api_config["hierarchy_map"]),
        )
    else:
        # Fallback: load from a local JSON file
        hierarchy_path = source_cfg.get("hierarchyJsonPath") or api_config.get(
            "hierarchy_json_path"
        )
        if not hierarchy_path:
            # Try same directory as script
            hierarchy_path = os.path.join(os.path.dirname(__file__), "Hierarchy.json")

        api_config["hierarchy_json_path"] = hierarchy_path

        try:
            api_config["hierarchy_map"] = load_hierarchy_map_from_file(hierarchy_path)
            log.info(
                "Loaded hierarchy_map from %s (%s entries).",
                hierarchy_path,
                len(api_config["hierarchy_map"]),
            )
        except Exception as e:
            api_config["hierarchy_map"] = {}
            log.exception("Failed loading hierarchy_map from %s: %s", hierarchy_path, e)

    log.info(
        "Config loaded. url=%s batchSize=%s schemaFields=%s",
        api_config["url"],
        api_config["batch_size"],
        len(api_config["schema_fields"]),
    )


async def handle_publish(ctx: dict, msg: dict):
    source_name = ctx.get(VantiqConnector.SOURCE_NAME)
    conn = connector_set.get_connection_for_source(source_name)
    log.info("Manual trigger received.")
    asyncio.create_task(run_fetch_pipeline(conn))


async def handle_query(ctx: dict, msg: dict):
    """
    Handle query requests including ping/health-check.
    """
    source_name = ctx.get(VantiqConnector.SOURCE_NAME)
    conn = connector_set.get_connection_for_source(source_name)

    # Extract op from msg or msg.body (handles nested structure)
    op = None
    if isinstance(msg, dict):
        op = msg.get("op")
        if not op and "body" in msg and isinstance(msg["body"], dict):
            op = msg["body"].get("op")

    if op == "ping":
        # Build standardized health-check response
        info = {
            "code": "io.vantiq.extsrc.api.multi_store.success",
            "message": "Pong",
            "value": CONNECTOR_VERSION,
            "timestamp": datetime.now().isoformat(),
        }
        log.info("Ping from Vantiq – responding with: %s", info)

        await conn.send_query_response(
            ctx,
            VantiqConnector.QUERY_COMPLETE,
            info,
        )
        return

    # If no supported query, log and ignore
    log.debug("Query received with unsupported op: %s", op)


async def handle_close(ctx: dict):
    pass


# ------------ Main ------------
async def main():
    global connector_set
    setup_logging()
    log.info("Starting ApiSourceConnector v%s", CONNECTOR_VERSION)

    connector_set = VantiqConnectorSet()
    connector_set.configure_handlers_for_all(
        handle_close, handle_connect, handle_publish, handle_query
    )

    await connector_set.declare_healthy()
    await connector_set.run_connectors()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
