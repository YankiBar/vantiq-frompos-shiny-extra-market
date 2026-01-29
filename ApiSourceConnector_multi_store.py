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
CONNECTOR_VERSION = "1.3.5"  # Updated based on 1.1.0
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
    FIX: Uses 'segment' as the single source for PluGroup and Department.
    """

    # ---- Resolve Segment (MANDATORY) ----
    segment = raw.get("segment")
    if segment is None or str(segment).strip() == "":
        # Log warning and skip or use fallback?
        # Per instructions: segment is mandatory now.
        log.warning("Item %s missing 'segment'. Skipping.", raw.get("barcode"))
        raise ValueError("Missing segment")

    segment_str = str(segment).strip()

    record: Dict[str, Any] = {
        # Use barcode as the primary Code and do NOT send item_code
        "Code": str(raw.get("barcode") or "").strip(),
        "Name": str(raw.get("item_name_ar") or raw.get("item_name_en") or "").strip(),
        "Price": str(raw.get("sell_price", "0")),
        "Weighted": "1" if _truthy(raw.get("is_weightable")) else "0",
        # ---- STRUCTURAL FIELDS (Modified) ----
        "Department": segment_str,  # Mapped from segment
        "PluGroup": segment_str,  # Mapped from segment
        # ---- Optional enrichments ----
        # EAN set from barcode only
        "EAN": str(raw.get("barcode") or "").strip(),
        "PicturePath": raw.get("image_name"),
        # ---- Debug / trace ----
        "ExtendedInfo": {
            "whs_code": raw.get("whs_code"),
            "segment": segment_str,
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
    FIX: Derived from segment logic.
    """
    dept_by_code: Dict[str, Dict[str, Any]] = {}
    group_by_code: Dict[str, Dict[str, Any]] = {}

    for it in raw_items:
        segment = str(it.get("segment") or "").strip()
        category = str(it.get("category") or "").strip()

        if not segment:
            continue

        # Department (Code = segment)
        if segment not in dept_by_code:
            dept_by_code[segment] = {
                "Code": segment,
                "Name": category or f"Dept {segment}",
                "Active": True,
            }

        # PluGroup (Code = segment, Linked to Department = segment)
        if segment not in group_by_code:
            group_by_code[segment] = {
                "Code": segment,
                "Name": category or f"Group {segment}",
                "Active": True,
                "DepartmentCode": segment,
            }

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

            # FIX: Use new extract_master_data based on segment
            departments, plu_groups = extract_master_data(store_items)

            # Map items
            vantiq_items = []
            for it in store_items:
                try:
                    vantiq_items.append(map_api_item_to_vantiq_item(it))
                except ValueError:
                    continue

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
            "segment",  # FIX: Added segment so it won't be filtered out
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

    # FIX: try/except block to handle Port 10048 error gracefully
    try:
        await connector_set.declare_healthy()
    except Exception as e:
        log.warning("Health check port busy (ignoring): %s", e)

    await connector_set.run_connectors()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
