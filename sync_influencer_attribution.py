# sync_influencer_attribution.py

import os
import sys
import json
import time
import re
from datetime import datetime, timezone, timedelta, date
from typing import Dict, Any, Optional, List, Set

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo  # Python 3.11 OK on GitHub Actions


# ----------------------------
# Env
# ----------------------------
SHOPIFY_SHOP = os.getenv("SHOPIFY_SHOP", "").strip()
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN", "").strip()
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01").strip()

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Influencer Schedule").strip()

# Linked products setup (in same base)
AIRTABLE_PRODUCTS_TABLE_NAME = os.getenv("AIRTABLE_PRODUCTS_TABLE_NAME", "Products").strip()
PRODUCT_GID_FIELD = os.getenv("PRODUCT_GID_FIELD", "Shopify Product GID").strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "5"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.05"))
MAX_ORDERS_PAGES = int(os.getenv("MAX_ORDERS_PAGES", "200"))

# subtotal or total
SALES_MODE = os.getenv("SALES_MODE", "subtotal").strip().lower()  # "subtotal" or "total"

ET_TZ = "America/New_York"


# ----------------------------
# Airtable field names (MATCH YOUR BASE)
# ----------------------------
F = {
    # Inputs
    "post_date": "Date",
    "code": "Code",
    "products_link": "Products",  # linked to Products table

    # Existing outputs
    "sales_out": "Sales",
    "orders_out": "Orders",

    # New outputs (YOU WILL CREATE THESE 4 NUMBER FIELDS IN AIRTABLE)
    "all_units_sold": "All Units Sold",
    "promoted_units_sold": "Promoted Units Sold",
    "all_units_sold_with_code": "All Units Sold With Code",
    "promoted_units_sold_with_code": "Promoted Units Sold With Code",

    # Optional (only if you add this field; otherwise set to "" or remove)
    "last_synced_at": "Last Synced At",
}


# ----------------------------
# HTTP session w/ retries
# ----------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST", "PATCH"),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


S = make_session()


def die(msg: str) -> None:
    print(msg, file=sys.stderr)
    sys.exit(1)


def shopify_graphql_url() -> str:
    if not SHOPIFY_SHOP:
        die("Missing SHOPIFY_SHOP.")
    return f"https://{SHOPIFY_SHOP}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"


def shopify_headers() -> Dict[str, str]:
    if not SHOPIFY_ADMIN_TOKEN:
        die("Missing SHOPIFY_ADMIN_TOKEN.")
    return {
        "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN,
        "Content-Type": "application/json",
    }


def airtable_headers() -> Dict[str, str]:
    if not AIRTABLE_TOKEN:
        die("Missing AIRTABLE_TOKEN.")
    return {"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Content-Type": "application/json"}


def airtable_url(path: str) -> str:
    if not AIRTABLE_BASE_ID:
        die("Missing AIRTABLE_BASE_ID.")
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{path}"


def shopify_graphql(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"query": query, "variables": variables}
    r = S.post(shopify_graphql_url(), headers=shopify_headers(), data=json.dumps(payload), timeout=90)
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data


def a_list_records(
    table_name: str,
    filter_formula: str,
    fields: Optional[List[str]] = None,
    page_size: int = 100
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"filterByFormula": filter_formula, "pageSize": page_size}
    if fields:
        for i, f in enumerate(fields):
            params[f"fields[{i}]"] = f

    out: List[Dict[str, Any]] = []
    offset = None
    while True:
        if offset:
            params["offset"] = offset
        r = S.get(airtable_url(table_name), headers=airtable_headers(), params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return out


def aupdate(table_name: str, record_id: str, fields: Dict[str, Any]) -> None:
    r = S.patch(
        airtable_url(f"{table_name}/{record_id}"),
        headers=airtable_headers(),
        data=json.dumps({"fields": fields}),
        timeout=60,
    )
    r.raise_for_status()


def parse_airtable_date(v: Any) -> Optional[date]:
    if not v:
        return None
    if isinstance(v, str):
        try:
            return datetime.strptime(v, "%Y-%m-%d").date()
        except Exception:
            return None
    return None


def et_midnight_to_utc(d: date) -> datetime:
    et = ZoneInfo(ET_TZ)
    local = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=et)
    return local.astimezone(timezone.utc)


def norm(s: str) -> str:
    return (s or "").strip().upper()


def order_codes(order_node: Dict[str, Any]) -> List[str]:
    """
    Shopify 'discountCodes' can come back as:
    - string "TEST10"
    - list ["TEST10"]
    - list of strings with commas
    """
    raw = order_node.get("discountCodes")
    if not raw:
        return []

    if isinstance(raw, list):
        out: List[str] = []
        for item in raw:
            if item is None:
                continue
            if isinstance(item, str):
                parts = re.split(r"[,\s]+", item.strip())
                out.extend([norm(p) for p in parts if p.strip()])
        # de-dupe
        seen = set()
        deduped = []
        for c in out:
            if c not in seen:
                seen.add(c)
                deduped.append(c)
        return deduped

    if isinstance(raw, str):
        parts = re.split(r"[,\s]+", raw.strip())
        return [norm(p) for p in parts if p.strip()]

    return []


def order_amount(order_node: Dict[str, Any]) -> float:
    key = "currentSubtotalPriceSet" if SALES_MODE == "subtotal" else "totalPriceSet"
    m = (((order_node.get(key) or {}).get("shopMoney")) or {})
    try:
        return float(m.get("amount"))
    except Exception:
        return 0.0


# ----------------------------
# Shopify query: orders + line items
# ----------------------------
QUERY_ORDERS = """
query Orders($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: false) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        createdAt
        cancelledAt
        discountCodes
        currentSubtotalPriceSet { shopMoney { amount currencyCode } }
        totalPriceSet { shopMoney { amount currencyCode } }
        lineItems(first: 250) {
          edges {
            node {
              quantity
              product { id }
            }
          }
        }
      }
    }
  }
}
"""


def get_selected_product_gids(linked_record_ids: List[str]) -> Set[str]:
    """
    linked_record_ids are Airtable record IDs from Influencer Schedule -> Products linked field.
    We look those up in the Products table to get Shopify Product GIDs.
    """
    if not linked_record_ids:
        return set()

    gids: Set[str] = set()
    chunk_size = 50  # keep formula length reasonable

    for i in range(0, len(linked_record_ids), chunk_size):
        chunk = linked_record_ids[i:i + chunk_size]
        ors = ",".join([f"RECORD_ID()='{rid}'" for rid in chunk])
        filter_formula = f"OR({ors})"

        recs = a_list_records(
            table_name=AIRTABLE_PRODUCTS_TABLE_NAME,
            filter_formula=filter_formula,
            fields=[PRODUCT_GID_FIELD],
            page_size=100
        )

        for r in recs:
            flds = r.get("fields") or {}
            gid = flds.get(PRODUCT_GID_FIELD)
            if isinstance(gid, str) and gid.strip():
                gids.add(gid.strip())

        time.sleep(SLEEP_SECONDS)

    return gids


def main() -> None:
    if SALES_MODE not in ("subtotal", "total"):
        die("SALES_MODE must be 'subtotal' or 'total'.")

    if not (SHOPIFY_SHOP and SHOPIFY_ADMIN_TOKEN and AIRTABLE_TOKEN and AIRTABLE_BASE_ID):
        die("Missing required env vars.")

    MODE = os.getenv("MODE", "incremental").strip().lower()
    BACKFILL_START = os.getenv("BACKFILL_START", "").strip()
    BACKFILL_END = os.getenv("BACKFILL_END", "").strip()

    today_et = datetime.now(ZoneInfo(ET_TZ)).date()

    if MODE == "backfill":
        if not BACKFILL_START or not BACKFILL_END:
            die("Backfill mode requires BACKFILL_START and BACKFILL_END (YYYY-MM-DD).")
        start_d = datetime.strptime(BACKFILL_START, "%Y-%m-%d").date()
        end_d = datetime.strptime(BACKFILL_END, "%Y-%m-%d").date()

        formula = (
            f"AND("
            f"{{{F['post_date']}}}!=BLANK(),"
            f"{{{F['code']}}}!=BLANK(),"
            f"IS_AFTER({{{F['post_date']}}}, '{(start_d - timedelta(days=1)).isoformat()}'),"
            f"IS_BEFORE({{{F['post_date']}}}, '{(end_d + timedelta(days=1)).isoformat()}')"
            f")"
        )
        print(f"[backfill] range {start_d}..{end_d} (ET)", flush=True)
    else:
        cutoff = today_et - timedelta(days=LOOKBACK_DAYS)
        cutoff_minus_1 = (cutoff - timedelta(days=1)).isoformat()
        formula = (
            f"AND("
            f"{{{F['post_date']}}}!=BLANK(),"
            f"{{{F['code']}}}!=BLANK(),"
            f"IS_AFTER({{{F['post_date']}}}, '{cutoff_minus_1}')"
            f")"
        )
        print(f"[incremental] since {cutoff} (ET)", flush=True)

    # Pull influencer schedule records (include linked Products field)
    recs = a_list_records(
        table_name=AIRTABLE_TABLE_NAME,
        filter_formula=formula,
        fields=[F["post_date"], F["code"], F["products_link"]],
        page_size=100
    )

    print(f"Found {len(recs)} records (ET).", flush=True)

    for i, rec in enumerate(recs, start=1):
        rid = rec["id"]
        af = rec.get("fields", {})

        d0 = parse_airtable_date(af.get(F["post_date"]))
        code = norm(af.get(F["code"]))

        if not d0 or not code:
            continue

        # Linked products are record IDs
        linked_products = af.get(F["products_link"]) or []
        if not isinstance(linked_products, list):
            linked_products = []

        selected_gids = get_selected_product_gids(linked_products) if linked_products else set()

        # 2-day window: D0 and D1 (ET)
        start_utc = et_midnight_to_utc(d0)
        end_utc = et_midnight_to_utc(d0 + timedelta(days=2))

        q = f"created_at:>={start_utc.isoformat()} created_at:<{end_utc.isoformat()}"

        after = None
        pages = 0

        # Existing metrics (WITH CODE)
        orders_with_code = 0
        sales_with_code = 0.0

        # New unit metrics
        all_units_sold = 0
        promoted_units_sold = 0
        all_units_sold_with_code = 0
        promoted_units_sold_with_code = 0

        while True:
            pages += 1
            if pages > MAX_ORDERS_PAGES:
                print(f"[WARN] hit MAX_ORDERS_PAGES on {rid}", flush=True)
                break

            data = shopify_graphql(QUERY_ORDERS, {"first": 250, "after": after, "query": q})
            conn = data["data"]["orders"]
            edges = conn["edges"]
            pi = conn["pageInfo"]

            for edge in edges:
                o = edge["node"]
                if o.get("cancelledAt"):
                    continue

                # Sum units for ALL orders in the window
                line_edges = (((o.get("lineItems") or {}).get("edges")) or [])
                order_units_all = 0
                order_units_promoted = 0

                for le in line_edges:
                    li = le.get("node") or {}
                    qty = int(li.get("quantity") or 0)
                    order_units_all += qty

                    pid = ((li.get("product") or {}).get("id") or "")
                    if pid and pid in selected_gids:
                        order_units_promoted += qty

                all_units_sold += order_units_all
                promoted_units_sold += order_units_promoted

                # If order used influencer code, also count "with code" metrics
                if code in order_codes(o):
                    orders_with_code += 1
                    sales_with_code += order_amount(o)

                    all_units_sold_with_code += order_units_all
                    promoted_units_sold_with_code += order_units_promoted

            if not pi["hasNextPage"]:
                break
            after = pi["endCursor"]
            if not after:
                break
            time.sleep(SLEEP_SECONDS)

        update = {
            # existing
            F["orders_out"]: orders_with_code,
            F["sales_out"]: round(sales_with_code, 2),

            # new unit metrics
            F["all_units_sold"]: all_units_sold,
            F["promoted_units_sold"]: promoted_units_sold,
            F["all_units_sold_with_code"]: all_units_sold_with_code,
            F["promoted_units_sold_with_code"]: promoted_units_sold_with_code,
        }

        # Optional (only if you add this field in Airtable)
        if F.get("last_synced_at"):
            update[F["last_synced_at"]] = datetime.now(timezone.utc).isoformat()

        aupdate(AIRTABLE_TABLE_NAME, rid, update)

        print(
            f"[{i}/{len(recs)}] {d0} code={code} "
            f"orders_with_code={orders_with_code} sales_with_code={sales_with_code:.2f} "
            f"all_units={all_units_sold} promoted_units={promoted_units_sold} "
            f"all_units_code={all_units_sold_with_code} promoted_units_code={promoted_units_sold_with_code}",
            flush=True
        )

        time.sleep(SLEEP_SECONDS)

    print("Done.", flush=True)


if __name__ == "__main__":
    main()
