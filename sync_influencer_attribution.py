# sync_influencer_attribution.py
import os
import sys
import json
import time
from datetime import datetime, timezone, timedelta, date
from typing import Dict, Any, Optional, List

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

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "5"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.05"))
MAX_ORDERS_PAGES = int(os.getenv("MAX_ORDERS_PAGES", "200"))
SALES_MODE = os.getenv("SALES_MODE", "subtotal").strip().lower()  # "subtotal" or "total"

ET_TZ = "America/New_York"

# Airtable field names (MATCHING YOUR SCREENSHOTS)
F = {
    "post_date": "Date",
    "code": "Code",
    "sales_out": "Sales",
    "orders_out": "Orders",
    "last_synced_at": "Last Synced At",  # optional; add this field if you want
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


def a_list_records(filter_formula: str, fields: Optional[List[str]] = None, page_size: int = 100) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"filterByFormula": filter_formula, "pageSize": page_size}
    if fields:
        for i, f in enumerate(fields):
            params[f"fields[{i}]"] = f

    out: List[Dict[str, Any]] = []
    offset = None
    while True:
        if offset:
            params["offset"] = offset
        r = S.get(airtable_url(AIRTABLE_TABLE_NAME), headers=airtable_headers(), params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return out


def aupdate(record_id: str, fields: Dict[str, Any]) -> None:
    r = S.patch(
        airtable_url(f"{AIRTABLE_TABLE_NAME}/{record_id}"),
        headers=airtable_headers(),
        data=json.dumps({"fields": fields}),
        timeout=60,
    )
    r.raise_for_status()


def parse_airtable_date(v: Any) -> Optional[date]:
    if not v:
        return None
    if isinstance(v, str):
        # Airtable Date field usually returns YYYY-MM-DD
        try:
            return datetime.strptime(v, "%Y-%m-%d").date()
        except Exception:
            return None
    return None


def et_midnight_to_utc(d: date) -> datetime:
    et = ZoneInfo(ET_TZ)
    local = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=et)
    return local.astimezone(timezone.utc)


QUERY_ORDERS = """
query Orders($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: false) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        createdAt
        cancelledAt
        discountCodes { code }
        currentSubtotalPriceSet { shopMoney { amount currencyCode } }
        totalPriceSet { shopMoney { amount currencyCode } }
      }
    }
  }
}
"""


def norm(s: str) -> str:
    return (s or "").strip().upper()


def order_codes(order_node: Dict[str, Any]) -> List[str]:
    codes = order_node.get("discountCodes") or []
    out = []
    for c in codes:
        code = (c or {}).get("code")
        if code:
            out.append(norm(code))
    return out


def order_amount(order_node: Dict[str, Any]) -> float:
    key = "currentSubtotalPriceSet" if SALES_MODE == "subtotal" else "totalPriceSet"
    m = (((order_node.get(key) or {}).get("shopMoney")) or {})
    try:
        return float(m.get("amount"))
    except Exception:
        return 0.0


def main() -> None:
    if SALES_MODE not in ("subtotal", "total"):
        die("SALES_MODE must be 'subtotal' or 'total'.")

    if not (SHOPIFY_SHOP and SHOPIFY_ADMIN_TOKEN and AIRTABLE_TOKEN and AIRTABLE_BASE_ID):
        die("Missing required env vars.")

    today_et = datetime.now(ZoneInfo(ET_TZ)).date()
    cutoff = today_et - timedelta(days=LOOKBACK_DAYS)

    cutoff_minus_1 = (cutoff - timedelta(days=1)).isoformat()

    formula = (
        f"AND("
        f"{{{F['post_date']}}}!=BLANK(),"
        f"{{{F['code']}}}!=BLANK(),"
        f"IS_AFTER({{{F['post_date']}}}, '{cutoff_minus_1}')"
        f")"
    )

    recs = a_list_records(
        filter_formula=formula,
        fields=[F["post_date"], F["code"]],
        page_size=100
    )



    recs = a_list_records(filter_formula=formula, fields=[F["post_date"], F["code"]], page_size=100)
    print(f"Found {len(recs)} records since {cutoff.isoformat()} (ET).", flush=True)

    for i, rec in enumerate(recs, start=1):
        rid = rec["id"]
        af = rec.get("fields", {})

        d0 = parse_airtable_date(af.get(F["post_date"]))
        code = norm(af.get(F["code"]))

        if not d0 or not code:
            continue

        # Your rule: orders on D0 and D1 (ET calendar days)
        # So fetch created_at in [D0 00:00 ET, D2 00:00 ET)
        start_utc = et_midnight_to_utc(d0)
        end_utc = et_midnight_to_utc(d0 + timedelta(days=2))

        q = f"created_at:>={start_utc.isoformat()} created_at:<{end_utc.isoformat()}"

        after = None
        pages = 0
        orders = 0
        sales = 0.0

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
                if code in order_codes(o):
                    orders += 1
                    sales += order_amount(o)

            if not pi["hasNextPage"]:
                break
            after = pi["endCursor"]
            if not after:
                break
            time.sleep(SLEEP_SECONDS)

        update = {
            F["orders_out"]: orders,
            F["sales_out"]: round(sales, 2),
        }

        # Optional (only if you add this field in Airtable)
        if F.get("last_synced_at"):
            update[F["last_synced_at"]] = datetime.now(timezone.utc).isoformat()

        aupdate(rid, update)
        print(f"[{i}/{len(recs)}] {d0} code={code} orders={orders} sales={sales:.2f}", flush=True)
        time.sleep(SLEEP_SECONDS)

    print("Done.", flush=True)


if __name__ == "__main__":
    main()
