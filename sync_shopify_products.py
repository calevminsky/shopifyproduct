import os
import sys
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ----------------------------
# Env
# ----------------------------
SHOPIFY_SHOP = os.getenv("SHOPIFY_SHOP", "").strip()  # e.g. yakirabella.myshopify.com
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN", "").strip()
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01").strip()

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "").strip()
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "").strip()
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Products").strip()

MODE = os.getenv("MODE", "incremental").strip().lower()  # "backfill" or "incremental"
SKIP_ARCHIVED = os.getenv("SKIP_ARCHIVED", "true").lower() in ("1", "true", "yes")  # for backfill
SINCE_DAYS = int(os.getenv("SINCE_DAYS", "7"))  # for incremental
MAX_PRODUCTS = int(os.getenv("MAX_PRODUCTS", "200000"))  # safety cap
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.03"))

# Airtable field names (match your table columns)
F = {
    "gid": "Shopify Product GID",
    "handle": "Handle",
    "title": "Title",
    "status": "Status",
    "image_url": "Image URL",
    "image": "Image",
    "shopify_updated_at": "Shopify Updated At",
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
        die("Missing SHOPIFY_SHOP (e.g. yourstore.myshopify.com).")
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


def aget_by_formula(formula: str) -> Dict[str, Any]:
    params = {"filterByFormula": formula, "maxRecords": 1}
    r = S.get(airtable_url(AIRTABLE_TABLE_NAME), headers=airtable_headers(), params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def acreate(fields: Dict[str, Any]) -> str:
    r = S.post(
        airtable_url(AIRTABLE_TABLE_NAME),
        headers=airtable_headers(),
        data=json.dumps({"fields": fields}),
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["id"]


def aupdate(record_id: str, fields: Dict[str, Any]) -> None:
    r = S.patch(
        airtable_url(f"{AIRTABLE_TABLE_NAME}/{record_id}"),
        headers=airtable_headers(),
        data=json.dumps({"fields": fields}),
        timeout=60,
    )
    r.raise_for_status()


def upsert_by_gid(gid: str, fields: Dict[str, Any]) -> str:
    safe = gid.replace('"', '\\"')
    formula = f'{{{F["gid"]}}}="{safe}"'
    res = aget_by_formula(formula)
    recs = res.get("records", [])
    if recs:
        rid = recs[0]["id"]
        aupdate(rid, fields)
        return rid
    else:
        fields[F["gid"]] = gid
        return acreate(fields)


# ----------------------------
# Shopify query: cursor pagination + sorted by UPDATED_AT
# ----------------------------
QUERY_PRODUCTS = """
query Products($first: Int!, $after: String, $reverse: Boolean!) {
  products(first: $first, after: $after, sortKey: UPDATED_AT, reverse: $reverse) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        handle
        title
        status
        updatedAt
        featuredImage { url }
        images(first: 1) { edges { node { url } } }
      }
    }
  }
}
"""


def pick_image_url(node: Dict[str, Any]) -> Optional[str]:
    fi = node.get("featuredImage") or {}
    if fi.get("url"):
        return fi["url"]
    img_edges = (((node.get("images") or {}).get("edges")) or [])
    if img_edges:
        return (img_edges[0].get("node") or {}).get("url")
    return None


def parse_dt(s: str) -> datetime:
    # Shopify returns ISO8601 with timezone; Python can parse via fromisoformat with minor tweak
    # Example: 2025-01-02T03:04:05Z
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def should_process(status: str) -> bool:
    if MODE == "backfill" and SKIP_ARCHIVED and status.upper() == "ARCHIVED":
        return False
    return True


def main() -> None:
    if not SHOPIFY_SHOP:
        die("Missing SHOPIFY_SHOP.")
    if not SHOPIFY_ADMIN_TOKEN:
        die("Missing SHOPIFY_ADMIN_TOKEN.")
    if not AIRTABLE_TOKEN:
        die("Missing AIRTABLE_TOKEN.")
    if not AIRTABLE_BASE_ID:
        die("Missing AIRTABLE_BASE_ID.")

    since_dt: Optional[datetime] = None
    if MODE == "incremental":
        since_dt = datetime.now(timezone.utc) - timedelta(days=SINCE_DAYS)
        print(f"[incremental] scanning products updated since {since_dt.isoformat()}", flush=True)
    else:
        print(f"[backfill] scanning all products (SKIP_ARCHIVED={SKIP_ARCHIVED})", flush=True)

    first = 250  # Shopify typical max page size for connections
    after = None
    reverse = True  # newest updated first

    processed = 0
    seen = 0
    page = 0

    while True:
        page += 1
        data = shopify_graphql(QUERY_PRODUCTS, {"first": first, "after": after, "reverse": reverse})
        conn = data["data"]["products"]
        edges = conn["edges"]
        page_info = conn["pageInfo"]

        if not edges:
            print("No more products returned.", flush=True)
            break

        # In incremental mode, because we sort by UPDATED_AT desc,
        # we can stop when we hit older-than-since products consistently.
        stop_due_to_since = False

        for edge in edges:
            node = edge["node"]
            seen += 1

            gid = node["id"]
            status = (node.get("status") or "").upper()
            updated_at_raw = node.get("updatedAt") or ""
            updated_at = parse_dt(updated_at_raw) if updated_at_raw else None

            # Incremental early-stop
            if since_dt and updated_at and updated_at < since_dt:
                stop_due_to_since = True
                continue

            if not should_process(status):
                continue

            img_url = pick_image_url(node)
            now_iso = datetime.now(timezone.utc).isoformat()

            fields: Dict[str, Any] = {
                F["handle"]: node.get("handle"),
                F["title"]: node.get("title"),
                F["status"]: status,
                F["shopify_updated_at"]: updated_at.isoformat() if updated_at else None,
                F["last_synced_at"]: now_iso,
            }

            if img_url:
                fields[F["image_url"]] = img_url
                fields[F["image"]] = [{"url": img_url, "filename": f"{(node.get('handle') or gid)}.jpg"}]

            upsert_by_gid(gid, fields)
            processed += 1

            if processed % 50 == 0:
                print(f"Progress: processed={processed} seen={seen} page={page}", flush=True)

            if processed >= MAX_PRODUCTS:
                print(f"Reached MAX_PRODUCTS={MAX_PRODUCTS}; stopping.", flush=True)
                return

            time.sleep(SLEEP_SECONDS)

        # Early stop in incremental mode: once a page contains older results, we can quit
        if since_dt and stop_due_to_since:
            print(f"Reached products older than SINCE_DAYS={SINCE_DAYS}; stopping incremental scan.", flush=True)
            break

        if not page_info["hasNextPage"]:
            break

        after = page_info["endCursor"]
        if not after:
            # safety guard against pagination issues
            print("Pagination endCursor missing; stopping to avoid infinite loop.", flush=True)
            break

    print(f"Done. Processed={processed}, Seen={seen}, Pages={page}.", flush=True)


if __name__ == "__main__":
    main()
