#!/usr/bin/env python3
import argparse
import ast
import sys
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import requests


def parse_platform_name(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    # Try to parse Python-like dicts from Excel cell (single quotes)
    try:
        obj = ast.literal_eval(s)
        if isinstance(obj, dict):
            # e.g. keys like web, iptv, smarttv, mobile
            return ",".join(sorted([str(k) for k in obj.keys()]))
    except Exception:
        pass
    # Fallback: return the raw string
    return s


def _is_missing_str(s: str) -> bool:
    ls = s.strip().lower()
    return ls in {"", "none", "nan", "null"}


def to_str(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if _is_missing_str(s):
        return None
    return s


def to_clean_str(value: Any) -> str:
    """Return a string, mapping None/NaN/'None'/'nan'/'null' to empty string."""
    s = to_str(value)
    return s if s is not None else ""


def to_int(value: Any) -> Optional[int]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def to_list_from_csv(value: Any) -> Optional[List[str]]:
    s = to_str(value)
    if not s:
        return None
    return [part.strip() for part in s.split(",") if part.strip()]


ALLOWED_INDEXES: Set[str] = {
    "cuepisode", "cumovie", "cuseason", "cuserie",
    "episode", "livetv", "movie", "program", "season", "serie",
}


def map_indexes_from_row(row: pd.Series) -> List[str]:
    # Prefer explicit 'indexes' column if present
    indexes_cell = row.get("indexes")
    indexes: List[str] = []
    if indexes_cell is not None and not (isinstance(indexes_cell, float) and pd.isna(indexes_cell)):
        parts = [p.strip().lower() for p in str(indexes_cell).split(",") if p.strip()]
        indexes.extend(parts)

    # Derive from 'type' if missing or to add common synonyms
    t = to_str(row.get("type"))
    if t:
        tl = t.lower()
        if tl == "movie":
            indexes.extend(["cumovie", "movie"])
        elif tl in ("episode", "ep", "part"):
            indexes.extend(["cuepisode", "episode"])
        elif tl in ("season",):
            indexes.extend(["cuseason", "season"])
        elif tl in ("series", "serie", "show"):
            indexes.extend(["cuserie", "serie"])
        elif tl in ("livetv", "live", "tv"):
            indexes.extend(["livetv"]) 
        elif tl in ("program",):
            indexes.extend(["program"]) 

    # Fallback
    if not indexes:
        indexes = ["cuepisode"]

    # Deduplicate and filter to allowed
    uniq = []
    seen = set()
    for ix in indexes:
        if ix and ix in ALLOWED_INDEXES and ix not in seen:
            uniq.append(ix)
            seen.add(ix)
    return uniq


def build_item_from_row(row: pd.Series) -> Dict[str, Any]:
    # Only include fields the API schema will accept
    name = to_clean_str(row.get("name")) or to_clean_str(row.get("engName"))
    description = to_clean_str(row.get("description")) or to_clean_str(row.get("engDescription"))
    content_id = to_int(row.get("contentId"))
    genres = to_list_from_csv(row.get("genres"))
    casts = to_list_from_csv(row.get("casts"))
    poster = to_clean_str(row.get("poster"))
    content_type = to_clean_str(row.get("type"))
    channel_genre = to_clean_str(row.get("channelGenre"))
    age_limit = to_clean_str(row.get("rtukRatingShort"))  # server maps to ageLimit; we set directly to avoid unknown prop
    year = None
    date_raw = to_str(row.get("date"))
    if date_raw and len(date_raw) >= 4:
        try:
            year = int(date_raw[:4])
        except Exception:
            pass
    platform_name = parse_platform_name(row.get("platform"))
    indexes = map_indexes_from_row(row)

    # Optional: build a semantic_text field for better retrieval
    semantic_parts = [p for p in [name, description, content_type, channel_genre] if p]
    if genres:
        semantic_parts.append(", ".join(genres))
    if casts:
        semantic_parts.append(", ".join(casts))
    semantic_text = " | ".join(semantic_parts) if semantic_parts else ""

    item: Dict[str, Any] = {}
    # Always include common text fields; send empty string if missing
    item["name"] = name
    item["description"] = description
    if content_id is not None:
        item["contentId"] = content_id
    if genres:
        item["genres"] = genres
    if casts:
        item["casts"] = casts
    item["poster"] = poster
    item["ageLimit"] = age_limit
    item["type"] = content_type
    item["channelGenre"] = channel_genre
    if year is not None:
        item["year"] = year
    # platform_name as text; map None to empty string
    item["platform_name"] = to_clean_str(platform_name)
    if indexes:
        item["indexes"] = indexes
    item["semantic_text"] = semantic_text

    return item


def send_row(session: requests.Session, api_url: str, user_id: str, collection: str, item: Dict[str, Any], timeout: int = 30) -> dict:
    url = f"{api_url.rstrip('/')}/db/{user_id}/import/{collection}"
    resp = session.post(url, json={"items": [item]}, timeout=timeout)
    try:
        data = resp.json()
    except Exception:
        data = {"error": f"Non-JSON response (status {resp.status_code})", "text": resp.text}
    if resp.status_code != 200 or data.get("error") or (data.get("errors") or []):
        raise RuntimeError(f"API error: status={resp.status_code}, data={data}")
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description="Import Excel rows into Elysia/Weaviate via API (row-by-row), routing to existing collections by indexes.")
    parser.add_argument("--excel", required=True, help="Path to Excel file (e.g., data.xlsx)")
    parser.add_argument("--sheet", default=0, help="Sheet name or index (default: 0)")
    parser.add_argument("--api-url", default="http://localhost:8000", help="Base API URL (default: http://localhost:8000)")
    parser.add_argument("--user-id", required=True, help="User ID used in API URLs")
    parser.add_argument("--collection", required=False, help="Optional: force a single target collection (overrides index routing)")
    parser.add_argument("--timeout", type=int, default=30, help="Request timeout seconds (default: 30)")
    args = parser.parse_args()

    # Load Excel
    try:
        df = pd.read_excel(args.excel, sheet_name=args.sheet, engine="openpyxl")
    except Exception as e:
        print(f"Failed to read Excel: {e}", file=sys.stderr)
        return 1

    total = len(df)
    success = 0
    failed = 0
    session = requests.Session()

    # Discover existing collections to avoid creating new ones
    existing: Set[str] = set()
    try:
        col_url = f"{args.api_url.rstrip('/')}/collections/{args.user_id}/list"
        r = session.get(col_url, timeout=args.timeout)
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        cols = data.get("collections", []) if isinstance(data, dict) else []
        for c in cols:
            name = c.get("name")
            if name:
                existing.add(str(name))
    except Exception as e:
        print(f"Warning: could not fetch collections list, proceeding without existence check: {e}", file=sys.stderr)

    force_collection = args.collection.strip() if args.collection else None

    for idx, row in df.iterrows():
        try:
            item = build_item_from_row(row)
            if not item:
                print(f"[{idx+1}/{total}] Skipped empty row")
                continue
            # Determine target collections
            targets: List[str]
            if force_collection:
                targets = [force_collection]
            else:
                idxs = item.get("indexes", [])
                targets = [ix for ix in idxs if not existing or ix in existing]

            if not targets:
                print(f"[{idx+1}/{total}] SKIP: no existing target collections for indexes={item.get('indexes')}")
                continue

            row_ok = True
            for coll in targets:
                try:
                    send_row(session, args.api_url, args.user_id, coll, item, timeout=args.timeout)
                except Exception as e:
                    row_ok = False
                    print(f"[{idx+1}/{total}] FAIL -> {coll}: {e}", file=sys.stderr)
            if row_ok:
                success += 1
                print(f"[{idx+1}/{total}] OK: {item.get('name','<no name>')} -> {', '.join(targets)}")
            else:
                failed += 1
        except Exception as e:
            failed += 1
            print(f"[{idx+1}/{total}] FAIL: {e}", file=sys.stderr)

    print(f"Done. Success: {success}, Failed: {failed}, Total: {total}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

#userid f4ca84e68ce93bc5b35f0606018c4881

#python importdata.py --excel "C:\path\to\your.xlsx" --user-id "<USER_ID>" --api-url "http://localhost:8000"