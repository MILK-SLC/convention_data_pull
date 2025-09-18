#!/usr/bin/env python3
import json, copy
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, quote
import requests
import pandas as pd
from dateutil import tz

BASE = "https://www.visitsaltlake.com"
API  = ("https://www.visitsaltlake.com/includes/rest_v2/plugins_events_events/find/?json="
        "%7B%22filter%22%3A%7B%22active%22%3Atrue%2C%22calendarid%22%3A%22conventions%22%2C%22%24and%22%3A%5B%7B%22categories.catId%22%3A%7B%22%24in%22%3A%5B%22conventions_2%22%2C%22conventions_14%22%2C%22conventions_20%22%2C%22conventions_151%22%2C%22conventions_155%22%2C%22conventions_6%22%2C%22conventions_5%22%2C%22conventions_19%22%2C%22conventions_8%22%2C%22conventions_13%22%2C%22conventions_3%22%2C%22conventions_9%22%2C%22conventions_11%22%2C%22conventions_4%22%2C%22conventions_7%22%2C%22conventions_40%22%2C%22conventions_153%22%2C%22conventions_21%22%2C%22conventions_1%22%5D%7D%7D%5D%2C%22dates%22%3A%7B%22%24elemMatch%22%3A%7B%22eventDate%22%3A%7B%22%24gte%22%3A%7B%22%24date%22%3A%222025-09-18T06%3A00%3A00.000Z%22%7D%2C%22%24lte%22%3A%7B%22%24date%22%3A%222027-09-18T06%3A00%3A00.000Z%22%7D%7D%7D%7D%7D%2C%22options%22%3A%7B%22limit%22%3A25%2C%22count%22%3Atrue%2C%22castDocs%22%3Afalse%2C%22fields%22%3A%7B%22_id%22%3A1%2C%22location%22%3A1%2C%22startDate%22%3A1%2C%22endDate%22%3A1%2C%22recurrence%22%3A1%2C%22categories%22%3A1%2C%22recurType%22%3A1%2C%22latitude%22%3A1%2C%22longitude%22%3A1%2C%22media_raw%22%3A1%2C%22recid%22%3A1%2C%22title%22%3A1%2C%22url%22%3A1%2C%22linkUrl%22%3A1%2C%22listing.title%22%3A1%2C%22listing.url%22%3A1%2C%22udfs_object.3550%22%3A1%2C%22udfs_object.3638%22%3A1%2C%22convention.roomattend%22%3A1%2C%22convention.showattend%22%3A1%2C%22convention.hostcompany%22%3A1%2C%22convention.hosturl%22%3A1%2C%22convention.facilities_raw%22%3A1%2C%22convention_facilities_ids%22%3A1%2C%22convention_facilities.recid%22%3A1%2C%22convention_facilities.title%22%3A1%2C%22convention_facilities.detail_type%22%3A1%2C%22convention_facilities.url%22%3A1%7D%2C%22hooks%22%3A%5B%22afterFind_convention_facilities%22%5D%2C%22sort%22%3A%7B%22startDate%22%3A1%2C%22title_sort%22%3A1%7D%7D%7D&token=9feaa8bf1573601ea1b096a94f2397a4")

MTZ = tz.gettz("America/Denver")

def parse_api_json(url):
    p = urlparse(url); q = parse_qs(p.query)
    return json.loads(q["json"][0]), q.get("token", [None])[0], p

def build_url(p, token, payload):
    q = parse_qs(p.query)
    q["json"][0] = json.dumps(payload, separators=(",", ":"))
    if token: q["token"] = [token]
    flat_q = {k: v[0] for k, v in q.items()}
    return urlunparse((p.scheme, p.netloc, p.path, p.params,
                       urlencode(flat_q, quote_via=quote), p.fragment))

def fetch_all(url, page_size=200):
    payload, token, parsed = parse_api_json(url)
    payload.setdefault("options", {}); payload["options"]["limit"] = page_size; payload["options"]["count"] = True
    r = requests.get(build_url(parsed, token, payload), timeout=30); r.raise_for_status()
    data = r.json(); total = data.get("docs", {}).get("count", 0); docs = data.get("docs", {}).get("docs", [])
    fetched = len(docs)
    while fetched < total:
        payload["options"]["skip"] = fetched
        rr = requests.get(build_url(parsed, token, payload), timeout=30); rr.raise_for_status()
        chunk = rr.json().get("docs", {}).get("docs", [])
        if not chunk: break
        docs.extend(chunk); fetched += len(chunk)
    return docs

def normalize(docs):
    def categories_join(xs): return ", ".join([x.get("catName","") for x in xs or [] if isinstance(x, dict)])
    def abs_url(u): return BASE + u if isinstance(u,str) and u.startswith("/") else (u or "")
    rows=[]
    for ev in docs:
        su = pd.to_datetime(ev.get("startDate"), utc=True, errors="coerce")
        eu = pd.to_datetime(ev.get("endDate"),   utc=True, errors="coerce")
        rows.append({
            "title": ev.get("title"),
            "start_local": su.tz_convert(MTZ) if pd.notna(su) else pd.NaT,
            "end_local":   eu.tz_convert(MTZ) if pd.notna(eu) else pd.NaT,
            "categories": categories_join(ev.get("categories")),
            "host_company": (ev.get("convention") or {}).get("hostcompany"),
            "primary_venue": (ev.get("convention_facilities") or [{}])[0].get("title",""),
            "event_url": abs_url(ev.get("url")),
        })
    df = pd.DataFrame(rows)
    # format datetimes so Google Sheets parses them
    for c in ("start_local","end_local"):
        if c in df:
            df[c] = df[c].dt.strftime("%Y-%m-%d %H:%M:%S%z").str.replace(r"(\d{2})(\d{2})$", r"\1:\2", regex=True)
    # sort by start date
    if "start_local" in df: df = df.sort_values("start_local", na_position="last")
    return df

def main():
    docs = fetch_all(API)
    df = normalize(docs)
    df.to_csv("data/conventions.csv", index=False)
    print("✅ Wrote data/conventions.csv with", len(df), "rows")

if __name__ == "__main__":
    main()

