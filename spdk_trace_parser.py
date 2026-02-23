#!/usr/bin/env python3
import re
import csv
from typing import Dict, List, Optional

# tabs OR multiple spaces
SPLIT_RE = re.compile(r"\t+| {2,}")
CORE_RE = re.compile(r"^(?P<core>\d+):$")

# value like: "i173929 (R212)" or "R212 (i142422)"
REL_RE = re.compile(r"^(?P<main>.+?)\s*\((?P<rel>.+?)\)\s*$")


def split_rel(v: str) -> Dict[str, str]:
    """
    If v matches 'MAIN (REL)', return {'main': MAIN, 'rel': REL, 'has_rel': '1'}
    else return {'main': v, 'rel': '', 'has_rel': '0'}
    """
    m = REL_RE.match(v)
    if not m:
        return {"main": v, "rel": "", "has_rel": "0"}
    return {"main": m.group("main").strip(), "rel": m.group("rel").strip(), "has_rel": "1"}


def parse_line(line: str) -> Optional[Dict[str, str]]:
    line = line.rstrip("\n")
    if not line.strip():
        return None
    if line.lstrip().startswith("#"):
        return None

    parts = [p for p in SPLIT_RE.split(line.strip()) if p != ""]
    if len(parts) < 4:
        return None

    m = CORE_RE.match(parts[0])
    if not m:
        return None

    row: Dict[str, str] = {
        "core": m.group("core"),
        "ts": parts[1],
        "obj": parts[2],
        "event_type": parts[3],
    }

    kv_tokens = parts[4:]
    i = 0
    while i < len(kv_tokens):
        tok = kv_tokens[i]
        if tok.endswith(":"):
            key = tok[:-1]
            val = kv_tokens[i + 1] if i + 1 < len(kv_tokens) else ""
            row[key] = val

            # Also split "MAIN (REL)" into extra columns: <key>_main, <key>_rel
            s = split_rel(val)
            row[f"{key}_main"] = s["main"]
            row[f"{key}_rel"] = s["rel"]
            row[f"{key}_has_rel"] = s["has_rel"]

            i += 2
        else:
            row.setdefault("_extra", "")
            row["_extra"] += (tok if row["_extra"] == "" else " " + tok)
            i += 1

    return row


def parse_file(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            r = parse_line(line)
            if r:
                rows.append(r)
    return rows


def write_csv(rows: List[Dict[str, str]], out_csv: str) -> None:
    keys = set()
    for r in rows:
        keys.update(r.keys())

    fixed = ["core", "ts", "obj", "event_type"]
    others = sorted(k for k in keys if k not in fixed)
    header = fixed + others

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: spdk_trace_parser.py <trace.txt> <out.csv>")
        raise SystemExit(2)

    rows = parse_file(sys.argv[1])
    write_csv(rows, sys.argv[2])
    print(f"Parsed {len(rows)} rows -> {sys.argv[2]}")
