#!/usr/bin/env python3
import argparse
import csv
import re
from typing import Dict, List, Optional

EVENT_RE = re.compile(r"^[A-Z0-9_]+$")  # e.g. BDEV_IO_START, BDEV_RAID_IO_DONE

OUT_FIELDS = ["core", "ts", "obj", "event_type", "size", "id", "type", "ctx", "offset", "qd", "time"]

KEY_MAP = {
    "size": "size",
    "id": "id",
    "type": "type",
    "ctx": "ctx",
    "offset": "offset",
    "qd": "qd",
    "time": "time",
}

INT_FIELDS = {"size", "type", "offset", "qd"}
FLOAT_FIELDS = {"ts", "time"}

def _first_non_empty(tokens: List[str], start: int) -> Optional[int]:
    for i in range(start, len(tokens)):
        if tokens[i] != "":
            return i
    return None

def parse_line(line: str) -> Optional[Dict[str, str]]:
    line = line.rstrip("\n")
    if not line.strip():
        return None

    # IMPORTANT: split by TAB, preserving empty fields from consecutive tabs
    tokens = line.split("\t")

    # Find first non-empty token -> "core: ts"
    i0 = _first_non_empty(tokens, 0)
    if i0 is None:
        return None
    core_ts = tokens[i0].strip()

    # Expect "0: 123.582"
    m = re.match(r"^\s*(\d+)\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*$", core_ts)
    if not m:
        # Not a trace line in expected format
        return None
    core = m.group(1)
    ts = m.group(2)

    # After core:ts, next non-empty token is either obj or event
    i1 = _first_non_empty(tokens, i0 + 1)
    if i1 is None:
        return {"core": core, "ts": ts}

    t1 = tokens[i1].strip()
    obj = ""
    event_type = ""

    if EVENT_RE.match(t1):
        # No obj; this is event
        event_type = t1
        idx_after_event = i1 + 1
    else:
        # Has obj; next non-empty must be event
        obj = t1
        i2 = _first_non_empty(tokens, i1 + 1)
        if i2 is None:
            return {"core": core, "ts": ts, "obj": obj}
        event_type = tokens[i2].strip()
        idx_after_event = i2 + 1

    row: Dict[str, str] = {
        "core": core,
        "ts": ts,
        "obj": obj,
        "event_type": event_type,
        "size": "",
        "id": "",
        "type": "",
        "ctx": "",
        "offset": "",
        "qd": "",
        "time": "",
    }

    # Parse key/value pairs: key token endswith ":" and value token is the next non-empty token
    i = idx_after_event
    while i < len(tokens):
        if tokens[i] == "":
            i += 1
            continue

        keytok = tokens[i].strip()
        if keytok.endswith(":"):
            key = keytok[:-1].strip()
            out_key = KEY_MAP.get(key)
            # Value is next non-empty token (value can include spaces inside the token)
            j = _first_non_empty(tokens, i + 1)
            if j is None:
                break
            val = tokens[j].strip()

            if out_key:
                row[out_key] = val
            i = j + 1
        else:
            # Sometimes there may be stray tokens; skip
            i += 1

    # Normalize numeric fields (optional, but nice): keep empty if missing
    for k in INT_FIELDS:
        if row.get(k, "") != "":
            # Accept ints like "1457"
            try:
                row[k] = str(int(float(row[k])))
            except ValueError:
                pass

    for k in FLOAT_FIELDS:
        if row.get(k, "") != "":
            try:
                row[k] = str(float(row[k]))
            except ValueError:
                pass

    return row

def main():
    ap = argparse.ArgumentParser(description="Parse SPDK spdk_trace text (TAB-delimited) into CSV.")
    ap.add_argument("input", help="Input trace text file (use - for stdin)")
    ap.add_argument("-o", "--output", default="-", help="Output CSV file (default: stdout)")
    args = ap.parse_args()

    inf = open(args.input, "r", encoding="utf-8", errors="replace") if args.input != "-" else None
    outf = open(args.output, "w", newline="", encoding="utf-8") if args.output != "-" else None

    try:
        fin = inf if inf is not None else __import__("sys").stdin
        fout = outf if outf is not None else __import__("sys").stdout
        w = csv.DictWriter(fout, fieldnames=OUT_FIELDS)
        w.writeheader()

        for line in fin:
            row = parse_line(line)
            if row is None:
                continue
            # Ensure all fields exist
            for f in OUT_FIELDS:
                row.setdefault(f, "")
            w.writerow({f: row.get(f, "") for f in OUT_FIELDS})
    finally:
        if inf is not None:
            inf.close()
        if outf is not None:
            outf.close()

if __name__ == "__main__":
    main()
