#!/usr/bin/env python3
import argparse
import csv
import re
from typing import Dict, List, Tuple, Optional

EVENT_RE = re.compile(r"^[A-Z0-9_]+$")        # event_type like BDEV_IO_START
CORE_TS_RE = re.compile(r"^(\d+):\s*([0-9]+(?:\.[0-9]+)?)$")  # "0: 123.582"
PAREN_TOKEN_RE = re.compile(r"^\([^()]+\)$")  # "(R73)" "(i134)"

BASE_FIELDS = ["core", "ts", "event_type", "obj"]
TIME_FIELD = "time"

def split_spaces(line: str) -> List[str]:
    # Split by 1+ whitespace; preserves id values by later logic
    return re.findall(r"\S+", line.strip())

def parse_one_line(line: str) -> Optional[Tuple[Dict[str, str], List[str]]]:
    """
    Returns:
      row dict containing at least core/ts/event_type/obj (obj may be ""),
      and a list of newly-seen keys in encounter order for header growth.
    """
    line = line.strip()
    if not line:
        return None

    toks = split_spaces(line)
    if not toks:
        return None

    # First two tokens are: core:  ts
    if len(toks) < 2:
        return None
    m = CORE_TS_RE.match(toks[0] + " " + toks[1])  # won't match; handle separately
    # Actually toks[0] is "0:" and toks[1] is "123.582" in many outputs
    # But sometimes it's "0:" "123.582" indeed; parse that directly:
    if toks[0].endswith(":") and re.fullmatch(r"\d+", toks[0][:-1]) and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", toks[1]):
        core = toks[0][:-1]
        ts = toks[1]
        idx = 2
    else:
        # Or it might be a single token "0:"+"123.582" already combined (rare in your newer spec, but keep robust)
        m2 = CORE_TS_RE.match(toks[0])
        if not m2:
            return None
        core, ts = m2.group(1), m2.group(2)
        idx = 1

    # Next token(s): either [event_type] or [obj event_type]
    if idx >= len(toks):
        return None

    obj = ""
    event_type = ""

    t = toks[idx]
    if EVENT_RE.match(t):
        event_type = t
        idx += 1
    else:
        obj = t
        idx += 1
        if idx >= len(toks):
            return None
        event_type = toks[idx]
        idx += 1

    row: Dict[str, str] = {
        "core": core,
        "ts": ts,
        "event_type": event_type,
        "obj": obj,
    }

    new_keys: List[str] = []

    # Parse remaining tokens as key: value (space-separated, multiple spaces possible)
    while idx < len(toks):
        keytok = toks[idx]
        if not keytok.endswith(":"):
            idx += 1
            continue

        key = keytok[:-1]
        idx += 1
        if idx >= len(toks):
            break

        val = toks[idx]
        idx += 1

        # Special rule: id value may include a single "(...)" token after it
        if key == "id" and idx < len(toks) and PAREN_TOKEN_RE.match(toks[idx]):
            val = val + " " + toks[idx]
            idx += 1

        row[key] = val
        new_keys.append(key)

    return row, new_keys

def main():
    ap = argparse.ArgumentParser(description="Parse SPDK trace (space-delimited) into dynamic CSV.")
    ap.add_argument("input", help="Input trace file, or '-' for stdin")
    ap.add_argument("-o", "--output", default="-", help="Output CSV file, or '-' for stdout")
    args = ap.parse_args()

    fin = open(args.input, "r", encoding="utf-8", errors="replace") if args.input != "-" else None
    fout = open(args.output, "w", newline="", encoding="utf-8") if args.output != "-" else None

    rows: List[Dict[str, str]] = []
    key_order: List[str] = []  # encounter order for non-base keys
    seen_keys = set(BASE_FIELDS)

    try:
        in_f = fin if fin is not None else __import__("sys").stdin

        for line in in_f:
            parsed = parse_one_line(line)
            if parsed is None:
                continue
            row, new_keys = parsed
            rows.append(row)
            for k in new_keys:
                if k in seen_keys:
                    continue
                # We'll place TIME_FIELD at the end regardless, so track it but don't append now
                if k == TIME_FIELD:
                    seen_keys.add(k)
                    continue
                seen_keys.add(k)
                key_order.append(k)

        # Build header:
        # core,ts,event_type,obj, <dynamic keys except time>, time(last if present)
        header = BASE_FIELDS + key_order
        if any(TIME_FIELD in r for r in rows):
            header.append(TIME_FIELD)

        out_f = fout if fout is not None else __import__("sys").stdout
        w = csv.DictWriter(out_f, fieldnames=header)
        w.writeheader()

        for r in rows:
            out_row = {h: r.get(h, "") for h in header}
            w.writerow(out_row)

    finally:
        if fin is not None:
            fin.close()
        if fout is not None:
            fout.close()

if __name__ == "__main__":
    main()
