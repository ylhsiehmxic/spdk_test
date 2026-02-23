#!/usr/bin/env python3
import argparse
import csv
import re
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

# event token usually looks like BDEV_IO_START / BDEV_RAID_IO_DONE
EVENT_RE = re.compile(r"^[A-Z0-9_]+$")

def _first_non_empty(tokens: List[str], start: int) -> Optional[int]:
    for i in range(start, len(tokens)):
        if tokens[i] != "":
            return i
    return None

def parse_trace_line(line: str) -> Optional[Tuple[Dict[str, str], OrderedDict]]:
    """
    Returns:
      - row dict containing at least core, ts, event_type, obj, plus any key/value pairs
      - ordered_keys: OrderedDict of dynamic keys found in this row (preserves per-row discovery order)
    """
    line = line.rstrip("\n")
    if not line.strip():
        return None

    # split by TAB; preserve empty tokens from consecutive tabs
    tokens = line.split("\t")

    i0 = _first_non_empty(tokens, 0)
    if i0 is None:
        return None

    # Expect "0: 123.582"
    m = re.match(r"^\s*(\d+)\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*$", tokens[i0].strip())
    if not m:
        return None

    core = m.group(1)
    ts = m.group(2)

    # Next non-empty token: could be obj or event
    i1 = _first_non_empty(tokens, i0 + 1)
    if i1 is None:
        return ({"core": core, "ts": ts, "event_type": "", "obj": ""}, OrderedDict())

    t1 = tokens[i1].strip()
    obj = ""
    event_type = ""
    idx_after_event = i1 + 1

    if EVENT_RE.match(t1):
        # No obj
        event_type = t1
    else:
        obj = t1
        i2 = _first_non_empty(tokens, i1 + 1)
        if i2 is None:
            return ({"core": core, "ts": ts, "event_type": "", "obj": obj}, OrderedDict())
        event_type = tokens[i2].strip()
        idx_after_event = i2 + 1

    row: Dict[str, str] = {"core": core, "ts": ts, "event_type": event_type, "obj": obj}
    found_keys_in_row: "OrderedDict[str, None]" = OrderedDict()

    # Parse key/value pairs: key token endswith ":" and value token is next non-empty token
    i = idx_after_event
    while i < len(tokens):
        if tokens[i] == "":
            i += 1
            continue

        keytok = tokens[i].strip()
        if keytok.endswith(":"):
            key = keytok[:-1].strip()
            j = _first_non_empty(tokens, i + 1)
            if j is None:
                break
            val = tokens[j].strip()

            # store
            row[key] = val
            found_keys_in_row[key] = None

            i = j + 1
        else:
            i += 1

    return row, found_keys_in_row

def build_header(dynamic_key_order: "OrderedDict[str, None]") -> List[str]:
    # core,ts,event_type,obj + dynamic keys (except time) + time at end
    fixed_prefix = ["core", "ts", "event_type", "obj"]
    keys = list(dynamic_key_order.keys())

    # remove duplicates of fixed fields if any weird input includes them as keys
    keys = [k for k in keys if k not in fixed_prefix]

    # keep time last if it exists
    middle = [k for k in keys if k != "time"]
    header = fixed_prefix + middle + ["time"]
    return header

def main():
    ap = argparse.ArgumentParser(description="Parse SPDK spdk_trace text (TAB-delimited) into CSV with dynamic columns.")
    ap.add_argument("input", help="Input trace text file (use - for stdin)")
    ap.add_argument("-o", "--output", default="-", help="Output CSV file (default: stdout)")
    args = ap.parse_args()

    # We need 2-pass (or store rows) to know all keys before writing CSV header.
    rows: List[Dict[str, str]] = []
    dynamic_key_order: "OrderedDict[str, None]" = OrderedDict()

    fin = open(args.input, "r", encoding="utf-8", errors="replace") if args.input != "-" else __import__("sys").stdin
    for line in fin:
        parsed = parse_trace_line(line)
        if parsed is None:
            continue
        row, found_keys = parsed
        rows.append(row)
        # global key discovery order (first time ever seen wins)
        for k in found_keys.keys():
            if k not in dynamic_key_order:
                dynamic_key_order[k] = None

    if args.input != "-":
        fin.close()

    header = build_header(dynamic_key_order)

    fout = open(args.output, "w", newline="", encoding="utf-8") if args.output != "-" else __import__("sys").stdout
    w = csv.DictWriter(fout, fieldnames=header, extrasaction="ignore")
    w.writeheader()

    for row in rows:
        # Ensure all header fields exist
        out = {k: row.get(k, "") for k in header}
        w.writerow(out)

    if args.output != "-":
        fout.close()

if __name__ == "__main__":
    main()
