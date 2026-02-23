#!/usr/bin/env python3
import argparse
import csv
import re
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

# core: ts  e.g. "0: 123.582"
CORE_TS_RE = re.compile(r"^\s*(\d+)\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*$")

# event_type usually looks like "BDEV_IO_START" / "BDEV_RAID_IO_DONE"
EVENT_RE = re.compile(r"^[A-Z0-9_]+$")

def split_ws(line: str) -> List[str]:
    """Split by arbitrary whitespace (multiple spaces/tabs)."""
    return re.findall(r"\S+", line.strip())

def parse_core_ts(tokens: List[str]) -> Tuple[Optional[str], Optional[str], int]:
    """
    Parse core and ts from the head tokens.
    Supports either:
      - token0 == "0:" and token1 == "123.582"
      - token0 == "0:123.582" / "0: 123.582" (rare)
    Returns (core, ts, next_index).
    """
    if not tokens:
        return None, None, 0

    # Case A: "0:" "123.582"
    if tokens[0].endswith(":") and len(tokens) >= 2:
        core_part = tokens[0][:-1]
        if core_part.isdigit() and re.match(r"^[0-9]+(?:\.[0-9]+)?$", tokens[1]):
            return core_part, tokens[1], 2

    # Case B: "0:123.582"
    m = re.match(r"^(\d+):([0-9]+(?:\.[0-9]+)?)$", tokens[0])
    if m:
        return m.group(1), m.group(2), 1

    # Case C: whole "0: 123.582" got merged into one token somehow
    m2 = CORE_TS_RE.match(tokens[0])
    if m2:
        return m2.group(1), m2.group(2), 1

    return None, None, 0

def parse_line(line: str) -> Optional[Dict[str, str]]:
    """
    Parse one trace line into a dict with mandatory keys:
      core, ts, event_type
    Optional:
      obj
    Dynamic:
      keys from "key:" "value" pairs
    """
    line = line.rstrip("\n")
    if not line.strip():
        return None

    tokens = split_ws(line)
    core, ts, i = parse_core_ts(tokens)
    if core is None or ts is None:
        return None  # not a trace line

    # Need event_type next; obj may exist.
    if i >= len(tokens):
        return None

    obj = ""
    event_type = ""

    # Strategy:
    # - Find the first token that looks like EVENT_RE and is NOT endingwith ":" (to avoid "size:")
    # - If the first candidate is at position i, then obj missing.
    # - If candidate is at i+1, then tokens[i] is obj.
    # - If further, we still handle: treat tokens before event as obj joined by space (rare but safe).
    event_idx = None
    for j in range(i, len(tokens)):
        t = tokens[j]
        if t.endswith(":"):
            continue
        if EVENT_RE.match(t):
            event_idx = j
            break

    if event_idx is None:
        return None

    if event_idx == i:
        event_type = tokens[i]
    else:
        # obj might be single token or multiple tokens (rare). Join them.
        obj = " ".join(tokens[i:event_idx])
        event_type = tokens[event_idx]

    row: Dict[str, str] = {
        "core": core,
        "ts": ts,
        "event_type": event_type,
        "obj": obj,
    }

    # Parse key/value pairs from tokens after event_idx
    k = event_idx + 1
    while k < len(tokens):
        keytok = tokens[k]
        if keytok.endswith(":"):
            key = keytok[:-1]
            # value is next token (if any)
            if k + 1 < len(tokens):
                val = tokens[k + 1]
                row[key] = val
                k += 2
            else:
                # dangling key without value
                row[key] = ""
                k += 1
        else:
            # stray token; skip
            k += 1

    return row

def collect_rows_and_keys(fin) -> Tuple[List[Dict[str, str]], List[str]]:
    rows: List[Dict[str, str]] = []
    dyn_keys = OrderedDict()  # preserves first-seen order

    for line in fin:
        row = parse_line(line)
        if row is None:
            continue
        rows.append(row)
        for k in row.keys():
            if k in ("core", "ts", "event_type", "obj"):
                continue
            if k == "time":
                continue  # force time to the end later
            if k not in dyn_keys:
                dyn_keys[k] = True

    return rows, list(dyn_keys.keys())

def main():
    ap = argparse.ArgumentParser(description="Parse SPDK trace (whitespace-delimited) into dynamic-column CSV.")
    ap.add_argument("input", help="Input trace file ('-' for stdin)")
    ap.add_argument("-o", "--output", default="-", help="Output CSV file ('-' for stdout)")
    args = ap.parse_args()

    inf = open(args.input, "r", encoding="utf-8", errors="replace") if args.input != "-" else None
    outf = open(args.output, "w", newline="", encoding="utf-8") if args.output != "-" else None

    try:
        fin = inf if inf is not None else __import__("sys").stdin
        fout = outf if outf is not None else __import__("sys").stdout

        rows, dyn_keys = collect_rows_and_keys(fin)

        # Final field order:
        # core,ts,event_type,obj, <dynamic keys>, time
        fields = ["core", "ts", "event_type", "obj"] + dyn_keys + ["time"]

        w = csv.DictWriter(fout, fieldnames=fields)
        w.writeheader()

        for r in rows:
            out = {f: r.get(f, "") for f in fields}
            w.writerow(out)

    finally:
        if inf is not None:
            inf.close()
        if outf is not None:
            outf.close()

if __name__ == "__main__":
    main()
