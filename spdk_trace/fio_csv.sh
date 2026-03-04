#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import glob
import os
import re
from typing import Optional, Dict, Any, List

FILENAME_RE = re.compile(
    r"""^fio_.*?
        _(?P<rw>read|write)_
        (?P<cores>\d+)core
        _qd(?P<qd>\d+)
        _bs(?P<bs>[^.]+)
        \.txt$
    """,
    re.IGNORECASE | re.VERBOSE,
)

LAT_RE = re.compile(
    r"""lat\s*\(\s*usec\s*\)\s*:\s*
        min=(?P<min>[\d.]+)\s*,\s*
        max=(?P<max>[\d.]+)\s*,\s*
        avg=(?P<avg>[\d.]+)\s*,\s*
        stdev=(?P<stdev>[\d.]+)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Examples:
#   WRITE: bw=2666MiB/s (2795MB/s), ...
#   READ:  bw=1.23GiB/s (1320MB/s), ...
BW_RE = re.compile(
    r"""\bbw\s*=\s*(?P<val>[\d.]+)\s*(?P<unit>[KMG]i?B/s)\b""",
    re.IGNORECASE,
)

def bw_to_mib_per_s(val: float, unit: str) -> float:
    u = unit.strip().lower()

    # Binary units: KiB/s MiB/s GiB/s
    if u == "kib/s":
        return val / 1024.0
    if u == "mib/s":
        return val
    if u == "gib/s":
        return val * 1024.0

    # Decimal units: KB/s MB/s GB/s  (convert to bytes then to MiB)
    # 1 MB = 1,000,000 bytes, 1 MiB = 1,048,576 bytes
    if u == "kb/s":
        bytes_per_s = val * 1_000
        return bytes_per_s / 1_048_576.0
    if u == "mb/s":
        bytes_per_s = val * 1_000_000
        return bytes_per_s / 1_048_576.0
    if u == "gb/s":
        bytes_per_s = val * 1_000_000_000
        return bytes_per_s / 1_048_576.0

    raise ValueError(f"Unknown bw unit: {unit}")

def parse_file(path: str) -> Optional[Dict[str, Any]]:
    base = os.path.basename(path)

    m = FILENAME_RE.match(base)
    if not m:
        # 不符合你這種命名規則就跳過
        return None

    meta = {
        "cores": int(m.group("cores")),
        "rw": m.group("rw").lower(),
        "qd": int(m.group("qd")),
        "bs": m.group("bs"),
    }

    text = ""
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    lat_m = LAT_RE.search(text)
    bw_m = BW_RE.search(text)

    if not lat_m or not bw_m:
        # 找不到關鍵欄位也先跳過（你也可以改成輸出空值）
        return None

    bw_val = float(bw_m.group("val"))
    bw_unit = bw_m.group("unit")
    bw_mib = bw_to_mib_per_s(bw_val, bw_unit)

    row = {
        **meta,
        "bw": round(bw_mib, 6),  # MiB/s
        "latency_avg": float(lat_m.group("avg")),
        "latency_min": float(lat_m.group("min")),
        "latency_max": float(lat_m.group("max")),
        "latency_stdev": float(lat_m.group("stdev")),
        "file": base,
    }
    return row

def main():
    ap = argparse.ArgumentParser(description="Parse fio_*.txt into CSV.")
    ap.add_argument("-i", "--input", default=".", help="input directory (default: current dir)")
    ap.add_argument("-o", "--output", default="fio_summary.csv", help="output CSV path")
    args = ap.parse_args()

    pattern = os.path.join(args.input, "fio_*.txt")
    paths = sorted(glob.glob(pattern))

    rows: List[Dict[str, Any]] = []
    skipped: List[str] = []

    for p in paths:
        r = parse_file(p)
        if r is None:
            skipped.append(os.path.basename(p))
            continue
        rows.append(r)

    fieldnames = [
        "cores",
        "rw",
        "qd",
        "bs",
        "bw",              # MiB/s
        "latency_avg",     # usec
        "latency_min",     # usec
        "latency_max",     # usec
        "latency_stdev",   # usec
        "file",
    ]

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Parsed {len(rows)} files -> {args.output}")
    if skipped:
        print(f"Skipped {len(skipped)} files (name/content not matched). Examples:")
        for s in skipped[:10]:
            print("  ", s)

if __name__ == "__main__":
    main()
