#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import csv
import glob
import os
import argparse
from typing import Optional, Dict, Any, Tuple

FNAME_RE = re.compile(
    r"""^fio_
        (?P<wowi>[^_]+)_
        (?P<rw>read|write|randread|randwrite|rw|readwrite|randrw)_
        (?P<cores>\d+)core_
        qd(?P<qd>\d+)_
        bs(?P<bs>[^.]+)
        \.txt$
    """,
    re.IGNORECASE | re.VERBOSE,
)

# e.g. "lat (usec): min=3951, max=36331, avg=12007.75, stdev=3422.69"
LAT_RE = re.compile(
    r"""^\s*lat\s*\((?P<unit>nsec|usec|msec|sec)\)\s*:\s*
        min=(?P<min>[\d.]+)\s*,\s*
        max=(?P<max>[\d.]+)\s*,\s*
        avg=(?P<avg>[\d.]+)\s*,\s*
        stdev=(?P<stdev>[\d.]+)
    """,
    re.IGNORECASE | re.VERBOSE | re.MULTILINE,
)

# Prefer MiB/s if present. Fio often prints: "bw=2666MiB/s (2795MB/s)"
BW_MIB_RE = re.compile(
    r"""\bbw\s*=\s*(?P<val>[\d.]+)\s*(?P<unit>KiB/s|MiB/s|GiB/s)\b""",
    re.IGNORECASE,
)

# Fallback: MB/s, KB/s, GB/s (decimal)
BW_DEC_RE = re.compile(
    r"""\bw\s*=\s*(?P<val>[\d.]+)\s*(?P<unit>KB/s|MB/s|GB/s)\b""",
    re.IGNORECASE,
)

UNIT_TO_MIB_BIN = {
    "kib/s": 1.0 / 1024.0,
    "mib/s": 1.0,
    "gib/s": 1024.0,
}

UNIT_TO_MIB_DEC = {
    "kb/s": (1_000.0 / 1024.0) / 1024.0,   # KB -> MiB
    "mb/s": (1_000_000.0 / 1024.0) / 1024.0,  # MB -> MiB
    "gb/s": (1_000_000_000.0 / 1024.0) / 1024.0,  # GB -> MiB
}

LAT_TO_USEC = {
    "nsec": 1.0 / 1000.0,
    "usec": 1.0,
    "msec": 1000.0,
    "sec": 1_000_000.0,
}


def parse_filename(fname: str) -> Optional[Dict[str, Any]]:
    base = os.path.basename(fname)
    m = FNAME_RE.match(base)
    if not m:
        return None
    d = m.groupdict()
    return {
        "wowi": d["wowi"],
        "rw": d["rw"].lower(),
        "cores": int(d["cores"]),
        "qd": int(d["qd"]),
        "bs": d["bs"],
    }


def parse_latency(text: str) -> Optional[Tuple[str, float, float, float, float]]:
    m = LAT_RE.search(text)
    if not m:
        return None
    unit = m.group("unit").lower()
    mn = float(m.group("min"))
    mx = float(m.group("max"))
    av = float(m.group("avg"))
    sd = float(m.group("stdev"))
    # Convert all to usec
    scale = LAT_TO_USEC.get(unit, 1.0)
    return (unit, av * scale, mn * scale, mx * scale, sd * scale)


def parse_bw_mib(text: str) -> Optional[float]:
    # Prefer binary units (KiB/MiB/GiB)
    m = BW_MIB_RE.search(text)
    if m:
        val = float(m.group("val"))
        unit = m.group("unit").lower()
        return val * UNIT_TO_MIB_BIN[unit]

    # Fallback to decimal units (KB/MB/GB)
    m = BW_DEC_RE.search(text)
    if m:
        val = float(m.group("val"))
        unit = m.group("unit").lower()
        return val * UNIT_TO_MIB_DEC[unit]

    return None


def process_file(path: str) -> Optional[Dict[str, Any]]:
    meta = parse_filename(path)
    if not meta:
        return None

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    bw = parse_bw_mib(text)
    lat = parse_latency(text)

    row = {
        "cores": meta["cores"],
        "wowi": meta["wowi"],
        "rw": meta["rw"],
        "qd": meta["qd"],
        "bs": meta["bs"],
        "bw": bw,  # MiB/s
        "avg": lat[1] if lat else None,   # usec
        "min": lat[2] if lat else None,   # usec
        "max": lat[3] if lat else None,   # usec
        "stdev": lat[4] if lat else None, # usec
        "file": os.path.basename(path),
    }
    return row


def main():
    ap = argparse.ArgumentParser(description="Parse fio_*.txt to CSV")
    ap.add_argument("-i", "--input", default=".", help="input dir (default: .)")
    ap.add_argument("-o", "--output", default="result.csv", help="output csv (default: result.csv)")
    ap.add_argument("--pattern", default="fio_*.txt", help="glob pattern (default: fio_*.txt)")
    args = ap.parse_args()

    paths = sorted(glob.glob(os.path.join(args.input, args.pattern)))
    rows = []
    skipped = []

    for p in paths:
        r = process_file(p)
        if r is None:
            skipped.append(os.path.basename(p))
        else:
            rows.append(r)

    # Output columns (as you requested)
    fieldnames = ["cores", "wowi", "rw", "qd", "bs", "bw", "avg", "min", "max", "stdev"]

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})

    print(f"OK: wrote {len(rows)} rows to {args.output}")
    if skipped:
        print(f"Skipped (filename not match pattern): {len(skipped)}")
        # print a few
        for s in skipped[:10]:
            print("  -", s)


if __name__ == "__main__":
    main()
