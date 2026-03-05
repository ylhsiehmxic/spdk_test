#!/usr/bin/env python3

import argparse
import csv
import re
from pathlib import Path

FNAME_RE = re.compile(
    r"^bdevperf_(?P<wowi>wo|wi)_(?P<rw>\w+)_(?P<cores>\d+)core_qd(?P<qd>\d+)_bs(?P<bs>[^.]+)\.txt$"
)

TOTAL_RE = re.compile(r"^\s*\^?M?\s*Total", re.IGNORECASE)


def parse_filename(name):
    m = FNAME_RE.match(name)
    if not m:
        return None
    d = m.groupdict()
    d["cores"] = int(d["cores"])
    d["qd"] = int(d["qd"])
    return d


def parse_total(text):
    for line in text.splitlines():

        if not TOTAL_RE.match(line):
            continue

        line = line.replace("^M", "").replace(":", " ")
        parts = line.split()

        nums = []
        for p in parts:
            try:
                nums.append(float(p))
            except:
                pass

        # runtime,iops,bw,fail,to,avg,min,max,stdev
        if len(nums) >= 9:
            bw = nums[2]
            avg = nums[5]
            min_v = nums[6]
            max_v = nums[7]
            stdev = nums[8]
            return bw, avg, min_v, max_v, stdev

        if len(nums) >= 8:
            bw = nums[2]
            avg = nums[5]
            min_v = nums[6]
            max_v = nums[7]
            stdev = 0.0
            return bw, avg, min_v, max_v, stdev

    return None


def main():

    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input_dir", default=".")
    ap.add_argument("-o", "--output", default="summary.csv")
    ap.add_argument("--glob", default="bdevperf_*.txt")
    args = ap.parse_args()

    files = sorted(Path(args.input_dir).glob(args.glob))

    rows = []

    for fp in files:

        meta = parse_filename(fp.name)
        if not meta:
            continue

        text = fp.read_text(errors="ignore")

        res = parse_total(text)
        if not res:
            continue

        bw, avg, min_v, max_v, stdev = res

        rows.append({
            "cores": meta["cores"],
            "wowi": meta["wowi"],
            "rw": meta["rw"],
            "qd": meta["qd"],
            "bs": meta["bs"],
            "bw": bw,
            "avg": avg,
            "min": min_v,
            "max": max_v,
            "stdev": stdev
        })

    # 數值排序
    rows.sort(key=lambda x: (x["cores"], x["wowi"], x["rw"], x["qd"]))

    fieldnames = [
        "cores",
        "wowi",
        "rw",
        "qd",
        "bs",
        "bw",
        "avg",
        "min",
        "max",
        "stdev"
    ]

    with open(args.output, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"parsed {len(rows)} files -> {args.output}")


if __name__ == "__main__":
    main()
