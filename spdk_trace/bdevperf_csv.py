#!/usr/bin/env python3

import os
import re
import csv
import argparse
from pathlib import Path


def parse_filename(name):
    # bdevperf_wi_read_2core_qd16_bs16K.txt
    m = re.match(r"bdevperf_(wi|wo)_(\w+)_(\d+)core_qd(\d+)_bs(\w+)\.txt", name)
    if not m:
        return None

    return {
        "wowi": m.group(1),
        "rw": m.group(2),
        "cores": int(m.group(3)),
        "qd": int(m.group(4)),
        "bs": m.group(5),
    }


def parse_total(filepath):

    with open(filepath, errors="ignore") as f:
        lines = f.readlines()

    for line in lines:

        line = line.replace("\r", "").strip()

        if "Total" not in line:
            continue

        nums = re.findall(r"[-+]?\d+\.\d+|\d+", line)

        if len(nums) < 8:
            continue

        # runtime iops mib/s fail to avg min max
        bw = float(nums[2])
        avg = float(nums[5])
        min_v = float(nums[6])
        max_v = float(nums[7])

        stdev = ""
        if len(nums) >= 9:
            stdev = float(nums[8])

        return bw, avg, min_v, max_v, stdev

    return None


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default=".")
    parser.add_argument("--out", default="summary.csv")

    args = parser.parse_args()

    folder = Path(args.dir)

    rows = []

    for file in folder.glob("bdevperf_*.txt"):

        meta = parse_filename(file.name)

        if not meta:
            print("skip filename:", file.name)
            continue

        res = parse_total(file)

        if not res:
            print("no total found:", file.name)
            continue

        bw, avg, min_v, max_v, stdev = res

        rows.append([
            meta["cores"],
            meta["wowi"],
            meta["rw"],
            meta["qd"],
            meta["bs"],
            bw,
            avg,
            min_v,
            max_v,
            stdev
        ])

    with open(args.out, "w", newline="") as f:

        writer = csv.writer(f)

        writer.writerow([
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
        ])

        for r in rows:
            writer.writerow(r)

    print(f"\nparsed {len(rows)} files -> {args.out}")


if __name__ == "__main__":
    main()
