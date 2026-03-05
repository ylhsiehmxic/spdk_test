#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import re
from pathlib import Path
from typing import Dict, Optional, Tuple, List

# 檔名範例（你照片那種）
# bdevperf_wo_read_2core_qd32_bs16K.txt
# bdevperf_wi_read_2core_qd64_bs1M.txt
FNAME_RE = re.compile(
    r"""
    ^bdevperf_
    (?P<wowi>wo|wi)_
    (?P<rw>read|write|randread|randwrite)_
    (?P<cores>\d+)core_
    qd(?P<qd>\d+)_
    bs(?P<bs>[^.]+)
    \.txt$
    """,
    re.VERBOSE,
)

# bdevperf 的 Total 行大多長這樣（欄位間用空白分隔）：
# Total : 476700.77 7448.45 0.00 0.00 66.92 16.37 204.80
#        runtime(s)  IOPS   MiB/s Fail/s TO/s  avg   min   max
TOTAL_LINE_RE = re.compile(r"^\s*\^?M?\s*Total\b", re.IGNORECASE)

def parse_filename(name: str) -> Optional[Dict[str, str]]:
    m = FNAME_RE.match(name)
    if not m:
        return None
    d = m.groupdict()
    # 保留原始 bs 字串，如 4K/16K/64K/1M
    return d

def parse_total_metrics(text: str) -> Optional[Tuple[float, float, float, float]]:
    """
    回傳 (mib_s, lat_avg_us, lat_min_us, lat_max_us)
    找不到就回 None
    """
    for raw_line in text.splitlines():
        line = raw_line.replace("\x00", "")
        if not TOTAL_LINE_RE.match(line):
            continue

        # 去掉前綴像 "^M" 之類，並把 ":" 拿掉方便 split
        line2 = line.replace("^M", "").replace(":", " ")
        parts = line2.split()

        # parts 可能像：
        # ["Total", "476700.77", "7448.45", "0.00", "0.00", "66.92", "16.37", "204.80"]
        # 也可能多一個 "Total" 後面有別的符號；我們用「從尾巴取 6 個數」更穩
        nums: List[float] = []
        for p in parts:
            try:
                nums.append(float(p))
            except ValueError:
                pass

        # 需要至少：runtime, iops, mib/s, fail/s, to/s, avg, min, max => 8 個數
        if len(nums) < 8:
            # 有些版本 Total 行可能只顯示 avg/min/max + MiB/s 等，這裡先保守處理
            continue

        # 按典型順序取：
        # runtime = nums[0], iops = nums[1], mib/s = nums[2], fail = nums[3], to = nums[4], avg = nums[5], min = nums[6], max = nums[7]
        mib_s = nums[2]
        lat_avg = nums[5]
        lat_min = nums[6]
        lat_max = nums[7]
        return (mib_s, lat_avg, lat_min, lat_max)

    return None

def main():
    ap = argparse.ArgumentParser(
        description="Parse bdevperf txt files, extract Total MiB/s and latency avg/min/max, output CSV."
    )
    ap.add_argument("-i", "--input_dir", default=".", help="txt 檔所在目錄（預設當前目錄）")
    ap.add_argument("-o", "--output_csv", default="bdevperf_summary.csv", help="輸出 CSV 檔名")
    ap.add_argument("--glob", default="bdevperf_*.txt", help="檔名 glob（預設 bdevperf_*.txt）")
    ap.add_argument("--recursive", action="store_true", help="遞迴掃描子目錄")
    args = ap.parse_args()

    in_dir = Path(args.input_dir)
    if not in_dir.exists():
        raise SystemExit(f"input_dir not found: {in_dir}")

    pattern = f"**/{args.glob}" if args.recursive else args.glob
    files = sorted(in_dir.glob(pattern))

    rows = []
    skipped = []

    for fp in files:
        meta = parse_filename(fp.name)
        if meta is None:
            skipped.append((str(fp), "filename_not_match"))
            continue

        try:
            text = fp.read_text(errors="ignore")
        except Exception as e:
            skipped.append((str(fp), f"read_error:{e}"))
            continue

        metrics = parse_total_metrics(text)
        if metrics is None:
            skipped.append((str(fp), "total_not_found_or_parse_failed"))
            continue

        mib_s, lat_avg, lat_min, lat_max = metrics

        rows.append({
            "file": fp.name,
            "wowi": meta["wowi"],
            "rw": meta["rw"],
            "cores": int(meta["cores"]),
            "qd": int(meta["qd"]),
            "bs": meta["bs"],
            "MiB/s": mib_s,
            "lat_avg_us": lat_avg,
            "lat_min_us": lat_min,
            "lat_max_us": lat_max,
        })

    out_csv = Path(args.output_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # 固定欄位順序
    fieldnames = ["file", "wowi", "rw", "cores", "qd", "bs", "MiB/s", "lat_avg_us", "lat_min_us", "lat_max_us"]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"[OK] parsed {len(rows)} files -> {out_csv}")
    if skipped:
        print(f"[WARN] skipped {len(skipped)} files (show up to 20):")
        for p, reason in skipped[:20]:
            print(f"  - {p}: {reason}")

if __name__ == "__main__":
    main()
