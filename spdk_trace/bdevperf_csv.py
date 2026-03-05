#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import re
from pathlib import Path
from typing import Optional, Dict, Any, List

# 檔名範例：
# bdevperf_wi_read_2core_qd16_bs1M.txt
# bdevperf_wo_read_2core_qd32_bs16K.txt
FNAME_RE = re.compile(
    r"^bdevperf_(?P<wowi>wi|wo)_(?P<rw>[A-Za-z0-9]+)_(?P<cores>\d+)core_qd(?P<qd>\d+)_bs(?P<bs>[A-Za-z0-9]+)\.txt$"
)

# 去掉 ANSI escape + control chars（你截圖裡有 ^M / 反白等）
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
CTRL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")  # 保留 \t \n \r 以外的控制碼


def clean_line(s: str) -> str:
    s = s.replace("\r", "")
    s = ANSI_RE.sub("", s)
    s = CTRL_RE.sub("", s)
    return s.strip()


def parse_filename(p: Path) -> Optional[Dict[str, Any]]:
    m = FNAME_RE.match(p.name)
    if not m:
        return None
    d = m.groupdict()
    return {
        "cores": int(d["cores"]),
        "wowi": d["wowi"],
        "rw": d["rw"],
        "qd": int(d["qd"]),
        "bs": d["bs"],
    }


def parse_total_line(lines: List[str]) -> Optional[Dict[str, float]]:
    """
    bdevperf 典型 Total 行（空白分隔）：
    Total : 5.00 476700.77 7448.45 0.00 0.00 66.92 16.37 204.80
            runtime IOPS    MiB/s   Fail TO    avg   min   max
    有些版本可能會在最後多一個 stdev（如果有就抓，沒有就留空）
    """
    total_line = None
    for raw in lines:
        s = clean_line(raw)
        if not s:
            continue
        # 可能是 "Total" 或 "Total:" 或前面有東西
        if re.search(r"\bTotal\b", s) and ":" in s:
            # 盡量挑最像表格那行（有很多數字）
            nums = re.findall(r"[-+]?\d+(?:\.\d+)?", s)
            if len(nums) >= 8:
                total_line = s
    if total_line is None:
        return None

    nums = [float(x) for x in re.findall(r"[-+]?\d+(?:\.\d+)?", total_line)]
    # 期望至少 8 個數字：runtime, iops, mib/s, fail/s, to/s, avg, min, max
    if len(nums) < 8:
        return None

    runtime, iops, mib_s, fail_s, to_s, avg, mn, mx = nums[:8]
    stdev = nums[8] if len(nums) >= 9 else None

    out = {
        "bw": mib_s,   # 你定義 bw = MiB/s
        "avg": avg,
        "min": mn,
        "max": mx,
    }
    if stdev is not None:
        out["stdev"] = stdev
    return out


def main():
    ap = argparse.ArgumentParser(
        description="Parse SPDK bdevperf txt outputs (bdevperf_*.txt) and summarize Total row into CSV."
    )
    ap.add_argument("--dir", required=True, help="放 bdevperf_*.txt 的資料夾")
    ap.add_argument("--out", default="bdevperf_summary.csv", help="輸出 CSV 檔名")
    args = ap.parse_args()

    in_dir = Path(args.dir)
    if not in_dir.is_dir():
        raise SystemExit(f"Not a directory: {in_dir}")

    rows = []
    skipped = []

    for p in sorted(in_dir.glob("bdevperf_*.txt")):
        meta = parse_filename(p)
        if meta is None:
            skipped.append((p.name, "filename_not_match"))
            continue

        try:
            text = p.read_text(errors="ignore").splitlines()
        except Exception as e:
            skipped.append((p.name, f"read_error:{e}"))
            continue

        total = parse_total_line(text)
        if total is None:
            skipped.append((p.name, "total_line_not_found_or_parse_failed"))
            continue

        row = {
            "cores": meta["cores"],
            "wowi": meta["wowi"],
            "rw": meta["rw"],
            "qd": meta["qd"],
            "bs": meta["bs"],
            "bw": total["bw"],
            "avg": total["avg"],
            "min": total["min"],
            "max": total["max"],
            "stdev": total.get("stdev", ""),  # 你要的欄位，若檔內沒提供就留空
        }
        rows.append(row)

    # 你指定的欄位順序
    fieldnames = ["cores", "wowi", "rw", "qd", "bs", "bw", "avg", "min", "max", "stdev"]

    out_path = Path(args.out)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"[OK] parsed {len(rows)} files -> {out_path}")
    if skipped:
        print(f"[WARN] skipped {len(skipped)} files:")
        for name, why in skipped[:50]:
            print(f"  - {name}: {why}")
        if len(skipped) > 50:
            print(f"  ... and {len(skipped)-50} more")


if __name__ == "__main__":
    main()
