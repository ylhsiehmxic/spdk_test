#!/usr/bin/env python3
import argparse
import csv
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

# 你要的 4 種事件（用這些來組 6 個時間點）
E_BDEV_START = "BDEV_IO_START"
E_BDEV_DONE  = "BDEV_IO_DONE"
E_RAID_START = "BDEV_RAID_IO_START"
E_RAID_DONE  = "BDEV_RAID_IO_DONE"

# 解析 id value 允許一次括號： "i232 (R73)" / "R73 (i134)" / "i217" / "N/A"
ID_RE = re.compile(r"^\s*(\S+)(?:\s*\(\s*([^)]+)\s*\)\s*)?$")

def parse_id(value: str) -> Tuple[str, Optional[str]]:
    """
    return (main, in_paren)
    """
    m = ID_RE.match(value or "")
    if not m:
        return (value or "").strip(), None
    return m.group(1), (m.group(2).strip() if m.group(2) else None)

def ffloat(x: str) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def warn(msg: str):
    print(f"[WARN] {msg}")

def is_na_id(v: str) -> bool:
    return (v or "").strip().upper() == "N/A"

def row_compact_str(row: Dict[str, str], max_fields: int = 20) -> str:
    # 方便印在螢幕：只印有值的欄位
    items = [(k, row.get(k, "")) for k in row.keys() if row.get(k, "") != ""]
    items = items[:max_fields]
    return "{" + ", ".join([f"{k}={v}" for k, v in items]) + "}"

def main():
    ap = argparse.ArgumentParser(description="Analyze SPDK trace CSV and compute per-IO step latencies.")
    ap.add_argument("csv_in", help="Input CSV produced by your parser")
    ap.add_argument("-o", "--output", default="analysis.csv", help="Output analysis CSV")
    ap.add_argument("--max-gap-us", type=float, default=5000.0,
                    help="Max allowed time gap (us) when matching root start/done around raid interval (default 5000us)")
    args = ap.parse_args()

    # 讀入所有 rows（保留原始順序、同時記錄 row number = 1-based）
    rows: List[Dict[str, str]] = []
    with open(args.csv_in, "r", encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)

    total_rows = len(rows)

    # 找最後一次出現 id == N/A 的 row index（1-based）
    last_na_idx_1based = 0
    last_na_row: Optional[Dict[str, str]] = None
    for i, row in enumerate(rows, start=1):
        if is_na_id(row.get("id", "")):
            last_na_idx_1based = i
            last_na_row = row

    print(f"[INFO] Total rows in CSV: {total_rows}")
    if last_na_idx_1based > 0:
        print(f"[INFO] Last N/A row index (1-based): {last_na_idx_1based}")
        print(f"[INFO] Last N/A row content: {row_compact_str(last_na_row or {})}")
        # 只保留最後一次 N/A 之後的 rows
        rows = rows[last_na_idx_1based:]
        print(f"[INFO] Rows kept for analysis: {len(rows)} (starting from original row {last_na_idx_1based + 1})")
    else:
        print("[INFO] No N/A id row found. Analyze all rows.")
        print(f"[INFO] Rows kept for analysis: {len(rows)}")

    # 過濾：必備欄位 core, ts, event_type
    filtered_rows: List[Dict[str, str]] = []
    for row in rows:
        if not row.get("core") or not row.get("ts") or not row.get("event_type"):
            continue
        # 如果 id 是 N/A，理論上已經被切掉；但保險起見跳過
        if is_na_id(row.get("id", "")):
            continue
        filtered_rows.append(row)

    # 防呆：同一個 ID + event_type 出現多次 START/DONE -> 印到螢幕
    count_by_id_event = defaultdict(int)
    for row in filtered_rows:
        idv = row.get("id", "")
        evt = row.get("event_type", "")
        if idv and evt:
            count_by_id_event[(idv, evt)] += 1

    for (idv, evt), c in count_by_id_event.items():
        if evt in (E_BDEV_START, E_BDEV_DONE, E_RAID_START, E_RAID_DONE) and c > 1:
            warn(f"Duplicate event for same id: id='{idv}' event='{evt}' count={c}")

    # 分類蒐集事件
    raid_start_by_raidmain: Dict[str, Tuple[float, str]] = {}
    raid_done_by_raidmain: Dict[str, Tuple[float, str]] = {}

    bdev_starts_by_id = defaultdict(list)  # id -> list[(ts, obj)]
    bdev_dones_by_id  = defaultdict(list)

    for row in filtered_rows:
        evt = (row.get("event_type") or "").strip()
        ts = ffloat(row.get("ts", ""))
        if ts is None:
            continue

        if evt == E_RAID_START or evt == E_RAID_DONE:
            id_raw = row.get("id", "")  # e.g. "R3 (i217)"
            main, _paren = parse_id(id_raw)
            if evt == E_RAID_START:
                raid_start_by_raidmain[main] = (ts, id_raw)
            else:
                raid_done_by_raidmain[main] = (ts, id_raw)

        elif evt == E_BDEV_START:
            id_raw = row.get("id", "")
            if id_raw:
                bdev_starts_by_id[id_raw].append((ts, row.get("obj", "")))
        elif evt == E_BDEV_DONE:
            id_raw = row.get("id", "")
            if id_raw:
                bdev_dones_by_id[id_raw].append((ts, row.get("obj", "")))

    # sort lists
    for k in bdev_starts_by_id:
        bdev_starts_by_id[k].sort(key=lambda x: x[0])
    for k in bdev_dones_by_id:
        bdev_dones_by_id[k].sort(key=lambda x: x[0])

    def find_root_times(parent_id: str, raid_t1: float, raid_t4: float, max_gap: float) -> Tuple[Optional[float], Optional[float], str]:
        starts = bdev_starts_by_id.get(parent_id, [])
        dones  = bdev_dones_by_id.get(parent_id, [])

        t0 = None
        root_obj = ""

        for (ts, obj) in reversed(starts):
            if ts <= raid_t1 and (raid_t1 - ts) <= max_gap:
                t0 = ts
                root_obj = obj
                break

        t5 = None
        for (ts, obj) in dones:
            if ts >= raid_t4 and (ts - raid_t4) <= max_gap:
                t5 = ts
                if not root_obj:
                    root_obj = obj
                break

        return t0, t5, root_obj

    def find_base_pair(raid_t1: float, raid_t4: float, parent_id: str) -> Tuple[Optional[str], Optional[float], Optional[float], str]:
        best = None  # (duration, id, t2, t3, obj)
        for bid, starts in bdev_starts_by_id.items():
            if bid == parent_id:
                continue
            dones = bdev_dones_by_id.get(bid, [])
            if not starts or not dones:
                continue

            for (t2, obj2) in starts:
                if t2 < raid_t1 or t2 > raid_t4:
                    continue
                for (t3, obj3) in dones:
                    if t3 < t2:
                        continue
                    if t3 > raid_t4:
                        break
                    dur = t3 - t2
                    if best is None or dur < best[0]:
                        best = (dur, bid, t2, t3, obj2 or obj3)
                    break

        if best is None:
            return None, None, None, ""
        _, bid, t2, t3, obj = best
        return bid, t2, t3, obj

    out_fields = [
        "raid_id",
        "parent_bdev_id",
        "root_obj",
        "base_bdev_id",
        "base_obj",
        "t0_root_bdev_start",
        "t1_raid_start",
        "t2_base_bdev_start",
        "t3_base_bdev_done",
        "t4_raid_done",
        "t5_root_bdev_done",
        "step1_t1_minus_t0",
        "step2_t2_minus_t1",
        "step3_t3_minus_t2",
        "step4_t4_minus_t3",
        "step5_t5_minus_t4",
        "lat_root_bdev",
        "lat_raid",
        "lat_base_bdev",
    ]

    results: List[Dict[str, str]] = []

    for raid_main, (t1, raid_id_raw_start) in raid_start_by_raidmain.items():
        if raid_main not in raid_done_by_raidmain:
            warn(f"RAID id '{raid_main}' missing RAID_DONE")
            continue
        t4, _raid_id_raw_done = raid_done_by_raidmain[raid_main]

        # parent_id: 括號內那個（通常 ixxx）
        _, paren = parse_id(raid_id_raw_start)
        parent_id = paren or ""

        if not parent_id:
            warn(f"RAID id '{raid_main}' has no '(parent_id)' in id field: '{raid_id_raw_start}'")

        t0, t5, root_obj = (None, None, "")
        if parent_id:
            t0, t5, root_obj = find_root_times(parent_id, t1, t4, args.max_gap_us)

        base_id, t2, t3, base_obj = (None, None, None, "")
        if parent_id:
            base_id, t2, t3, base_obj = find_base_pair(t1, t4, parent_id)
        else:
            base_id, t2, t3, base_obj = find_base_pair(t1, t4, parent_id="__no_parent__")

        row_out = {
            "raid_id": raid_main,
            "parent_bdev_id": parent_id,
            "root_obj": root_obj,
            "base_bdev_id": base_id or "",
            "base_obj": base_obj or "",
            "t0_root_bdev_start": "" if t0 is None else f"{t0:.6f}",
            "t1_raid_start": f"{t1:.6f}",
            "t2_base_bdev_start": "" if t2 is None else f"{t2:.6f}",
            "t3_base_bdev_done": "" if t3 is None else f"{t3:.6f}",
            "t4_raid_done": f"{t4:.6f}",
            "t5_root_bdev_done": "" if t5 is None else f"{t5:.6f}",
            "step1_t1_minus_t0": "" if (t0 is None) else f"{(t1 - t0):.6f}",
            "step2_t2_minus_t1": "" if (t2 is None) else f"{(t2 - t1):.6f}",
            "step3_t3_minus_t2": "" if (t2 is None or t3 is None) else f"{(t3 - t2):.6f}",
            "step4_t4_minus_t3": "" if (t3 is None) else f"{(t4 - t3):.6f}",
            "step5_t5_minus_t4": "" if (t5 is None) else f"{(t5 - t4):.6f}",
            "lat_root_bdev": "" if (t0 is None or t5 is None) else f"{(t5 - t0):.6f}",
            "lat_raid": f"{(t4 - t1):.6f}",
            "lat_base_bdev": "" if (t2 is None or t3 is None) else f"{(t3 - t2):.6f}",
        }

        if t0 is None or t5 is None:
            warn(f"RAID '{raid_main}': cannot find root bdev start/done by parent_id='{parent_id}' within max_gap={args.max_gap_us}us")
        if t2 is None or t3 is None:
            warn(f"RAID '{raid_main}': cannot find base bdev start/done inside raid interval")

        results.append(row_out)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()
        for r in results:
            w.writerow(r)

    print(f"[OK] Wrote {len(results)} IO records to {args.output}")

if __name__ == "__main__":
    main()
