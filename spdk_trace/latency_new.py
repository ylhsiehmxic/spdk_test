#!/usr/bin/env python3
import argparse
import csv
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

E_BDEV_START = "BDEV_IO_START"
E_BDEV_DONE  = "BDEV_IO_DONE"
E_RAID_START = "BDEV_RAID_IO_START"
E_RAID_DONE  = "BDEV_RAID_IO_DONE"

# id value 允許一次括號： "i232 (R73)" / "R73 (i134)" / "i217" / "N/A"
ID_RE = re.compile(r"^\s*(\S+)(?:\s*\(\s*([^)]+)\s*\)\s*)?$")

def parse_id(value: str) -> Tuple[str, Optional[str]]:
    v = (value or "").strip()
    m = ID_RE.match(v)
    if not m:
        return v, None
    return m.group(1), (m.group(2).strip() if m.group(2) else None)

def ffloat(x: str) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def warn(msg: str):
    print(f"[WARN] {msg}")

def info(msg: str):
    print(f"[INFO] {msg}")

def is_na_id(v: str) -> bool:
    return (v or "").strip().upper() == "N/A"

def row_compact_str(row: Dict[str, str], max_fields: int = 30) -> str:
    if not row:
        return "{}"
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

    # 讀入所有 rows（保留原始順序、1-based row index）
    rows: List[Dict[str, str]] = []
    with open(args.csv_in, "r", encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)

    total_rows = len(rows)

    # 找最後一次 id == N/A 的 row index（1-based）
    last_na_idx = 0
    last_na_row: Optional[Dict[str, str]] = None
    for i, row in enumerate(rows, start=1):
        if is_na_id(row.get("id", "")):
            last_na_idx = i
            last_na_row = row

    info(f"Total rows in CSV: {total_rows}")
    if last_na_idx > 0:
        info(f"Last N/A row index (1-based): {last_na_idx}")
        info(f"Last N/A row content: {row_compact_str(last_na_row)}")
        # 丟掉 (含 N/A 那行) 之前的
        rows = rows[last_na_idx:]
        info(f"Rows kept for analysis: {len(rows)} (starting from original row {last_na_idx + 1})")
    else:
        info("No N/A id row found. Analyze all rows.")
        info(f"Rows kept for analysis: {len(rows)}")

    # 過濾必備欄位 + 避免殘留 N/A
    filtered: List[Dict[str, str]] = []
    for row in rows:
        if not row.get("core") or not row.get("ts") or not row.get("event_type"):
            continue
        if is_na_id(row.get("id", "")):
            continue
        filtered.append(row)

    # 防呆：同一個 raw id + event_type 出現多次（限我們關心的 4 類事件）
    cnt = defaultdict(int)
    for row in filtered:
        rid = (row.get("id") or "").strip()
        evt = (row.get("event_type") or "").strip()
        if rid and evt in (E_BDEV_START, E_BDEV_DONE, E_RAID_START, E_RAID_DONE):
            cnt[(rid, evt)] += 1
    for (rid, evt), c in cnt.items():
        if c > 1:
            warn(f"Duplicate event for same raw id: id='{rid}' event='{evt}' count={c}")

    # --- 收集事件 ---
    # RAID: 用 raid_main (e.g. R5917) 當 key
    raid_start: Dict[str, Tuple[float, str]] = {}
    raid_done:  Dict[str, Tuple[float, str]] = {}

    # BDEV events：用 raw id 字串當 key（因為可能長得像 "i1314 (R5917)"）
    bdev_starts_by_rawid = defaultdict(list)  # rawid -> list[(ts, obj, core)]
    bdev_dones_by_rawid  = defaultdict(list)

    # 索引：raid_main -> bdev rawid candidates (where rawid has "(raid_main)")
    bdev_rawids_by_raid = defaultdict(set)

    for row in filtered:
        evt = (row.get("event_type") or "").strip()
        ts = ffloat(row.get("ts", ""))
        if ts is None:
            continue
        rawid = (row.get("id") or "").strip()
        obj = (row.get("obj") or "").strip()
        core = (row.get("core") or "").strip()

        if evt in (E_RAID_START, E_RAID_DONE):
            raid_main, _raid_paren = parse_id(rawid)  # "Rxxxx (iYYYY)" => main=Rxxxx
            if evt == E_RAID_START:
                raid_start[raid_main] = (ts, rawid)
            else:
                raid_done[raid_main] = (ts, rawid)

        elif evt == E_BDEV_START:
            if rawid:
                bdev_starts_by_rawid[rawid].append((ts, obj, core))
                _main, paren = parse_id(rawid)
                if paren and paren.startswith("R"):
                    bdev_rawids_by_raid[paren].add(rawid)

        elif evt == E_BDEV_DONE:
            if rawid:
                bdev_dones_by_rawid[rawid].append((ts, obj, core))
                _main, paren = parse_id(rawid)
                if paren and paren.startswith("R"):
                    bdev_rawids_by_raid[paren].add(rawid)

    # sort
    for rid in bdev_starts_by_rawid:
        bdev_starts_by_rawid[rid].sort(key=lambda x: x[0])
    for rid in bdev_dones_by_rawid:
        bdev_dones_by_rawid[rid].sort(key=lambda x: x[0])

    def find_root_times(parent_id: str, t1: float, t4: float, max_gap: float) -> Tuple[Optional[float], Optional[float], str, str, str]:
        """
        returns (t0, t5, root_obj, core_at_start, core_at_done)
        """
        starts = bdev_starts_by_rawid.get(parent_id, [])
        dones  = bdev_dones_by_rawid.get(parent_id, [])

        t0 = None
        root_obj = ""
        core0 = ""
        for (ts, obj, core) in reversed(starts):
            if ts <= t1 and (t1 - ts) <= max_gap:
                t0 = ts
                root_obj = obj
                core0 = core
                break

        t5 = None
        core5 = ""
        for (ts, obj, core) in dones:
            if ts >= t4 and (ts - t4) <= max_gap:
                t5 = ts
                if not root_obj:
                    root_obj = obj
                core5 = core
                break

        return t0, t5, root_obj, core0, core5

    def find_base_pair_for_raid(raid_main: str, t1: float, t4: float) -> Tuple[Optional[str], Optional[float], Optional[float], str, str, str]:
        """
        returns (base_rawid, t2, t3, base_obj, base_core_at_start, base_core_at_done)
        """
        candidates = list(bdev_rawids_by_raid.get(raid_main, set()))
        if not candidates:
            return None, None, None, "", "", ""

        best = None  # (dur, rawid, t2, t3, obj, core2, core3)
        multi_pairs = 0

        for rawid in candidates:
            starts = bdev_starts_by_rawid.get(rawid, [])
            dones  = bdev_dones_by_rawid.get(rawid, [])
            if not starts or not dones:
                continue

            for (t2, obj2, core2) in starts:
                if not (t1 <= t2 <= t4):
                    continue
                for (t3, obj3, core3) in dones:
                    if t3 < t2:
                        continue
                    if t3 > t4:
                        break
                    dur = t3 - t2
                    multi_pairs += 1
                    if best is None or dur < best[0]:
                        best = (dur, rawid, t2, t3, obj2 or obj3, core2, core3)
                    break

        if multi_pairs > 1:
            warn(f"RAID '{raid_main}': multiple base pairs found inside raid interval (pairs={multi_pairs}). Choose the shortest one.")

        if best is None:
            return None, None, None, "", "", ""
        _, rawid, t2, t3, obj, core2, core3 = best
        return rawid, t2, t3, obj, core2, core3

    out_fields = [
        "raid_id",
        "parent_bdev_id",
        "root_obj",
        "root_core_start",
        "root_core_done",
        "base_bdev_id",
        "base_obj",
        "base_core_start",
        "base_core_done",
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

    for raid_main, (t1, raid_rawid_start) in raid_start.items():
        if raid_main not in raid_done:
            warn(f"RAID '{raid_main}': missing RAID_DONE")
            continue
        t4, _raid_rawid_done = raid_done[raid_main]

        # parent_id from RAID id's paren: "R5917 (i3313)" -> parent=i3313
        _main, parent_id = parse_id(raid_rawid_start)
        parent_id = parent_id or ""

        if not parent_id:
            warn(f"RAID '{raid_main}': raid start id has no '(parent_id)': '{raid_rawid_start}'")

        # root times + cores
        t0, t5, root_obj, root_core0, root_core5 = (None, None, "", "", "")
        if parent_id:
            t0, t5, root_obj, root_core0, root_core5 = find_root_times(parent_id, t1, t4, args.max_gap_us)

        # base pair + cores (STRICT bind by raid_main via "(Rxxxx)")
        base_rawid, t2, t3, base_obj, base_core2, base_core3 = find_base_pair_for_raid(raid_main, t1, t4)

        row_out = {
            "raid_id": raid_main,
            "parent_bdev_id": parent_id,
            "root_obj": root_obj,
            "root_core_start": root_core0,
            "root_core_done": root_core5,
            "base_bdev_id": base_rawid or "",
            "base_obj": base_obj or "",
            "base_core_start": base_core2,
            "base_core_done": base_core3,
            "t0_root_bdev_start": "" if t0 is None else f"{t0:.6f}",
            "t1_raid_start": f"{t1:.6f}",
            "t2_base_bdev_start": "" if t2 is None else f"{t2:.6f}",
            "t3_base_bdev_done": "" if t3 is None else f"{t3:.6f}",
            "t4_raid_done": f"{t4:.6f}",
            "t5_root_bdev_done": "" if t5 is None else f"{t5:.6f}",
            "step1_t1_minus_t0": "" if t0 is None else f"{(t1 - t0):.6f}",
            "step2_t2_minus_t1": "" if t2 is None else f"{(t2 - t1):.6f}",
            "step3_t3_minus_t2": "" if (t2 is None or t3 is None) else f"{(t3 - t2):.6f}",
            "step4_t4_minus_t3": "" if t3 is None else f"{(t4 - t3):.6f}",
            "step5_t5_minus_t4": "" if t5 is None else f"{(t5 - t4):.6f}",
            "lat_root_bdev": "" if (t0 is None or t5 is None) else f"{(t5 - t0):.6f}",
            "lat_raid": f"{(t4 - t1):.6f}",
            "lat_base_bdev": "" if (t2 is None or t3 is None) else f"{(t3 - t2):.6f}",
        }

        if t0 is None or t5 is None:
            warn(f"RAID '{raid_main}': cannot find root bdev start/done by parent_id='{parent_id}' within max_gap={args.max_gap_us}us")
        if t2 is None or t3 is None:
            warn(f"RAID '{raid_main}': cannot find base bdev start/done bound to '( {raid_main} )' inside raid interval")

        results.append(row_out)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()
        for r in results:
            w.writerow(r)

    print(f"[OK] Wrote {len(results)} IO records to {args.output}")

if __name__ == "__main__":
    main()
