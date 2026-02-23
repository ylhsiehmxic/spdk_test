#!/usr/bin/env python3
import argparse
import csv
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

# 你要的 6 個事件
E_ROOT_START = "BDEV_IO_START"
E_ROOT_DONE  = "BDEV_IO_DONE"
E_RAID_START = "BDEV_RAID_IO_START"
E_RAID_DONE  = "BDEV_RAID_IO_DONE"

# 解析 id value 允許一次括號： "i232 (R73)" / "R73 (i134)" / "i217"
ID_RE = re.compile(r"^\s*(\S+)(?:\s*\(\s*([^)]+)\s*\)\s*)?$")

def parse_id(value: str) -> Tuple[str, Optional[str]]:
    """
    return (main, in_paren)
    """
    m = ID_RE.match(value or "")
    if not m:
        return value.strip(), None
    return m.group(1), (m.group(2).strip() if m.group(2) else None)

def ffloat(x: str) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def warn(msg: str):
    print(f"[WARN] {msg}")

def main():
    ap = argparse.ArgumentParser(description="Analyze SPDK trace CSV and compute per-IO step latencies.")
    ap.add_argument("csv_in", help="Input CSV produced by your parser")
    ap.add_argument("-o", "--output", default="analysis.csv", help="Output analysis CSV")
    ap.add_argument("--max-gap-us", type=float, default=5000.0,
                    help="Max allowed time gap (us) when matching root start/done around raid interval (default 5000us)")
    args = ap.parse_args()

    # 讀入所有 rows
    rows: List[Dict[str, str]] = []
    with open(args.csv_in, "r", encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f)
        for row in r:
            # 必備欄位：core, ts, event_type
            if not row.get("core") or not row.get("ts") or not row.get("event_type"):
                continue
            rows.append(row)

    # 按 (id, event_type) 做防呆統計
    count_by_id_event = defaultdict(int)
    for row in rows:
        idv = row.get("id", "")
        evt = row.get("event_type", "")
        if idv and evt:
            count_by_id_event[(idv, evt)] += 1

    for (idv, evt), c in count_by_id_event.items():
        # 你說「同一個 ID 若出現多次 START/DONE」視為異常
        if evt in (E_ROOT_START, E_ROOT_DONE, E_RAID_START, E_RAID_DONE) and c > 1:
            warn(f"Duplicate event for same id: id='{idv}' event='{evt}' count={c}")

    # 將 row 依 event 分類
    raid_start_by_raidmain = {}   # raid_main -> (ts, id_raw)
    raid_done_by_raidmain  = {}
    # root bdev events: keyed by bdev-id (例如 i217)
    bdev_starts_by_id = defaultdict(list)  # id -> list of (ts, obj)
    bdev_dones_by_id  = defaultdict(list)

    for row in rows:
        evt = row.get("event_type", "")
        ts = ffloat(row.get("ts", ""))
        if ts is None:
            continue

        if evt == E_RAID_START or evt == E_RAID_DONE:
            id_raw = row.get("id", "")  # 例如 "R3 (i217)"
            main, paren = parse_id(id_raw)
            # RAID id 的 main 多半是 Rxx；paren 多半是 ixxx（parent bdev id）
            if evt == E_RAID_START:
                raid_start_by_raidmain[main] = (ts, id_raw)
            else:
                raid_done_by_raidmain[main] = (ts, id_raw)

        elif evt == E_ROOT_START:
            id_raw = row.get("id", "")
            if id_raw:
                bdev_starts_by_id[id_raw].append((ts, row.get("obj", "")))
        elif evt == E_ROOT_DONE:
            id_raw = row.get("id", "")
            if id_raw:
                bdev_dones_by_id[id_raw].append((ts, row.get("obj", "")))

    # 排序，方便取最近
    for k in bdev_starts_by_id:
        bdev_starts_by_id[k].sort(key=lambda x: x[0])
    for k in bdev_dones_by_id:
        bdev_dones_by_id[k].sort(key=lambda x: x[0])

    # 找 base bdev（child）那對 start/done：
    # 在 RAID 區間內，找一個 bdev-id != parent_id 且有 start/done 的 pair，且 start < done，
    # 並且 start/done 都落在 [raid_start, raid_done]（允許一點點鬆動）
    def find_base_pair(raid_t1: float, raid_t4: float, parent_id: str) -> Tuple[Optional[str], Optional[float], Optional[float], str]:
        # 搜尋所有 bdev id（除了 parent）
        best = None  # (duration, id, t2, t3, obj)
        for bid, starts in bdev_starts_by_id.items():
            if bid == parent_id:
                continue
            dones = bdev_dones_by_id.get(bid, [])
            if not starts or not dones:
                continue

            # 取所有可能的配對（因為你說不 split，理論上只有一對）
            for (t2, obj2) in starts:
                if t2 < raid_t1 or t2 > raid_t4:
                    continue
                # 找第一個 done >= t2 且 <= raid_t4
                for (t3, obj3) in dones:
                    if t3 < t2:
                        continue
                    if t3 > raid_t4:
                        break
                    dur = t3 - t2
                    # 選最短的那個通常就是那筆 child/base IO
                    if best is None or dur < best[0]:
                        best = (dur, bid, t2, t3, obj2 or obj3)
                    break

        if best is None:
            return None, None, None, ""
        _, bid, t2, t3, obj = best
        return bid, t2, t3, obj

    # 找 root start/done：
    # 用 parent_id（通常在 RAID id 的括號裡）去找 BDEV_IO_START/DONE
    # root start：選最接近 raid_start 且 <= raid_start 的 start
    # root done ：選最接近 raid_done  且 >= raid_done  的 done
    def find_root_times(parent_id: str, raid_t1: float, raid_t4: float, max_gap: float) -> Tuple[Optional[float], Optional[float], str]:
        starts = bdev_starts_by_id.get(parent_id, [])
        dones  = bdev_dones_by_id.get(parent_id, [])

        t0 = None
        root_obj = ""

        # 找 t0
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

    # 對每個 raid_main（Rxx）輸出一筆分析
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
        t4, raid_id_raw_done = raid_done_by_raidmain[raid_main]

        # 解析 RAID id 拿 parent_id：以 start 的 id 為主
        _, paren = parse_id(raid_id_raw_start)
        parent_id = paren or ""

        if not parent_id:
            # 若括號沒有 ixxx，仍可運作，但 root matching 會弱很多
            warn(f"RAID id '{raid_main}' has no '(parent_id)' in id field: '{raid_id_raw_start}'")

        # 找 root
        t0, t5, root_obj = (None, None, "")
        if parent_id:
            t0, t5, root_obj = find_root_times(parent_id, t1, t4, args.max_gap_us)

        # 找 base pair
        base_id, t2, t3, base_obj = (None, None, None, "")
        if parent_id:
            base_id, t2, t3, base_obj = find_base_pair(t1, t4, parent_id)
        else:
            # 沒 parent_id 就退而求其次：找 raid interval 內任一對 bdev start/done
            base_id, t2, t3, base_obj = find_base_pair(t1, t4, parent_id="__no_parent__")

        def d(a: Optional[float], b: Optional[float]) -> str:
            return "" if (a is None or b is None) else f"{(a - b):.6f}"

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

        # 缺任何關鍵點都提示（但仍輸出，方便你看到哪裡漏）
        if t0 is None or t5 is None:
            warn(f"RAID '{raid_main}': cannot find root bdev start/done by parent_id='{parent_id}' within max_gap={args.max_gap_us}us")
        if t2 is None or t3 is None:
            warn(f"RAID '{raid_main}': cannot find base bdev start/done inside raid interval")

        results.append(row_out)

    # 寫出結果
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()
        for r in results:
            w.writerow(r)

    print(f"[OK] Wrote {len(results)} IO records to {args.output}")

if __name__ == "__main__":
    main()
