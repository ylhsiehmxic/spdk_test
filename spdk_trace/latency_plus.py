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

# (ts, obj, core)
EventRec = Tuple[float, str, str]
# (rawid, t2, t3, obj, core2, core3, dur)
ChildPair = Tuple[str, float, float, str, str, str, float]

def fmt(x: Optional[float]) -> str:
    return "" if x is None else f"{x:.6f}"

def main():
    ap = argparse.ArgumentParser(description="Analyze SPDK trace CSV and compute per-IO step latencies (agg + per-child).")
    ap.add_argument("csv_in", help="Input CSV produced by your parser")
    ap.add_argument("--output-agg", default="analysis_agg.csv", help="Output CSV (one row per RAID IO, choose slowest child)")
    ap.add_argument("--output-child", default="analysis_child.csv", help="Output CSV (one row per child IO)")
    ap.add_argument("--max-gap-us", type=float, default=5000.0,
                    help="Max allowed time gap (us) when matching root start/done around raid interval (default 5000us)")
    args = ap.parse_args()

    # ---- read all rows ----
    rows: List[Dict[str, str]] = []
    with open(args.csv_in, "r", encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)

    total_rows = len(rows)

    # ---- find last N/A row index (focused events only) ----
    FOCUS_EVENTS = {E_BDEV_START, E_BDEV_DONE, E_RAID_START, E_RAID_DONE}

    last_na_idx = 0
    last_na_row: Optional[Dict[str, str]] = None
    for i, row in enumerate(rows, start=1):
        evt = (row.get("event_type") or "").strip()
        if evt in FOCUS_EVENTS and is_na_id(row.get("id", "")):
            last_na_idx = i
            last_na_row = row

    info(f"Total rows in CSV: {total_rows}")
    if last_na_idx > 0:
        info(f"Last N/A row index (1-based, focused events): {last_na_idx}")
        info(f"Last N/A row content: {row_compact_str(last_na_row)}")
        # drop rows <= last_na_idx
        rows = rows[last_na_idx:]
        info(f"Rows kept for analysis: {len(rows)} (starting from original row {last_na_idx + 1})")
    else:
        info("No N/A id row found (within focused events). Analyze all rows.")
        info(f"Rows kept for analysis: {len(rows)}")

    # ---- basic filter ----
    filtered: List[Dict[str, str]] = []
    for row in rows:
        if not row.get("core") or not row.get("ts") or not row.get("event_type"):
            continue
        if is_na_id(row.get("id", "")):
            continue
        filtered.append(row)

    # ---- duplicate check (focused events only) ----
    cnt = defaultdict(int)
    for row in filtered:
        rid = (row.get("id") or "").strip()
        evt = (row.get("event_type") or "").strip()
        if rid and evt in FOCUS_EVENTS:
            cnt[(rid, evt)] += 1
    for (rid, evt), c in cnt.items():
        if c > 1:
            warn(f"Duplicate event for same raw id: id='{rid}' event='{evt}' count={c}")

    # ---- collect events ----
    raid_start: Dict[str, Tuple[float, str]] = {}
    raid_done:  Dict[str, Tuple[float, str]] = {}

    # BDEV events by raw id
    bdev_starts_by_rawid: Dict[str, List[EventRec]] = defaultdict(list)
    bdev_dones_by_rawid:  Dict[str, List[EventRec]] = defaultdict(list)

    # ✅ NEW: BDEV events by id_main (to match parent i30 vs i30 (u6))
    bdev_starts_by_main: Dict[str, List[EventRec]] = defaultdict(list)
    bdev_dones_by_main:  Dict[str, List[EventRec]] = defaultdict(list)

    # raid_main -> set(rawid) where rawid has "(raid_main)" (child IOs)
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
            raid_main, _paren = parse_id(rawid)  # "Rxxxx (iYYYY)" => main=Rxxxx
            if evt == E_RAID_START:
                raid_start[raid_main] = (ts, rawid)
            else:
                raid_done[raid_main] = (ts, rawid)

        elif evt == E_BDEV_START:
            if not rawid:
                continue
            bdev_starts_by_rawid[rawid].append((ts, obj, core))

            main, paren = parse_id(rawid)
            if main:
                bdev_starts_by_main[main].append((ts, obj, core))

            if paren and paren.startswith("R"):
                bdev_rawids_by_raid[paren].add(rawid)

        elif evt == E_BDEV_DONE:
            if not rawid:
                continue
            bdev_dones_by_rawid[rawid].append((ts, obj, core))

            main, paren = parse_id(rawid)
            if main:
                bdev_dones_by_main[main].append((ts, obj, core))

            if paren and paren.startswith("R"):
                bdev_rawids_by_raid[paren].add(rawid)

    # sort all lists
    for k in bdev_starts_by_rawid:
        bdev_starts_by_rawid[k].sort(key=lambda x: x[0])
    for k in bdev_dones_by_rawid:
        bdev_dones_by_rawid[k].sort(key=lambda x: x[0])

    for k in bdev_starts_by_main:
        bdev_starts_by_main[k].sort(key=lambda x: x[0])
    for k in bdev_dones_by_main:
        bdev_dones_by_main[k].sort(key=lambda x: x[0])

    # ---- root matching ----
    # 你要求：若有多組候選 (t0,t5)，用 dur=(t5-t0) 取最大
    def find_root_times(parent_id: str, t1: float, t4: float, max_gap: float) -> Tuple[Optional[float], Optional[float], str, str, str]:
        """
        returns (t0, t5, root_obj, core0, core5)
        Matching uses id_main aggregation (so parent i30 matches rawid i30 and i30(u6)).
        """
        starts = bdev_starts_by_main.get(parent_id, [])
        dones  = bdev_dones_by_main.get(parent_id, [])

        cand_t0 = [(ts, obj, core) for (ts, obj, core) in starts if ts <= t1 and (t1 - ts) <= max_gap]
        cand_t5 = [(ts, obj, core) for (ts, obj, core) in dones  if ts >= t4 and (ts - t4) <= max_gap]

        if not cand_t0 or not cand_t5:
            return None, None, "", "", ""

        best = None  # (dur, t0, t5, obj, core0, core5)
        for (t0, obj0, core0) in cand_t0:
            for (t5, obj5, core5) in cand_t5:
                if t5 < t0:
                    continue
                dur = t5 - t0
                if best is None or dur > best[0]:
                    obj = obj0 or obj5
                    best = (dur, t0, t5, obj, core0, core5)

        if best is None:
            return None, None, "", "", ""
        _, t0, t5, obj, core0, core5 = best
        return t0, t5, obj, core0, core5

    # ---- child pairing ----
    def get_child_pairs_for_raid(raid_main: str, t1: float, t4: float) -> List[ChildPair]:
        pairs: List[ChildPair] = []
        candidates = list(bdev_rawids_by_raid.get(raid_main, set()))
        if not candidates:
            return pairs

        for rawid in candidates:
            starts = bdev_starts_by_rawid.get(rawid, [])
            dones  = bdev_dones_by_rawid.get(rawid, [])
            if not starts or not dones:
                continue

            for (t2, obj2, core2) in starts:
                if not (t1 <= t2 <= t4):
                    continue
                chosen = None
                for (t3, obj3, core3) in dones:
                    if t3 < t2:
                        continue
                    if t3 > t4:
                        break
                    chosen = (t3, obj3, core3)
                    break
                if chosen is None:
                    continue

                t3, obj3, core3 = chosen
                dur = t3 - t2
                pairs.append((rawid, t2, t3, (obj2 or obj3), core2, core3, dur))

        return pairs

    # ---- output schemas ----
    agg_fields = [
        "raid_id",
        "parent_bdev_id",
        "root_obj",
        "root_core_start",
        "root_core_done",
        "num_children",
        "base_bdev_id",         # slowest child rawid (max t3-t2)
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

    child_fields = [
        "raid_id",
        "parent_bdev_id",
        "root_obj",
        "root_core_start",
        "root_core_done",
        "child_index",
        "child_bdev_id",
        "child_obj",
        "child_core_start",
        "child_core_done",
        "t0_root_bdev_start",
        "t1_raid_start",
        "t2_child_bdev_start",
        "t3_child_bdev_done",
        "t4_raid_done",
        "t5_root_bdev_done",
        "step1_t1_minus_t0",
        "step2_t2_minus_t1",
        "step3_t3_minus_t2",
        "step4_t4_minus_t3",
        "step5_t5_minus_t4",
        "lat_root_bdev",
        "lat_raid",
        "lat_child_bdev",
    ]

    agg_rows: List[Dict[str, str]] = []
    child_rows: List[Dict[str, str]] = []

    for raid_main, (t1, raid_rawid_start) in raid_start.items():
        if raid_main not in raid_done:
            warn(f"RAID '{raid_main}': missing RAID_DONE")
            continue
        t4, _raid_rawid_done = raid_done[raid_main]

        # parent id from "Rxxx (iYYY)" -> iYYY
        _main, parent_id = parse_id(raid_rawid_start)
        parent_id = parent_id or ""
        if not parent_id:
            warn(f"RAID '{raid_main}': raid start id has no '(parent_id)': '{raid_rawid_start}'")

        t0, t5, root_obj, root_core0, root_core5 = (None, None, "", "", "")
        if parent_id:
            t0, t5, root_obj, root_core0, root_core5 = find_root_times(parent_id, t1, t4, args.max_gap_us)

        if t0 is None or t5 is None:
            warn(f"RAID '{raid_main}': cannot find root bdev start/done by parent_id='{parent_id}' within max_gap={args.max_gap_us}us")

        pairs = get_child_pairs_for_raid(raid_main, t1, t4)

        if not pairs:
            warn(f"RAID '{raid_main}': cannot find any child base bdev pairs bound to '( {raid_main} )' inside raid interval")
            agg_rows.append({
                "raid_id": raid_main,
                "parent_bdev_id": parent_id,
                "root_obj": root_obj,
                "root_core_start": root_core0,
                "root_core_done": root_core5,
                "num_children": "0",
                "base_bdev_id": "",
                "base_obj": "",
                "base_core_start": "",
                "base_core_done": "",
                "t0_root_bdev_start": fmt(t0),
                "t1_raid_start": fmt(t1),
                "t2_base_bdev_start": "",
                "t3_base_bdev_done": "",
                "t4_raid_done": fmt(t4),
                "t5_root_bdev_done": fmt(t5),
                "step1_t1_minus_t0": "" if t0 is None else f"{(t1 - t0):.6f}",
                "step2_t2_minus_t1": "",
                "step3_t3_minus_t2": "",
                "step4_t4_minus_t3": "",
                "step5_t5_minus_t4": "" if t5 is None else f"{(t5 - t4):.6f}",
                "lat_root_bdev": "" if (t0 is None or t5 is None) else f"{(t5 - t0):.6f}",
                "lat_raid": f"{(t4 - t1):.6f}",
                "lat_base_bdev": "",
            })
            continue

        pairs_sorted = sorted(pairs, key=lambda x: x[1])  # by t2

        # per-child rows
        for idx, (rawid, t2, t3, obj, core2, core3, dur) in enumerate(pairs_sorted, start=1):
            child_rows.append({
                "raid_id": raid_main,
                "parent_bdev_id": parent_id,
                "root_obj": root_obj,
                "root_core_start": root_core0,
                "root_core_done": root_core5,
                "child_index": str(idx),
                "child_bdev_id": rawid,
                "child_obj": obj,
                "child_core_start": core2,
                "child_core_done": core3,
                "t0_root_bdev_start": fmt(t0),
                "t1_raid_start": fmt(t1),
                "t2_child_bdev_start": fmt(t2),
                "t3_child_bdev_done": fmt(t3),
                "t4_raid_done": fmt(t4),
                "t5_root_bdev_done": fmt(t5),
                "step1_t1_minus_t0": "" if t0 is None else f"{(t1 - t0):.6f}",
                "step2_t2_minus_t1": f"{(t2 - t1):.6f}",
                "step3_t3_minus_t2": f"{(t3 - t2):.6f}",
                "step4_t4_minus_t3": f"{(t4 - t3):.6f}",
                "step5_t5_minus_t4": "" if t5 is None else f"{(t5 - t4):.6f}",
                "lat_root_bdev": "" if (t0 is None or t5 is None) else f"{(t5 - t0):.6f}",
                "lat_raid": f"{(t4 - t1):.6f}",
                "lat_child_bdev": f"{dur:.6f}",
            })

        # agg row chooses slowest child (max dur=t3-t2), tie-break by latest t3
        slowest = max(pairs, key=lambda x: (x[6], x[2]))
        base_rawid, t2, t3, base_obj, base_core2, base_core3, base_dur = slowest

        agg_rows.append({
            "raid_id": raid_main,
            "parent_bdev_id": parent_id,
            "root_obj": root_obj,
            "root_core_start": root_core0,
            "root_core_done": root_core5,
            "num_children": str(len(pairs)),
            "base_bdev_id": base_rawid,
            "base_obj": base_obj,
            "base_core_start": base_core2,
            "base_core_done": base_core3,
            "t0_root_bdev_start": fmt(t0),
            "t1_raid_start": fmt(t1),
            "t2_base_bdev_start": fmt(t2),
            "t3_base_bdev_done": fmt(t3),
            "t4_raid_done": fmt(t4),
            "t5_root_bdev_done": fmt(t5),
            "step1_t1_minus_t0": "" if t0 is None else f"{(t1 - t0):.6f}",
            "step2_t2_minus_t1": f"{(t2 - t1):.6f}",
            "step3_t3_minus_t2": f"{(t3 - t2):.6f}",
            "step4_t4_minus_t3": f"{(t4 - t3):.6f}",
            "step5_t5_minus_t4": "" if t5 is None else f"{(t5 - t4):.6f}",
            "lat_root_bdev": "" if (t0 is None or t5 is None) else f"{(t5 - t0):.6f}",
            "lat_raid": f"{(t4 - t1):.6f}",
            "lat_base_bdev": f"{base_dur:.6f}",
        })

    # write outputs
    with open(args.output_agg, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=agg_fields)
        w.writeheader()
        for r in agg_rows:
            w.writerow(r)

    with open(args.output_child, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=child_fields)
        w.writeheader()
        for r in child_rows:
            w.writerow(r)

    print(f"[OK] Wrote {len(agg_rows)} RAID records to {args.output_agg}")
    print(f"[OK] Wrote {len(child_rows)} child records to {args.output_child}")

if __name__ == "__main__":
    main()
