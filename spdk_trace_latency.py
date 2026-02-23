#!/usr/bin/env python3
import csv
from collections import defaultdict, Counter
from statistics import mean

def read_csv_rows(path: str):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for r in reader:
            r["event_type"] = (r.get("event_type") or "").strip()
            r["id_main"] = (r.get("id_main") or r.get("id") or "").strip()
            r["id_rel"] = (r.get("id_rel") or "").strip()
            r["id_has_rel"] = (r.get("id_has_rel") or "").strip()
            try:
                r["ts"] = float(r.get("ts", "nan"))
            except ValueError:
                r["ts"] = float("nan")
            yield r

def pick_one_ts(events, key):
    ts_list = events.get(key, [])
    if not ts_list:
        return None
    if key.endswith("_START"):
        return min(ts_list)
    if key.endswith("_DONE"):
        return max(ts_list)
    return min(ts_list)

def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)

def main(in_csv: str, out_csv: str):
    raid_by_root = {}       # root_i -> Rxxx
    base_by_raid = {}       # Rxxx -> child_i

    # id_main -> event_type -> [ts]
    events_by_idmain = defaultdict(lambda: defaultdict(list))

    rows = list(read_csv_rows(in_csv))

    # ---- pass 1: collect events + mapping ----
    for r in rows:
        ev = r["event_type"]
        ts = r["ts"]
        id_main = r["id_main"]
        id_rel = r["id_rel"]
        if not id_main or ts != ts:  # NaN
            continue

        events_by_idmain[id_main][ev].append(ts)

        if ev in ("BDEV_RAID_IO_START", "BDEV_RAID_IO_DONE"):
            if id_rel:
                raid_by_root[id_rel] = id_main

        if ev in ("BDEV_IO_START", "BDEV_IO_DONE"):
            if id_rel and id_rel.startswith("R"):
                base_by_raid[id_rel] = id_main

    # ---- NEW: duplicate report ----
    # 定義你在意的 event（你也可擴充）
    interesting_prefix = ("BDEV_IO_", "BDEV_RAID_IO_")
    dup_items = []  # (id_main, event_type, count, sorted_ts_list)

    for idm, ev_map in events_by_idmain.items():
        for ev, ts_list in ev_map.items():
            if not ev.startswith(interesting_prefix):
                continue
            if len(ts_list) > 1:
                dup_items.append((idm, ev, len(ts_list), sorted(ts_list)))

    if dup_items:
        # 排序：先 count 多的、再 id
        dup_items.sort(key=lambda x: (-x[2], x[0], x[1]))
        print("\n[Duplicate START/DONE detected]")
        print(f"Total duplicate (id_main,event_type) pairs: {len(dup_items)}\n")
        for idm, ev, cnt, ts_list in dup_items[:200]:
            # 只印前 200 組避免太爆；你要全印就把 [:200] 拿掉
            ts_str = ", ".join(f"{t:.6f}" for t in ts_list)
            print(f"- id_main={idm}  event_type={ev}  count={cnt}  ts=[{ts_str}]")
        if len(dup_items) > 200:
            print(f"\n... truncated, more {len(dup_items)-200} duplicate pairs not shown\n")
    else:
        print("\n[Duplicate START/DONE detected] none\n")

    # ---- determine candidate roots ----
    candidate_roots = set(raid_by_root.keys())
    for idm, evs in events_by_idmain.items():
        if "BDEV_IO_START" in evs and "BDEV_IO_DONE" in evs:
            if not idm.startswith("R") and idm not in base_by_raid.values():
                candidate_roots.add(idm)

    # ---- build 6 timepoints per root ----
    out_rows = []
    missing = 0

    for root in sorted(candidate_roots):
        R = raid_by_root.get(root, "")
        child = base_by_raid.get(R, "") if R else ""

        t0 = pick_one_ts(events_by_idmain[root], "BDEV_IO_START") if root in events_by_idmain else None
        t5 = pick_one_ts(events_by_idmain[root], "BDEV_IO_DONE")  if root in events_by_idmain else None
        t1 = pick_one_ts(events_by_idmain[R], "BDEV_RAID_IO_START") if R and R in events_by_idmain else None
        t4 = pick_one_ts(events_by_idmain[R], "BDEV_RAID_IO_DONE")  if R and R in events_by_idmain else None
        t2 = pick_one_ts(events_by_idmain[child], "BDEV_IO_START") if child and child in events_by_idmain else None
        t3 = pick_one_ts(events_by_idmain[child], "BDEV_IO_DONE")  if child and child in events_by_idmain else None

        def dt(a, b):
            return (b - a) if (a is not None and b is not None) else ""

        gap_01 = dt(t0, t1)
        gap_12 = dt(t1, t2)
        gap_23 = dt(t2, t3)
        gap_34 = dt(t3, t4)
        gap_45 = dt(t4, t5)

        dur_parent = dt(t0, t5)
        dur_raid   = dt(t1, t4)
        dur_base   = dt(t2, t3)

        points = [t0, t1, t2, t3, t4, t5]
        missing_points = sum(1 for x in points if x is None)
        if missing_points:
            missing += 1

        out_rows.append({
            "root_id": root,
            "raid_id": R,
            "base_id": child,
            "t0_BDEV_IO_START": t0 if t0 is not None else "",
            "t1_BDEV_RAID_IO_START": t1 if t1 is not None else "",
            "t2_BASE_BDEV_IO_START": t2 if t2 is not None else "",
            "t3_BASE_BDEV_IO_DONE": t3 if t3 is not None else "",
            "t4_BDEV_RAID_IO_DONE": t4 if t4 is not None else "",
            "t5_BDEV_IO_DONE": t5 if t5 is not None else "",
            "gap_01_bdev_to_raid_start": gap_01,
            "gap_12_raid_to_base_start": gap_12,
            "gap_23_base_io": gap_23,
            "gap_34_base_done_to_raid_done": gap_34,
            "gap_45_raid_done_to_bdev_done": gap_45,
            "dur_parent_bdev_start_to_done": dur_parent,
            "dur_raid_start_to_done": dur_raid,
            "dur_base_bdev_start_to_done": dur_base,
            "missing_points": missing_points,
        })

    # ---- write latency csv ----
    fieldnames = [
        "root_id","raid_id","base_id",
        "t0_BDEV_IO_START","t1_BDEV_RAID_IO_START","t2_BASE_BDEV_IO_START",
        "t3_BASE_BDEV_IO_DONE","t4_BDEV_RAID_IO_DONE","t5_BDEV_IO_DONE",
        "gap_01_bdev_to_raid_start","gap_12_raid_to_base_start","gap_23_base_io",
        "gap_34_base_done_to_raid_done","gap_45_raid_done_to_bdev_done",
        "dur_parent_bdev_start_to_done","dur_raid_start_to_done","dur_base_bdev_start_to_done",
        "missing_points",
    ]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    # ---- stats for complete ones ----
    complete = [r for r in out_rows if r["missing_points"] == 0]

    def collect(col):
        vals = []
        for r in complete:
            v = r[col]
            if v != "" and v is not None:
                vals.append(float(v))
        vals.sort()
        return vals

    for col in [
        "gap_01_bdev_to_raid_start",
        "gap_12_raid_to_base_start",
        "gap_23_base_io",
        "gap_34_base_done_to_raid_done",
        "gap_45_raid_done_to_bdev_done",
        "dur_parent_bdev_start_to_done",
        "dur_raid_start_to_done",
        "dur_base_bdev_start_to_done",
    ]:
        vals = collect(col)
        if not vals:
            continue
        print(f"{col}: n={len(vals)} mean={mean(vals):.3f} p50={percentile(vals,50):.3f} p95={percentile(vals,95):.3f} p99={percentile(vals,99):.3f}")

    print(f"\nTotal roots: {len(out_rows)}, complete: {len(complete)}, missing: {missing}")
    print(f"Wrote: {out_csv}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: analyze_trace_latency.py <parsed.csv> <out_latency.csv>")
        raise SystemExit(2)
    main(sys.argv[1], sys.argv[2])
