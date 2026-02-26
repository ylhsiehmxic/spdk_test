#!/usr/bin/env python3
import argparse
import csv
import re
from typing import Dict, List, Tuple, Optional

# event_type 通常像 BDEV_IO_START / BDEV_RAID_IO_DONE / UBLK_*
EVENT_RE = re.compile(r"^[A-Z0-9_]+$")

# 找 key: 的 regex（key 允許底線/數字）
KEY_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*:")

# 行首 core: ts
HEAD_RE = re.compile(r"^\s*(\d+)\s*:\s*([0-9]+(?:\.[0-9]+)?)\s+")

# 解析 id value：允許一次括號
# 例：u22143 / i24321 (u22143) / R335 (i25342) / i25342 (R335)
ID_SPLIT_RE = re.compile(r"^\s*(\S+)(?:\s*\(\s*([^)]+)\s*\))?\s*$")

BASE_COLS = ["core", "ts", "event_type", "obj"]

# 新增：固定衍生欄位（若 id 有出現就會填，沒出現就留空）
DERIVED_ID_COLS = ["id_main", "id_link"]

def split_id(raw: str) -> Tuple[str, str]:
    """
    raw id string -> (id_main, id_link)
    id_link 可能為空字串
    """
    raw = (raw or "").strip()
    if not raw:
        return "", ""
    m = ID_SPLIT_RE.match(raw)
    if not m:
        return raw, ""
    main = (m.group(1) or "").strip()
    link = (m.group(2) or "").strip() if m.group(2) else ""
    return main, link

def parse_one_line(line: str) -> Optional[Tuple[Dict[str, str], List[str]]]:
    """
    Return: (row_dict, keys_seen_in_this_line)
    row_dict includes base cols and any parsed key/value (+ derived id cols if id exists).
    """
    s = line.rstrip("\n")
    if not s.strip():
        return None

    m = HEAD_RE.match(s)
    if not m:
        return None

    core, ts = m.group(1), m.group(2)
    rest = s[m.end():]

    # 找出 rest 中所有 key: 的位置
    km = list(KEY_RE.finditer(rest))
    first_key_pos = km[0].start() if km else len(rest)

    # key: 之前的部分 -> 用來抽 obj + event_type
    prefix = rest[:first_key_pos].strip()
    prefix_tokens = prefix.split() if prefix else []

    event_type = ""
    obj = ""

    if prefix_tokens:
        if EVENT_RE.match(prefix_tokens[-1]):
            event_type = prefix_tokens[-1]
            obj = " ".join(prefix_tokens[:-1]).strip()
        else:
            idx = -1
            for i in range(len(prefix_tokens) - 1, -1, -1):
                if EVENT_RE.match(prefix_tokens[i]):
                    idx = i
                    break
            if idx >= 0:
                event_type = prefix_tokens[idx]
                obj = " ".join(prefix_tokens[:idx]).strip()
            else:
                event_type = prefix_tokens[-1]
                obj = " ".join(prefix_tokens[:-1]).strip()

    row: Dict[str, str] = {
        "core": core,
        "ts": ts,
        "event_type": event_type,
        "obj": obj,
        # derived cols default empty
        "id_main": "",
        "id_link": "",
    }

    keys_seen: List[str] = []

    # 用「key: 的位置切片」抓 value（value = key_end ~ next_key_start）
    for i, kmatch in enumerate(km):
        key = kmatch.group(1)
        val_start = kmatch.end()
        val_end = km[i + 1].start() if i + 1 < len(km) else len(rest)
        value = rest[val_start:val_end].strip()

        row[key] = value
        keys_seen.append(key)

        # 若是 id，另外拆出 id_main / id_link
        if key == "id":
            main, link = split_id(value)
            row["id_main"] = main
            row["id_link"] = link

    return row, keys_seen


def build_header(all_keys: List[str]) -> List[str]:
    # 固定：time 欄位最後
    # 固定：在 base cols 之後放 derived id cols
    keys = []
    for k in all_keys:
        if k in BASE_COLS or k in DERIVED_ID_COLS or k == "time":
            continue
        keys.append(k)

    header = BASE_COLS + DERIVED_ID_COLS + keys
    header.append("time")
    return header


def main():
    ap = argparse.ArgumentParser(description="Parse SPDK trace text into CSV with dynamic columns.")
    ap.add_argument("input", help="Input trace text file, or '-' for stdin")
    ap.add_argument("-o", "--output", default="-", help="Output CSV file, or '-' for stdout")
    args = ap.parse_args()

    fin = open(args.input, "r", encoding="utf-8", errors="replace") if args.input != "-" else __import__("sys").stdin
    lines = fin.readlines() if fin is not __import__("sys").stdin else fin.read().splitlines(True)
    if fin is not __import__("sys").stdin:
        fin.close()

    rows: List[Dict[str, str]] = []
    all_keys_in_order: List[str] = []

    seen_set = set(BASE_COLS)
    for c in DERIVED_ID_COLS:
        seen_set.add(c)
    seen_set.add("time")  # time 預留（最後輸出）

    for line in lines:
        parsed = parse_one_line(line)
        if not parsed:
            continue
        row, keys_seen = parsed
        rows.append(row)
        for k in keys_seen:
            if k not in seen_set:
                seen_set.add(k)
                all_keys_in_order.append(k)

    header = build_header(all_keys_in_order)

    fout = open(args.output, "w", newline="", encoding="utf-8") if args.output != "-" else __import__("sys").stdout
    w = csv.DictWriter(fout, fieldnames=header)
    w.writeheader()

    for r in rows:
        out = {h: r.get(h, "") for h in header}
        if "time" not in out:
            out["time"] = ""
        w.writerow(out)

    if fout is not __import__("sys").stdout:
        fout.close()


if __name__ == "__main__":
    main()
