#!/usr/bin/env python3
import argparse
import csv
import re
from typing import Dict, List, Tuple, Optional

# event_type 通常像 BDEV_IO_START / BDEV_RAID_IO_DONE
EVENT_RE = re.compile(r"^[A-Z0-9_]+$")

# 找 key: 的 regex（key 允許底線/數字）
KEY_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*:")

# 行首 core: ts
HEAD_RE = re.compile(r"^\s*(\d+)\s*:\s*([0-9]+(?:\.[0-9]+)?)\s+")

BASE_COLS = ["core", "ts", "event_type", "obj"]

def parse_one_line(line: str) -> Optional[Tuple[Dict[str, str], List[str]]]:
    """
    Return: (row_dict, keys_seen_in_this_line)
    row_dict includes base cols and any parsed key/value.
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
        # 假設 event_type 是 prefix 最後一個 token，且通常是全大寫底線
        if EVENT_RE.match(prefix_tokens[-1]):
            event_type = prefix_tokens[-1]
            obj = " ".join(prefix_tokens[:-1]).strip()
        else:
            # fallback：找最後一個像 event 的 token
            idx = -1
            for i in range(len(prefix_tokens)-1, -1, -1):
                if EVENT_RE.match(prefix_tokens[i]):
                    idx = i
                    break
            if idx >= 0:
                event_type = prefix_tokens[idx]
                obj = " ".join(prefix_tokens[:idx]).strip()
            else:
                # 真的找不到 event，就先塞到 event_type，obj 留空
                event_type = prefix_tokens[-1]
                obj = " ".join(prefix_tokens[:-1]).strip()

    row: Dict[str, str] = {
        "core": core,
        "ts": ts,
        "event_type": event_type,
        "obj": obj,
    }

    keys_seen: List[str] = []

    # 用「key: 的位置切片」抓 value（value = key_end ~ next_key_start）
    for i, kmatch in enumerate(km):
        key = kmatch.group(1)
        # value 起點：冒號後（允許冒號後有空白）
        val_start = kmatch.end()
        val_end = km[i + 1].start() if i + 1 < len(km) else len(rest)
        value = rest[val_start:val_end].strip()

        # 你提到 id value 允許有一次括號：i232 (R73) / R73 (i134)
        # 這裡不強制格式，只是「允許有空白」；若你想要嚴格驗證可打開下面檢查
        # if key == "id":
        #     if not re.match(r"^\S+(?:\s+\(\S+\))?$", value):
        #         pass  # 不符合也照收，避免漏資料

        # 記錄
        row[key] = value
        keys_seen.append(key)

    return row, keys_seen


def build_header(all_keys: List[str]) -> List[str]:
    # 固定：time 欄位最後
    keys = []
    for k in all_keys:
        if k in BASE_COLS or k == "time":
            continue
        keys.append(k)
    # 保持出現順序（all_keys 已經是依出現順序加入）
    header = BASE_COLS + keys
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

    seen_set = set(BASE_COLS)  # base cols 視為已知
    seen_set.add("time")       # time 也預留（最後輸出）

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
        # 保證 time 沒出現也空著
        if "time" not in out:
            out["time"] = ""
        w.writerow(out)

    if fout is not __import__("sys").stdout:
        fout.close()


if __name__ == "__main__":
    main()
