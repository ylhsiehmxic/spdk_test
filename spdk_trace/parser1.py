#!/usr/bin/env python3
import csv
import re
import sys
from typing import Dict, List, Optional

# 你要的輸出欄位順序
OUT_FIELDS = ["core", "ts", "obj", "event_type", "size", "id", "type", "ctx", "offset", "qd", "time"]

CORE_RE = re.compile(r"^(?P<core>\d+):$")

def parse_line(line: str) -> Optional[Dict[str, str]]:
    line = line.rstrip("\n")
    if not line.strip():
        return None

    # spdk_trace 這種輸出你說是用 tab 分隔（但保險起見也先把連續空白視為分隔）
    # 主要以 tab split；如果沒有 tab，退化成空白 split
    parts = line.split("\t")
    if len(parts) == 1:
        parts = re.split(r"\s+", line.strip())

    # 去掉空 token（有些輸出會有多個 tab）
    parts = [p for p in parts if p != ""]
    if len(parts) < 4:
        # 不符合預期
        return None

    # 前四欄固定：core:, ts, obj, event_type
    core_tok, ts_tok, obj_tok, event_tok = parts[0], parts[1], parts[2], parts[3]

    m = CORE_RE.match(core_tok)
    if not m:
        return None

    row: Dict[str, str] = {k: "" for k in OUT_FIELDS}
    row["core"] = m.group("core")
    row["ts"] = ts_tok
    row["obj"] = obj_tok
    row["event_type"] = event_tok

    # 後面是 key/value pairs
    # 你的格式：key: \t value
    # 但有些情況可能會變成 "key: value" 在同一個 token，所以要同時支援
    i = 4
    while i < len(parts):
        tok = parts[i]

        # case A: tok 本身就是 "key:" (最常見)
        if tok.endswith(":") and len(tok) > 1:
            key = tok[:-1]
            val = ""
            if i + 1 < len(parts):
                val = parts[i + 1]
                i += 2
            else:
                i += 1
            row[key] = val
            continue

        # case B: tok 可能是 "key: value"（同一格）
        # 例如某些版本或你先前示例用空白看起來像這樣
        if ":" in tok:
            # 只切第一個冒號，避免 ctx 之類含冒號的情況（通常不會，但保險）
            k, v = tok.split(":", 1)
            k = k.strip()
            v = v.strip()
            if k:
                row[k] = v
                i += 1
                continue

        # 其他：跳過（避免 parser 因為未知欄位壞掉）
        i += 1

    return row


def main():
    # stdin 或檔案
    if len(sys.argv) > 1:
        f = open(sys.argv[1], "r", encoding="utf-8", errors="replace")
    else:
        f = sys.stdin

    writer = csv.DictWriter(sys.stdout, fieldnames=OUT_FIELDS, lineterminator="\n")
    writer.writeheader()

    for line in f:
        row = parse_line(line)
        if row is not None:
            writer.writerow(row)

    if f is not sys.stdin:
        f.close()


if __name__ == "__main__":
    main()
