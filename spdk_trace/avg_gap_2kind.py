#!/usr/bin/env python3
import argparse
import csv
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Calculate average timestamp delta for adjacent name pairs in CSV."
    )
    parser.add_argument("csv_file", help="Input CSV file path")
    parser.add_argument("first_name", help="Expected name in current row")
    parser.add_argument("second_name", help="Expected name in next adjacent row")
    parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter, default is ','"
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="File encoding, default is utf-8"
    )
    return parser.parse_args()


def safe_get(row, idx):
    return row[idx].strip() if idx < len(row) else ""


def main():
    args = parse_args()

    rows = []

    try:
        with open(args.csv_file, "r", encoding=args.encoding, newline="") as f:
            reader = csv.reader(f, delimiter=args.delimiter)
            for line_num, row in enumerate(reader, start=1):
                if len(row) < 3:
                    continue
                rows.append((line_num, row))
    except FileNotFoundError:
        print(f"Error: file not found: {args.csv_file}", file=sys.stderr)
        sys.exit(1)

    deltas = []
    violations = []

    for i in range(len(rows) - 1):
        line_num_1, row1 = rows[i]
        line_num_2, row2 = rows[i + 1]

        name1 = safe_get(row1, 2)
        name2 = safe_get(row2, 2)

        if name1 != args.first_name:
            continue

        if name2 != args.second_name:
            violations.append((line_num_1, row1[:3]))
            continue

        ts1_str = safe_get(row1, 1)
        ts2_str = safe_get(row2, 1)

        try:
            ts1 = float(ts1_str)
            ts2 = float(ts2_str)
        except ValueError:
            print(
                f"Warning: invalid timestamp at line {line_num_1} or {line_num_2}",
                file=sys.stderr
            )
            continue

        deltas.append(ts2 - ts1)

    print(f"First name : {args.first_name}")
    print(f"Second name: {args.second_name}")
    print()

    if violations:
        print("Violations:")
        for line_num, cols in violations:
            c1 = cols[0] if len(cols) > 0 else ""
            c2 = cols[1] if len(cols) > 1 else ""
            c3 = cols[2] if len(cols) > 2 else ""
            print(f"line {line_num}: {c1}, {c2}, {c3}")
        print()
    else:
        print("Violations: none")
        print()

    if deltas:
        print("Valid deltas:")
        for idx, delta in enumerate(deltas, start=1):
            print(f"  delta[{idx}] = {delta}")
        print()
        avg_delta = sum(deltas) / len(deltas)
        print(f"Valid pair count: {len(deltas)}")
        print(f"Average delta : {avg_delta}")
    else:
        print("Valid pair count: 0")
        print("Average delta : N/A")


if __name__ == "__main__":
    main()
