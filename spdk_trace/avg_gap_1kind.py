#!/usr/bin/env python3
import argparse
import csv
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Calculate average timestamp gap for rows matching a given name."
    )
    parser.add_argument("csv_file", help="Input CSV file path")
    parser.add_argument("target_name", help="Target name to match in column 3")
    parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter, default is ','",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="File encoding, default is utf-8",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    timestamps = []

    try:
        with open(args.csv_file, "r", encoding=args.encoding, newline="") as f:
            reader = csv.reader(f, delimiter=args.delimiter)
            for line_num, row in enumerate(reader, start=1):
                if len(row) < 3:
                    continue

                name = row[2].strip()
                if name != args.target_name:
                    continue

                try:
                    ts = float(row[1].strip())
                except ValueError:
                    print(
                        f"Warning: line {line_num} has invalid timestamp: {row[1]!r}",
                        file=sys.stderr,
                    )
                    continue

                timestamps.append(ts)

    except FileNotFoundError:
        print(f"Error: file not found: {args.csv_file}", file=sys.stderr)
        sys.exit(1)

    if len(timestamps) < 2:
        print(f"Matched name: {args.target_name}")
        print(f"Count: {len(timestamps)}")
        print("Not enough matched rows to calculate gaps.")
        sys.exit(0)

    gaps = []
    for i in range(1, len(timestamps)):
        gaps.append(timestamps[i] - timestamps[i - 1])

    avg_gap = sum(gaps) / len(gaps)

    print(f"Matched name: {args.target_name}")
    print(f"Matched rows: {len(timestamps)}")
    print(f"Gap count: {len(gaps)}")
    print("Gaps:")
    for i, gap in enumerate(gaps, start=1):
        print(f"  gap[{i}] = {gap}")

    print(f"Average gap: {avg_gap}")


if __name__ == "__main__":
    main()
