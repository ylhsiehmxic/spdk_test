#!/usr/bin/env python3
import argparse
import csv
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Calculate average timestamp delta for name pairs in CSV."
    )
    parser.add_argument("csv_file", help="Input CSV file path")
    parser.add_argument("first_name", help="Expected first name")
    parser.add_argument("second_name", help="Expected second name")
    parser.add_argument(
        "--must-adjacent",
        choices=["yes", "no"],
        default="no",
        help="Whether second_name must be in the immediately next row (default: yes)",
    )
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


def safe_get(row, idx):
    return row[idx].strip() if idx < len(row) else ""


def first_three_cols(row):
    return [safe_get(row, 0), safe_get(row, 1), safe_get(row, 2)]


def print_violation(line_num, row):
    c1, c2, c3 = first_three_cols(row)
    print(f"line {line_num}: {c1}, {c2}, {c3}")


def main():
    args = parse_args()
    must_adjacent = (args.must_adjacent == "yes")

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

    i = 0
    while i < len(rows):
        line_num_1, row1 = rows[i]
        name1 = safe_get(row1, 2)

        if name1 != args.first_name:
            i += 1
            continue

        ts1_str = safe_get(row1, 1)
        try:
            ts1 = float(ts1_str)
        except ValueError:
            print(f"Warning: invalid timestamp at line {line_num_1}", file=sys.stderr)
            i += 1
            continue

        matched = False

        if must_adjacent:
            if i + 1 < len(rows):
                line_num_2, row2 = rows[i + 1]
                name2 = safe_get(row2, 2)

                if name2 == args.second_name:
                    ts2_str = safe_get(row2, 1)
                    try:
                        ts2 = float(ts2_str)
                        deltas.append(ts2 - ts1)
                        matched = True
                    except ValueError:
                        print(
                            f"Warning: invalid timestamp at line {line_num_2}",
                            file=sys.stderr,
                        )
                else:
                    violations.append((line_num_1, row1))
            else:
                violations.append((line_num_1, row1))

        else:
            j = i + 1
            while j < len(rows):
                line_num_2, row2 = rows[j]
                name2 = safe_get(row2, 2)

                if name2 == args.second_name:
                    ts2_str = safe_get(row2, 1)
                    try:
                        ts2 = float(ts2_str)
                        deltas.append(ts2 - ts1)
                        matched = True
                    except ValueError:
                        print(
                            f"Warning: invalid timestamp at line {line_num_2}",
                            file=sys.stderr,
                        )
                    break
                j += 1

            if not matched:
                violations.append((line_num_1, row1))

        i += 1

    print(f"First name     : {args.first_name}")
    print(f"Second name    : {args.second_name}")
    print(f"Must adjacent  : {args.must_adjacent}")
    print()

    if violations:
        print("Violations:")
        for line_num, row in violations:
            print_violation(line_num, row)
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
        print(f"Average delta   : {avg_delta}")
    else:
        print("Valid pair count: 0")
        print("Average delta   : N/A")


if __name__ == "__main__":
    main()
