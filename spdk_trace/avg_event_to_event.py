#!/usr/bin/env python3
import argparse
import csv
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute average time from event A to next event B."
    )

    parser.add_argument("csv_file", help="Input CSV file")

    parser.add_argument(
        "--submit-name",
        default="UBLK_BDEV_SUBMIT",
        help="Start event name (default: UBLK_BDEV_SUBMIT)",
    )

    parser.add_argument(
        "--done-name",
        default="UBLK_BDEV_DONE",
        help="Middle event name (default: UBLK_BDEV_DONE)",
    )

    parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter (default ,)",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each delta",
    )

    return parser.parse_args()


def safe_get(row, idx):
    return row[idx].strip() if idx < len(row) else ""


def main():

    args = parse_args()

    rows = []

    try:
        with open(args.csv_file, newline="") as f:
            reader = csv.reader(f, delimiter=args.delimiter)

            for line_num, row in enumerate(reader, start=1):

                if len(row) < 3:
                    continue

                name = safe_get(row, 2)

                try:
                    ts = float(safe_get(row, 1))
                except ValueError:
                    continue

                rows.append(
                    {
                        "line": line_num,
                        "name": name,
                        "ts": ts,
                    }
                )

    except FileNotFoundError:
        print("file not found")
        sys.exit(1)

    submit_indices = [
        i for i, r in enumerate(rows) if r["name"] == args.submit_name
    ]

    if len(submit_indices) < 2:
        print("Not enough submit events")
        return

    deltas = []

    for i in range(len(submit_indices) - 1):

        start = submit_indices[i]
        end = submit_indices[i + 1]

        next_submit_ts = rows[end]["ts"]

        for j in range(start + 1, end):

            if rows[j]["name"] != args.done_name:
                continue

            delta = next_submit_ts - rows[j]["ts"]

            deltas.append(delta)

            if args.verbose:
                print(
                    f"DONE line {rows[j]['line']} -> "
                    f"next SUBMIT line {rows[end]['line']} "
                    f"delta={delta}"
                )

    print(f"submit event: {args.submit_name}")
    print(f"done event  : {args.done_name}")
    print()

    print(f"submit count : {len(submit_indices)}")
    print(f"done samples : {len(deltas)}")

    if not deltas:
        print("average delta: N/A")
        return

    avg = sum(deltas) / len(deltas)

    print(f"average delta: {avg}")


if __name__ == "__main__":
    main()
