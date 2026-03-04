#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import re
from html import escape

import pandas as pd
import matplotlib.pyplot as plt


def parse_size_to_bytes(s: str) -> float:
    """
    Parse fio-like block size strings: 4K, 16k, 1M, 512, 2G ...
    Return bytes (float). If cannot parse, return NaN.
    """
    if s is None:
        return float("nan")
    s = str(s).strip()
    m = re.fullmatch(r"(?i)\s*(\d+(?:\.\d+)?)\s*([kmgt]?)\s*B?\s*", s)
    if not m:
        return float("nan")
    val = float(m.group(1))
    unit = m.group(2).lower()
    mul = {
        "": 1,
        "k": 1024,
        "m": 1024**2,
        "g": 1024**3,
        "t": 1024**4,
    }[unit]
    return val * mul


def smart_sort_values(values):
    """
    Sort values with best-effort:
    - numeric sort if all convertible
    - size sort for strings like 16K/1M/512
    - otherwise lexicographic
    """
    vals = list(values)
    # Try numeric
    num_ok = True
    num_vals = []
    for v in vals:
        try:
            num_vals.append(float(v))
        except Exception:
            num_ok = False
            break
    if num_ok:
        return [v for _, v in sorted(zip(num_vals, vals), key=lambda x: x[0])]

    # Try size (bytes)
    size_vals = []
    size_ok = True
    for v in vals:
        b = parse_size_to_bytes(v)
        if b != b:  # NaN
            size_ok = False
            break
        size_vals.append(b)
    if size_ok:
        return [v for _, v in sorted(zip(size_vals, vals), key=lambda x: x[0])]

    # Fallback lexicographic
    return sorted(vals, key=lambda x: str(x))


def sanitize_filename(s: str) -> str:
    s = str(s)
    s = re.sub(r"[^\w.\-]+", "_", s)
    return s[:200] if len(s) > 200 else s


def make_html_gallery(img_items, outdir, cols: int, title: str):
    # img_items: list of (img_filename, caption)
    html = []
    html.append("<!doctype html>")
    html.append("<html><head><meta charset='utf-8'>")
    html.append(f"<title>{escape(title)}</title>")
    html.append("""
<style>
body { font-family: sans-serif; margin: 16px; }
table { border-collapse: collapse; }
td { border: 1px solid #ccc; padding: 8px; vertical-align: top; }
.caption { margin: 6px 0 0 0; font-size: 12px; color: #333; }
img { max-width: 520px; height: auto; display: block; }
</style>
""")
    html.append("</head><body>")
    html.append(f"<h2>{escape(title)}</h2>")
    html.append("<table>")

    for i, (img, cap) in enumerate(img_items):
        if i % cols == 0:
            html.append("<tr>")
        html.append("<td>")
        html.append(f"<img src='{escape(img)}'>")
        html.append(f"<div class='caption'>{escape(cap)}</div>")
        html.append("</td>")
        if i % cols == cols - 1:
            html.append("</tr>")

    if len(img_items) % cols != 0:
        html.append("</tr>")

    html.append("</table></body></html>")

    with open(os.path.join(outdir, "index.html"), "w", encoding="utf-8") as f:
        f.write("\n".join(html))


def main():
    ap = argparse.ArgumentParser(
        description="Draw grouped bar/line charts from fio summary CSV and generate HTML gallery."
    )
    ap.add_argument("--csv", required=True, help="input csv (e.g., fio_summary.csv)")
    ap.add_argument("--outdir", default="images", help="output dir (default: images)")
    ap.add_argument("--x", required=True, help="x-axis column (e.g., qd)")
    ap.add_argument("--y", required=True, help="y column (e.g., bw, avg, min, max, stdev)")
    ap.add_argument("--bars", required=True, help="column for different bar series (e.g., cores or bs or qd)")
    ap.add_argument("--group", default=None,
                    help="produce one chart per each value of this column (e.g., bs or cores or rw). If omitted, one chart total.")
    ap.add_argument("--kind", choices=["bar", "line"], default="bar", help="bar or line (default: bar)")
    ap.add_argument("--agg", choices=["mean", "median", "min", "max"], default="mean",
                    help="if duplicate keys exist, how to aggregate (default: mean)")
    ap.add_argument("--cols", type=int, default=3, help="HTML table columns (default: 3)")
    ap.add_argument("--figw", type=float, default=10.0, help="figure width (default: 10)")
    ap.add_argument("--figh", type=float, default=5.5, help="figure height (default: 5.5)")
    ap.add_argument("--rotate", type=int, default=0, help="x tick rotation (default: 0)")
    ap.add_argument("--title", default=None, help="HTML title (optional)")
    args = ap.parse_args()

    df = pd.read_csv(args.csv)

    # Ensure output dir exists
    os.makedirs(args.outdir, exist_ok=True)

    # Validate columns
    for c in [args.x, args.y, args.bars] + ([args.group] if args.group else []):
        if c and c not in df.columns:
            raise SystemExit(f"Column not found in CSV: {c}. Available: {list(df.columns)}")

    # Coerce y to numeric if possible
    df[args.y] = pd.to_numeric(df[args.y], errors="coerce")

    # Build list of group values
    if args.group:
        group_vals = smart_sort_values(df[args.group].dropna().unique())
    else:
        group_vals = [None]

    img_items = []

    for gv in group_vals:
        sub = df if gv is None else df[df[args.group] == gv]

        # pivot -> index=x, columns=bars, values=y
        pivot = sub.pivot_table(
            index=args.x,
            columns=args.bars,
            values=args.y,
            aggfunc=args.agg
        )

        # Sort index/columns smartly
        if pivot.index.dtype == "object":
            pivot = pivot.reindex(index=smart_sort_values(pivot.index))
        else:
            pivot = pivot.sort_index()

        if pivot.columns.dtype == "object":
            pivot = pivot.reindex(columns=smart_sort_values(pivot.columns))
        else:
            pivot = pivot.reindex(sorted(pivot.columns), axis=1)

        # Plot
        plt.figure(figsize=(args.figw, args.figh))
        ax = pivot.plot(kind=args.kind)

        # Labels
        xlabel = args.x
        ylabel = args.y
        if args.kind == "bar":
            plt.xlabel(xlabel)
            plt.ylabel(ylabel)
        else:
            plt.xlabel(xlabel)
            plt.ylabel(ylabel)
            plt.grid(True, axis="y")

        # Title
        if gv is None:
            plot_title = f"{args.y} vs {args.x} (series={args.bars})"
        else:
            plot_title = f"{args.group}={gv}: {args.y} vs {args.x} (series={args.bars})"
        plt.title(plot_title)

        # Tick rotation
        if args.rotate != 0:
            plt.xticks(rotation=args.rotate)

        plt.tight_layout()

        # Save
        if gv is None:
            fname = f"plot_{sanitize_filename(args.y)}_vs_{sanitize_filename(args.x)}_by_{sanitize_filename(args.bars)}.{args.kind}.png"
            caption = plot_title
        else:
            fname = f"{sanitize_filename(args.group)}_{sanitize_filename(gv)}_{sanitize_filename(args.y)}_vs_{sanitize_filename(args.x)}_by_{sanitize_filename(args.bars)}.{args.kind}.png"
            caption = plot_title

        outpath = os.path.join(args.outdir, fname)
        plt.savefig(outpath, dpi=150)
        plt.close()

        img_items.append((fname, caption))

    # HTML gallery
    title = args.title or f"{os.path.basename(args.csv)} charts"
    make_html_gallery(img_items, args.outdir, args.cols, title)

    print(f"OK: wrote {len(img_items)} image(s) to {args.outdir}/ and HTML gallery {args.outdir}/index.html")


if __name__ == "__main__":
    main()
