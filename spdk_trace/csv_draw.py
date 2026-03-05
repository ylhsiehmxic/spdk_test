#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import shutil
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import Any, List, Tuple, Optional, Dict


# -------------------------
# Parsing: repeated --set metric --yscale scale (interleaved)
# -------------------------
def parse_metric_scale_pairs(argv: List[str]) -> Tuple[List[Tuple[str, str]], List[str]]:
    """
    Accepts interleaved tokens like:
      --set bw --yscale linear --set avg --yscale log10

    Returns:
      pairs: [("bw","linear"), ("avg","log10"), ...]
      remaining_argv: argv with those tokens removed (so argparse can parse the rest)
    """
    pairs: List[Tuple[str, str]] = []
    out: List[str] = []
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok == "--set":
            if i + 1 >= len(argv):
                raise SystemExit("ERROR: --set needs a metric name, e.g. --set bw")
            metric = argv[i + 1]
            i += 2
            if i >= len(argv) or argv[i] != "--yscale":
                raise SystemExit(f"ERROR: after --set {metric}, you must provide --yscale linear|log10")
            if i + 1 >= len(argv):
                raise SystemExit("ERROR: --yscale needs a value: linear or log10")
            scale = argv[i + 1].lower()
            if scale not in ("linear", "log10"):
                raise SystemExit(f"ERROR: invalid --yscale {scale}, use linear or log10")
            pairs.append((metric, scale))
            i += 2
        else:
            out.append(tok)
            i += 1
    return pairs, out


# -------------------------
# Sorting helpers (numeric semantics)
# -------------------------
SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kKmMgGtT])?\s*(i?[bB])?\s*$")


def parse_size_like_to_number(v: Any) -> Optional[float]:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None

    if isinstance(v, (int, float, np.integer, np.floating)) and not isinstance(v, bool):
        return float(v)

    s = str(v).strip()
    try:
        return float(s)
    except Exception:
        pass

    m = SIZE_RE.match(s)
    if not m:
        return None

    num = float(m.group(1))
    suf = m.group(2)
    if not suf:
        return num

    suf = suf.lower()
    mult = {
        "k": 1024.0,
        "m": 1024.0 ** 2,
        "g": 1024.0 ** 3,
        "t": 1024.0 ** 4,
    }[suf]
    return num * mult


def smart_sorted(values: List[Any]) -> List[Any]:
    def key_fn(x: Any):
        n = parse_size_like_to_number(x)
        if n is not None:
            return (0, n, str(x))
        return (1, str(x))
    return sorted(values, key=key_fn)


# -------------------------
# Metric transform
# -------------------------
def transform_values(vals: np.ndarray, scale: str) -> np.ndarray:
    vals = np.asarray(vals, dtype=float)
    if scale == "linear":
        return vals
    # log10: keep <=0 as 0 so bars still render
    out = np.zeros_like(vals)
    mask = vals > 0
    out[mask] = np.log10(vals[mask])
    return out


# -------------------------
# Plotting
# -------------------------
def plot_grouped_bar(
    subset: pd.DataFrame,
    group_col: str,
    x_col: str,
    y_col: str,
    yscale: str,
    all_groups: List[Any],
    all_xvals: List[Any],
    global_ymax: float,
    title: str,
    out_png: str,
    agg: str = "mean",
) -> bool:
    if subset.empty:
        return False

    if agg == "mean":
        pivot = subset.groupby([group_col, x_col], dropna=False)[y_col].mean().reset_index()
    elif agg == "max":
        pivot = subset.groupby([group_col, x_col], dropna=False)[y_col].max().reset_index()
    elif agg == "min":
        pivot = subset.groupby([group_col, x_col], dropna=False)[y_col].min().reset_index()
    else:
        raise ValueError(f"Unsupported agg: {agg}")

    mat = np.zeros((len(all_groups), len(all_xvals)), dtype=float)
    lookup: Dict[Tuple[Any, Any], float] = {}
    for _, r in pivot.iterrows():
        lookup[(r[group_col], r[x_col])] = float(r[y_col])

    for gi, g in enumerate(all_groups):
        for xi, xv in enumerate(all_xvals):
            mat[gi, xi] = lookup.get((g, xv), 0.0)

    mat_t = transform_values(mat, yscale)

    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    width = 0.82 / max(1, len(all_groups))
    xbase = np.arange(len(all_xvals), dtype=float)

    for gi, g in enumerate(all_groups):
        pos = xbase + gi * width
        ax.bar(pos, mat_t[gi, :], width, label=str(g))

    ax.set_xticks(xbase + width * (len(all_groups) - 1) / 2 if len(all_groups) > 1 else xbase)
    ax.set_xticklabels([str(v) for v in all_xvals])

    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col if yscale == "linear" else f"log10({y_col})")
    ax.set_ylim(0, global_ymax)
    ax.set_title(title)
    ax.legend(fontsize=8, ncol=min(4, max(1, len(all_groups))))

    plt.tight_layout()
    plt.savefig(out_png, dpi=140)
    plt.close(fig)
    return True


def build_html_table(
    metric_name: str,
    scale: str,
    rows: List[Any],
    cols: List[Any],
    row_field: str,
    col_field: str,
    cell_img: Dict[Tuple[Any, Any], str],
    out_html: str,
):
    html = []
    html.append("<html><head><meta charset='utf-8'>")
    html.append("<style>")
    html.append("table { border-collapse: collapse; }")
    html.append("th, td { border: 1px solid #999; padding: 6px; vertical-align: top; }")
    html.append("th { background: #f2f2f2; }")
    html.append("</style></head><body>")
    html.append(f"<h2>Metric: {metric_name} &nbsp;&nbsp; Scale: {scale}</h2>")
    html.append("<table>")

    html.append("<tr>")
    html.append(f"<th>{row_field} \\ {col_field}</th>")
    for c in cols:
        html.append(f"<th>{col_field}={c}</th>")
    html.append("</tr>")

    for r in rows:
        html.append("<tr>")
        html.append(f"<th>{row_field}={r}</th>")
        for c in cols:
            img = cell_img.get((r, c))
            if img:
                html.append(f"<td><img src='{img}' width='520' title='{row_field}={r}, {col_field}={c}'></td>")
            else:
                html.append("<td></td>")
        html.append("</tr>")

    html.append("</table></body></html>")

    with open(out_html, "w", encoding="utf-8") as f:
        f.write("\n".join(html))


# -------------------------
# Scope filtering
# -------------------------
def apply_scopes(df: pd.DataFrame, scopes: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    scopes: list of (col, value)
    Keep rows where df[col] == value for all scope pairs.
    Comparison:
      - if df[col] is numeric, compare numerically when possible
      - else compare as string
    """
    out = df
    for col, val in scopes:
        if col not in out.columns:
            raise SystemExit(f"ERROR: scope column not found in CSV: {col}")

        series = out[col]
        # Try numeric compare if possible
        val_num = parse_size_like_to_number(val)
        if pd.api.types.is_numeric_dtype(series) and val_num is not None:
            out = out[series.astype(float) == float(val_num)]
        else:
            out = out[series.astype(str) == str(val)]
    return out


def main():
    pairs, rest = parse_metric_scale_pairs(sys.argv[1:])

    ap = argparse.ArgumentParser(description="Generate grouped bar charts + HTML dashboard from fio_summary.csv")

    ap.add_argument("--csv", required=True, help="input CSV")
    ap.add_argument("--group", required=True, help="group column (legend)")
    ap.add_argument("--x", required=True, help="x-axis column")
    ap.add_argument("--row", required=True, help="HTML table row column")
    ap.add_argument("--col", required=True, help="HTML table col column")
    ap.add_argument("--outdir", default="images", help="output dir (default: images)")
    ap.add_argument("--agg", choices=["mean", "max", "min"], default="mean", help="aggregate duplicates (default: mean)")

    # scopes: can repeat
    ap.add_argument("--scope", action="append", default=[], help="filter column name (repeatable)")
    ap.add_argument("--scope_value", action="append", default=[], help="filter value (repeatable)")

    args = ap.parse_args(rest)

    if not pairs:
        raise SystemExit("ERROR: you must specify at least one metric set, e.g. --set bw --yscale linear")

    if len(args.scope) != len(args.scope_value):
        raise SystemExit("ERROR: --scope and --scope_value must appear in pairs (same count).")

    scopes = list(zip(args.scope, args.scope_value))

    df = pd.read_csv(args.csv)

    # Apply scope filters early
    if scopes:
        df = apply_scopes(df, scopes)

    if df.empty:
        raise SystemExit("ERROR: after applying scope filters, no rows remain.")

    need_cols = {args.group, args.x, args.row, args.col}
    metric_cols = {m for (m, _) in pairs}
    missing = [c for c in list(need_cols | metric_cols) if c not in df.columns]
    if missing:
        raise SystemExit(f"ERROR: CSV missing columns: {missing}")

    # recreate output dir
    if os.path.exists(args.outdir):
        shutil.rmtree(args.outdir)
    os.makedirs(args.outdir, exist_ok=True)

    # sorted axes values (numeric semantics)
    all_groups = smart_sorted(df[args.group].dropna().unique().tolist())
    all_xvals = smart_sorted(df[args.x].dropna().unique().tolist())
    all_rows = smart_sorted(df[args.row].dropna().unique().tolist())
    all_cols = smart_sorted(df[args.col].dropna().unique().tolist())

    dashboards = []

    for metric, scale in pairs:
        vals = df[metric].to_numpy(dtype=float)
        tvals = transform_values(vals, scale)
        global_ymax = float(np.nanmax(tvals)) if len(tvals) else 0.0
        if not np.isfinite(global_ymax):
            global_ymax = 0.0

        cell_img: Dict[Tuple[Any, Any], str] = {}

        for r in all_rows:
            for c in all_cols:
                subset = df[(df[args.row] == r) & (df[args.col] == c)]
                if subset.empty:
                    continue

                safe_r = str(r).replace("/", "_")
                safe_c = str(c).replace("/", "_")
                out_png = os.path.join(args.outdir, f"{metric}_{scale}_{args.row}_{safe_r}_{args.col}_{safe_c}.png")

                # add scope info into title (useful when you filter)
                scope_str = ", ".join([f"{k}={v}" for k, v in scopes]) if scopes else ""
                title = f"{args.row}={r}, {args.col}={c}" + (f" [{scope_str}]" if scope_str else "")

                ok = plot_grouped_bar(
                    subset=subset,
                    group_col=args.group,
                    x_col=args.x,
                    y_col=metric,
                    yscale=scale,
                    all_groups=all_groups,
                    all_xvals=all_xvals,
                    global_ymax=global_ymax,
                    title=title,
                    out_png=out_png,
                    agg=args.agg,
                )
                if ok:
                    cell_img[(r, c)] = os.path.basename(out_png)

        out_html = os.path.join(args.outdir, f"dashboard_{metric}_{scale}.html")
        build_html_table(
            metric_name=metric,
            scale=scale,
            rows=all_rows,
            cols=all_cols,
            row_field=args.row,
            col_field=args.col,
            cell_img=cell_img,
            out_html=out_html,
        )
        dashboards.append((metric, scale, out_html))

    # index page
    index = []
    index.append("<html><head><meta charset='utf-8'></head><body>")
    index.append("<h1>FIO Dashboards</h1>")
    if scopes:
        index.append("<p><b>Scope:</b> " + ", ".join([f"{k}={v}" for k, v in scopes]) + "</p>")
    index.append("<ul>")
    for metric, scale, htmlp in dashboards:
        base = os.path.basename(htmlp)
        index.append(f"<li><a href='{base}'>metric={metric} scale={scale}</a></li>")
    index.append("</ul>")
    index.append("</body></html>")

    with open(os.path.join(args.outdir, "index.html"), "w", encoding="utf-8") as f:
        f.write("\n".join(index))

    print("Done.")
    print(f"Open: {os.path.join(args.outdir, 'index.html')}")


if __name__ == "__main__":
    main()
