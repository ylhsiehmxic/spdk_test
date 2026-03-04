import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import argparse
import shutil

parser = argparse.ArgumentParser()

parser.add_argument("--csv", required=True)

parser.add_argument("--group", required=True)
parser.add_argument("--x", required=True)

parser.add_argument("--row", required=True)
parser.add_argument("--col", required=True)

parser.add_argument("--yscale", choices=["linear","log10"], default="linear")

args = parser.parse_args()

df = pd.read_csv(args.csv)

img_dir = "images"

if os.path.exists(img_dir):
    shutil.rmtree(img_dir)

os.makedirs(img_dir)

# ---------- sorting helpers ----------

def smart_sort(values):

    try:
        return sorted(values, key=lambda x: float(str(x).replace("K","000").replace("M","000000")))
    except:
        return sorted(values)

groups = smart_sort(df[args.group].unique())
xvals = smart_sort(df[args.x].unique())
rows = smart_sort(df[args.row].unique())
cols = smart_sort(df[args.col].unique())

# ---------- y axis range ----------

bw_max = df["bw"].max()
lat_max = df["avg"].max()

if args.yscale == "log10":
    bw_max = np.log10(bw_max)
    lat_max = np.log10(lat_max)

# ---------- plotting function ----------

def plot_metric(metric, ymax):

    html_rows = []

    for r in rows:

        row_html = []

        for c in cols:

            subset = df[(df[args.row]==r) & (df[args.col]==c)]

            if len(subset)==0:
                row_html.append("")
                continue

            fig, ax = plt.subplots(figsize=(6,4))

            width = 0.8 / len(groups)

            for i,g in enumerate(groups):

                gdf = subset[subset[args.group]==g]

                ys = []

                for xv in xvals:

                    val = gdf[gdf[args.x]==xv][metric]

                    if len(val)==0:
                        ys.append(0)
                    else:
                        ys.append(val.values[0])

                ys = np.array(ys)

                if args.yscale == "log10":
                    ys = np.log10(ys)

                pos = np.arange(len(xvals)) + i*width

                ax.bar(pos, ys, width, label=g)

            ax.set_xticks(np.arange(len(xvals)) + width*(len(groups)-1)/2)
            ax.set_xticklabels(xvals)

            ax.set_ylim(0, ymax)

            ax.set_xlabel(args.x)
            ax.set_ylabel(metric)

            ax.set_title(f"{args.row}={r}, {args.col}={c}")

            ax.legend()

            fname = f"{img_dir}/{metric}_{args.row}_{r}_{args.col}_{c}.png"

            plt.tight_layout()
            plt.savefig(fname)
            plt.close()

            row_html.append(fname)

        html_rows.append(row_html)

    return html_rows


bw_imgs = plot_metric("bw", bw_max)
lat_imgs = plot_metric("avg", lat_max)

# ---------- HTML dashboard ----------

html = []

html.append("<html><body>")
html.append("<h1>FIO Dashboard</h1>")

def html_table(title, imgs):

    html.append(f"<h2>{title}</h2>")
    html.append("<table border=1>")

    for r,row in zip(rows,imgs):

        html.append("<tr>")

        for img in row:

            if img=="":
                html.append("<td></td>")
            else:
                html.append(f"<td><img src='{img}' width=420></td>")

        html.append("</tr>")

    html.append("</table>")


html_table("Bandwidth (MiB/s)", bw_imgs)
html_table("Latency avg (usec)", lat_imgs)

html.append("</body></html>")

with open(f"{img_dir}/dashboard.html","w") as f:
    f.write("\n".join(html))

print("Dashboard generated:")
print("images/dashboard.html")
