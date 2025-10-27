#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import re, os, sys, math

# === 0. 參數檢查 ===
if len(sys.argv) < 2:
    print("Error: csv is not given")
    sys.exit(1)

csv_file = sys.argv[1]
if not os.path.exists(csv_file):
    print(f"Error: can't find {csv_file}")
    sys.exit(1)

# === 1. 讀取 CSV ===
df = pd.read_csv(csv_file)

# === 2. throughput 單位轉換 ===
def to_mibps(val):
    if pd.isna(val):
        return None
    m = re.match(r"([0-9.]+)\s*([A-Za-z/]+)", str(val))
    if not m:
        return None
    num = float(m.group(1))
    unit = m.group(2).lower()
    if "gib" in unit:
        return num * 1024
    elif "mib" in unit:
        return num
    elif "kib" in unit:
        return num / 1024
    elif "gb" in unit:
        return num * 953.7 / 1000  # approx GiB->MiB
    elif "mb" in unit:
        return num
    elif "kb" in unit:
        return num / 1024
    else:
        return num

df["throughput_mib"] = df["throughput"].apply(to_mibps)

# === 3. CPU util 轉百分比 ===
for col in ["thr_cpu_util", "po_cpu_util"]:
    if col in df.columns:
        df[col] = df[col] * 100

# === 4. 共用縱軸範圍 ===
ymax_thr = df["throughput_mib"].max() * 1.1
ymax_iops = df["iops"].max() * 1.1

# === 5. block size 排序 ===
def bs_key(bs):
    m = re.match(r"(\d+)([KMG]?)", str(bs).upper())
    if not m: return math.inf
    num, unit = int(m.group(1)), m.group(2)
    mult = {"":1, "K":1024, "M":1024**2, "G":1024**3}
    return num * mult.get(unit, 1)

df["bs"] = df["bs"].astype(str)
unique_bs_sorted = sorted(df["bs"].unique(), key=bs_key)

# === 6. 繪圖共用函式 ===
def plot_group(df_subset, x_col, group_col, metric, ylabel, fixed_ymax, outdir, title_prefix, filename_prefix):
    pivot_df = df_subset.pivot_table(
        index=x_col,
        columns=group_col,
        values=metric,
        aggfunc="mean"
    )

    # 保持 x 軸順序一致
    if x_col == "bs":
        pivot_df = pivot_df.reindex(unique_bs_sorted)

    pivot_df.plot(kind="bar", figsize=(8, 5))
    plt.title(f"{title_prefix}: {ylabel}")
    plt.xlabel(x_col)
    plt.ylabel(ylabel)
    if fixed_ymax:
        plt.ylim(0, fixed_ymax)
    plt.legend(title=group_col, bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    fname = f"{filename_prefix}_{metric}.png"
    path = os.path.join(outdir, fname)
    plt.savefig(path, dpi=150)
    plt.close()
    return fname

# === 7. 第一類 ===
out1 = "plots_bs"; os.makedirs(out1, exist_ok=True)
metrics = [
    ("throughput_mib", "Throughput (MiB/s)", ymax_thr),
    ("iops", "IOPS", ymax_iops),
    ("thr_cpu_util", "Thread CPU Util (%)", None),
    ("po_cpu_util", "Poller CPU Util (%)", None),
]
html1 = "<h2>View 1: by bs</h2><table border=1>"

for metric, ylabel, ymax in metrics:
    html1 += "<tr>"
    for bs_val, df_bs in df.groupby("bs"):
        fname = plot_group(df_bs, "core_num", "thread_num", metric, ylabel, ymax, out1,
                           f"{ylabel} (bs={bs_val})", bs_val)
        html1 += f"<td align='center'><img src='{out1}/{fname}' width='320'><br>{bs_val}</td>"
    html1 += "</tr>"
html1 += "</table>"

# === 8. 第二類 ===
out2 = "plots_threadnum"; os.makedirs(out2, exist_ok=True)
html2 = "<h2>View 2: by thread_num</h2><table border=1>"

for metric, ylabel, ymax in metrics:
    html2 += "<tr>"
    for thread_val, df_th in df.groupby("thread_num"):
        fname = plot_group(df_th, "bs", "core_num", metric, ylabel, ymax, out2,
                           f"{ylabel} (thread={thread_val})", f"thread{thread_val}")
        html2 += f"<td align='center'><img src='{out2}/{fname}' width='320'><br>thread={thread_val}</td>"
    html2 += "</tr>"
html2 += "</table>"

# === 9. 第三類 ===
out3 = "plots_corenu"; os.makedirs(out3, exist_ok=True)
html3 = "<h2>View 3：by core_num</h2><table border=1>"

for metric, ylabel, ymax in metrics:
    html3 += "<tr>"
    for core_val, df_core in df.groupby("core_num"):
        fname = plot_group(df_core, "thread_num", "bs", metric, ylabel, ymax, out3,
                           f"{ylabel} (core={core_val})", f"core{core_val}")
        html3 += f"<td align='center'><img src='{out3}/{fname}' width='320'><br>core={core_val}</td>"
    html3 += "</tr>"
html3 += "</table>"

# === 10. 輸出 HTML ===
with open("index.html", "w") as f:
    f.write("<html><head><meta charset='utf-8'><title>SPDK Performance Charts</title></head><body>")
    f.write(html1 + html2 + html3)
    f.write("</body></html>")

print("Open summary.html to see")
