#!/usr/bin/env python3
import sys
import os
import re
import pandas as pd
import matplotlib.pyplot as plt

# ==== 讀取命令列參數 ====
if len(sys.argv) < 2:
    print("Error: csv file is not given")
    sys.exit(1)

csv_file = sys.argv[1]
if not os.path.exists(csv_file):
    print(f"Error: given file {csv_file} is not found")
    sys.exit(1)

# ==== 讀取 CSV ====
df = pd.read_csv(csv_file)

# ==== 單位轉換 ====
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
        return num * 953.7 / 1000  # 近似轉換
    elif "mb" in unit:
        return num
    elif "kb" in unit:
        return num / 1024
    else:
        return num

df["throughput_mib"] = df["throughput"].apply(to_mibps)

# ==== CPU Util 化為百分比 ====
for col in ["thr_cpu_util", "po_cpu_util"]:
    if col in df.columns:
        df[col] = df[col].astype(float) * 100

# ==== block size 排序 ====
def parse_bs(bs):
    m = re.match(r"(\d+)([KMGkmg]?)", str(bs))
    if not m:
        return float("inf")
    num = float(m.group(1))
    unit = m.group(2).upper()
    if unit == "G":
        num *= 1024
    elif unit == "M":
        num *= 1
    elif unit == "K":
        num /= 1024
    return num

df["bs_sort"] = df["bs"].apply(parse_bs)
df = df.sort_values("bs_sort")

# ==== 全域 y 軸最大值 ====
ymax_thr = df["throughput_mib"].max() * 1.1
ymax_iops = df["iops"].max() * 1.1

metrics = [
    ("throughput_mib", "Throughput (MiB/s)", ymax_thr),
    ("iops", "IOPS", ymax_iops),
    ("thr_cpu_util", "Thread CPU Util (%)", None),
    ("po_cpu_util", "Poller CPU Util (%)", None),
]

# ==== 畫圖通用函式 ====
def save_group_bar(df_group, index_col, col_col, metric, ylabel, fixed_ymax, title, savepath):
    pivot_df = df_group.pivot_table(index=index_col, columns=col_col, values=metric, aggfunc="mean")
    pivot_df.plot(kind="bar", figsize=(8, 5))
    plt.title(title)
    plt.xlabel(index_col)
    plt.ylabel(ylabel)
    if fixed_ymax:
        plt.ylim(0, fixed_ymax)
    plt.legend(title=col_col, bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig(savepath, dpi=150)
    plt.close()

# ==== 各類輸出資料夾 ====
dirs = {
    "class1": "plots_bs",
    "class2": "plots_threadnum",
    "class3": "plots_corenu"
}
for d in dirs.values():
    os.makedirs(d, exist_ok=True)

# ==== 第一類：固定 bs ====
for bs_val, df_bs in df.groupby("bs"):
    for metric, ylabel, ymax in metrics:
        savepath = os.path.join(dirs["class1"], f"{bs_val}_{metric}.png")
        save_group_bar(df_bs, "core_num", "thread_num", metric, ylabel, ymax,
                       f"{ylabel} vs core_num (bs={bs_val})", savepath)

# ==== 第二類：固定 thread_num ====
for thread_val, df_th in df.groupby("thread_num"):
    for metric, ylabel, ymax in metrics:
        savepath = os.path.join(dirs["class2"], f"thread{thread_val}_{metric}.png")
        save_group_bar(df_th, "bs", "core_num", metric, ylabel, ymax,
                       f"{ylabel} vs bs (thread_num={thread_val})", savepath)

# ==== 第三類：固定 core_num ====
for core_val, df_core in df.groupby("core_num"):
    for metric, ylabel, ymax in metrics:
        savepath = os.path.join(dirs["class3"], f"core{core_val}_{metric}.png")
        save_group_bar(df_core, "thread_num", "bs", metric, ylabel, ymax,
                       f"{ylabel} vs thread_num (core_num={core_val})", savepath)

# ==== 產生 HTML ====
def make_table_html(title, folder, index_list, metrics):
    html = f"<h2>{title}</h2><table border=1 cellspacing=0 cellpadding=4>"
    for metric, ylabel, _ in metrics:
        html += f"<tr><th colspan='{len(index_list)}'>{ylabel}</th></tr><tr>"
        for idx in index_list:
            fname = f"{idx}_{metric}.png"
            img_path = os.path.join(folder, fname)
            if os.path.exists(img_path):
                html += f"<td><img src='{img_path}' width='400'></td>"
            else:
                html += "<td>missing</td>"
        html += "</tr>"
    html += "</table>"
    return html

bs_list = sorted(df["bs"].unique(), key=parse_bs)
th_list = sorted(df["thread_num"].unique())
core_list = sorted(df["core_num"].unique())

html = "<html><head><meta charset='utf-8'><title>SPDK Results</title></head><body>"
html += make_table_html("第一類：固定 bs", dirs["class1"], bs_list, metrics)
html += make_table_html("第二類：固定 thread_num", dirs["class2"], [f"thread{t}" for t in th_list], metrics)
html += make_table_html("第三類：固定 core_num", dirs["class3"], [f"core{c}" for c in core_list], metrics)
html += "</body></html>"

with open("summary.html", "w", encoding="utf-8") as f:
    f.write(html)

print("Done, view with summary.html")
