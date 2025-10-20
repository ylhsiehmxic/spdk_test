#!/usr/bin/env python3
import sys, os, re
import pandas as pd
import matplotlib.pyplot as plt

# === 1. 命令列輸入檢查 ===
if len(sys.argv) < 2:
    print("❌ 請提供 CSV 檔案名稱，例如：")
    print("   python plot_all.py data.csv")
    sys.exit(1)

csv_file = sys.argv[1]
if not os.path.exists(csv_file):
    print(f"❌ 找不到檔案：{csv_file}")
    sys.exit(1)

# === 2. 讀取 CSV ===
df = pd.read_csv(csv_file)

# === 3. 單位轉換: throughput 轉成 MiB/s ===
def to_mibps(val):
    if pd.isna(val): return None
    m = re.match(r"([0-9.]+)\s*([A-Za-z/]+)", str(val))
    if not m: return None
    num = float(m.group(1))
    unit = m.group(2).lower()
    if "gib" in unit: return num * 1024
    if "mib" in unit: return num
    if "kib" in unit: return num / 1024
    if "gb" in unit: return num * 953.7/1000
    if "mb" in unit: return num
    if "kb" in unit: return num / 1024
    return num

df["throughput_mib"] = df["throughput"].apply(to_mibps)

# === 4. CPU util 轉百分比 ===
for col in ["thr_cpu_util", "po_cpu_util"]:
    if col in df.columns:
        df[col] = df[col].astype(float) * 100

# === 5. 準備統一縱軸範圍 ===
ymax_throughput = df["throughput_mib"].max() * 1.1
ymax_iops = df["iops"].max() * 1.1

# === 6. 通用繪圖函式 ===
def plot_group(df_grouped, index, columns, value, ylabel, output_dir, prefix, fixed_ymax=None):
    os.makedirs(output_dir, exist_ok=True)
    for key, group in df_grouped:
        plt.figure(figsize=(8,5))
        pivot_df = group.pivot_table(index=index, columns=columns, values=value, aggfunc="mean").sort_index()
        pivot_df.plot(kind="bar", ax=plt.gca())
        plt.title(f"{ylabel} ({prefix}={key})")
        plt.xlabel(index)
        plt.ylabel(ylabel)
        if fixed_ymax: plt.ylim(0, fixed_ymax)
        plt.legend(title=columns, bbox_to_anchor=(1.05,1), loc="upper left")
        plt.tight_layout()
        fname = os.path.join(output_dir, f"{prefix}{key}_{value}.png")
        plt.savefig(fname, dpi=150)
        plt.close()

# === 7. 第一類 ===
plot_group(df.groupby("bs"), "core_num", "thread_num", "throughput_mib", "Throughput (MiB/s)", "plots_bs", "bs", ymax_throughput)
plot_group(df.groupby("bs"), "core_num", "thread_num", "iops", "IOPS", "plots_bs", "bs", ymax_iops)
plot_group(df.groupby("bs"), "core_num", "thread_num", "thr_cpu_util", "Thread CPU Util (%)", "plots_bs", "bs")
plot_group(df.groupby("bs"), "core_num", "thread_num", "po_cpu_util", "Poller CPU Util (%)", "plots_bs", "bs")

# === 8. 第二類 ===
plot_group(df.groupby("thread_num"), "bs", "core_num", "throughput_mib", "Throughput (MiB/s)", "plots_threadnum", "thread", ymax_throughput)
plot_group(df.groupby("thread_num"), "bs", "core_num", "iops", "IOPS", "plots_threadnum", "thread", ymax_iops)
plot_group(df.groupby("thread_num"), "bs", "core_num", "thr_cpu_util", "Thread CPU Util (%)", "plots_threadnum", "thread")
plot_group(df.groupby("thread_num"), "bs", "core_num", "po_cpu_util", "Poller CPU Util (%)", "plots_threadnum", "thread")

# === 9. 第三類 ===
plot_group(df.groupby("core_num"), "thread_num", "bs", "throughput_mib", "Throughput (MiB/s)", "plots_corenu", "core", ymax_throughput)
plot_group(df.groupby("core_num"), "thread_num", "bs", "iops", "IOPS", "plots_corenu", "core", ymax_iops)
plot_group(df.groupby("core_num"), "thread_num", "bs", "thr_cpu_util", "Thread CPU Util (%)", "plots_corenu", "core")
plot_group(df.groupby("core_num"), "thread_num", "bs", "po_cpu_util", "Poller CPU Util (%)", "plots_corenu", "core")

# === 10. 產生 HTML 表格 ===
def html_table(title, folder, group_col, metrics, group_values):
    html = f"<h2>{title}</h2><table border='1' cellspacing='0' cellpadding='5'>"
    html += "<tr><th>Metric</th>" + "".join(f"<th>{v}</th>" for v in group_values) + "</tr>"
    for metric in metrics:
        html += f"<tr><td>{metric}</td>"
        for v in group_values:
            fname = f"{group_col}{v}_{metric}.png"
            path = os.path.join(folder, fname)
            if os.path.exists(path):
                html += f"<td><img src='{path}' width='300'></td>"
            else:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table>"
    return html

metrics_list = ["throughput_mib", "iops", "thr_cpu_util", "po_cpu_util"]
html = "<html><body><h1>SPDK Performance Plots</h1>"

html += html_table("第一類 (各 bs)", "plots_bs", "bs", metrics_list, sorted(df["bs"].unique()))
html += html_table("第二類 (各 thread_num)", "plots_threadnum", "thread", metrics_list, sorted(df["thread_num"].unique()))
html += html_table("第三類 (各 core_num)", "plots_corenu", "core", metrics_list, sorted(df["core_num"].unique()))

html += "</body></html>"

with open("summary.html", "w") as f:
    f.write(html)

print("✅ 全部圖表完成，請開啟 summary.html 查看。")
