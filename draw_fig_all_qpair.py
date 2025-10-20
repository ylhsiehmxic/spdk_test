import pandas as pd
import matplotlib.pyplot as plt
import re
import os

# === 1. 讀取 CSV ===
df = pd.read_csv("data.csv")

# === 2. 單位轉換成 MiB/s ===
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
    elif "gb" in unit:   # 若單位非二進制
        return num * (1000/0.9537)  # 近似轉換成MiB
    elif "mb" in unit:
        return num
    elif "kb" in unit:
        return num / 1024
    else:
        return num  # fallback

df["throughput_mib"] = df["throughput"].apply(to_mibps)

# === 3. 找出 throughput 的全域 y 軸範圍 ===
ymax = df["throughput_mib"].max() * 1.1

# === 4. 為每個 bs 畫圖 ===
output_dir = "plots_bs"
os.makedirs(output_dir, exist_ok=True)

metrics = [
    ("throughput_mib", "Throughput (MiB/s)", ymax),
    ("thr_cpu_util", "Thread CPU Utilization (%)", None),
    ("po_cpu_util", "Poller CPU Utilization (%)", None),
]

for bs_val, df_bs in df.groupby("bs"):
    for metric, ylabel, fixed_ymax in metrics:
        plt.figure(figsize=(8, 5))

        # 依 core_num, thread_num 群組
        pivot_df = df_bs.pivot_table(
            index="core_num",
            columns="thread_num",
            values=metric,
            aggfunc="mean",
        ).sort_index()

        pivot_df.plot(kind="bar", ax=plt.gca())

        plt.title(f"{ylabel} vs core_num (bs={bs_val})")
        plt.xlabel("Core Number")
        plt.ylabel(ylabel)
        if fixed_ymax:
            plt.ylim(0, fixed_ymax)
        plt.legend(title="Thread Num", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()

        save_path = os.path.join(output_dir, f"{bs_val}_{metric}.png")
        plt.savefig(save_path, dpi=150)
        plt.close()

print(f"✅ 圖片已輸出到 {output_dir}/")
