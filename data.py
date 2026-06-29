#!/usr/bin/env python3

from pathlib import Path
import polars as pl

DATA_DIR = Path(".")
COLUMN = "raw_throughput_bps"

print(f"mean of {COLUMN}")

files = list(DATA_DIR.glob("*.csv"))

grouped_file_means = {
    "with_llm": [],
    "without_llm": [],
}

for file_path in files:
    name = file_path.name

    if "with_llm" in name:
        group = "with_llm"
    elif "without_llm" in name:
        group = "without_llm"
    else:
        continue

    df = pl.read_csv(file_path)

    if COLUMN not in df.columns:
        raise KeyError(f"{file_path} is missing column '{COLUMN}'")

    file_mean = df.select(pl.col(COLUMN).mean()).item()
    grouped_file_means[group].append(file_mean)

for group, means in grouped_file_means.items():
    if not means:
        print(f"{group}: no matching files")
        continue

    mean_of_file_means = sum(means) / len(means)
    print(f"{group} mean of file means: {mean_of_file_means:.6f}")

# # Optional: row-weighted mean across all rows in each group
# grouped_frames = {"with_llm": [], "without_llm": []}

# for file_path in files:
#     name = file_path.name

#     if "with_llm" in name:
#         group = "with_llm"
#     elif "without_llm" in name:
#         group = "without_llm"
#     else:
#         continue

#     df = pl.read_csv(file_path).select(COLUMN)
#     grouped_frames[group].append(df)

# for group, frames in grouped_frames.items():
#     if not frames:
#         continue

#     combined = pl.concat(frames, how="vertical")
#     row_weighted_mean = combined.select(pl.col(COLUMN).mean()).item()
#     print(f"{group} row-weighted mean: {row_weighted_mean:.6f}")
