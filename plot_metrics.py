#!/usr/bin/env python3

import csv
import os
import sys

import matplotlib
matplotlib.use("Agg")  # Use non-interactive backend for PDF output
import matplotlib.pyplot as plt

CSV_PATHS = sys.argv[1:]

if len(CSV_PATHS) < 1:
    raise ValueError("Usage: script.py file1.csv [file2.csv ...]")

# Output PDF name
OUT_PDF = "csv_comparison_plots.pdf"

# Read all CSVs
datasets = []
for path in CSV_PATHS:
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    if not rows:
        raise ValueError(f'CSV file "{path}" is empty')

    if fieldnames is None:
        raise ValueError(f'CSV file "{path}" has no header')

    datasets.append({
        "path": path,
        "name": os.path.basename(path),
        "rows": rows,
        "fieldnames": fieldnames,
    })

# Only plot columns shared by all CSVs, preserving first file order
common_columns = [
    col for col in datasets[0]["fieldnames"]
    if all(col in ds["fieldnames"] for ds in datasets)
]

if not common_columns:
    raise ValueError("No common columns found across the provided CSV files")

num_cols = len(common_columns)

# Tall figure: one subplot per column
fig, axes = plt.subplots(num_cols, 1, figsize=(12, 4 * num_cols), sharex=False)

if num_cols == 1:
    axes = [axes]

for ax, col in zip(axes, common_columns):
    for ds in datasets:
        try:
            y = [float(row[col]) for row in ds["rows"]]
        except ValueError:
            raise ValueError(
                f'Column "{col}" in file "{ds["path"]}" contains non-numeric values and cannot be plotted'
            )

        x = list(range(len(y)))
        ax.plot(x, y, label=ds["name"])

    ax.set_title(col)
    ax.set_ylabel("Value")
    ax.grid(True)
    ax.legend()

axes[-1].set_xlabel("Sample")
fig.tight_layout()

fig.savefig(OUT_PDF, format="pdf", bbox_inches="tight")
print(f"Saved to {OUT_PDF}")
