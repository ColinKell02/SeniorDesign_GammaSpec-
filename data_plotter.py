"""
NASA PDS Data Plotter
"""

import sys
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt

try:
    import pds4_tools as pds
except ImportError:
    print("Error: 'pds4_tools' missing. Run: pip install pds4_tools")
    sys.exit(1)


def plot_mentor_style(file_path: Path, mission_label: str):
    """
    Directly mimics the logic from read_data_LP.py.
    """
    print(f"\n[Status] Reading {file_path.name}...")
    try:
        # Step 1: Read the XML label
        struct = pds.read(str(file_path), lazy_load=False)
        
        # Step 2: Access data via structure ID
        iden = struct[0].id
        data = struct[iden].data
        names = data.dtype.names
        
        # Step 3: Identify spectral data column
        # Usually the first column in LP data as per mentor script
        target_col = names[0]
        spectrum = data[target_col]
        
        # Step 4: Accumulate spectrum if 2D
        if spectrum.ndim > 1:
            spectrum = spectrum.sum(axis=0)

        # Step 5: Plotting
        plt.figure(figsize=(10, 6))
        plt.plot(spectrum, drawstyle="steps-mid", color="royalblue")
        plt.yscale("log")
        plt.title(f"{mission_label} Spectrum: {file_path.name}")
        plt.xlabel("Channel / Bin")
        plt.ylabel("Counts (Log)")
        plt.grid(True, which="both", linestyle="--", alpha=0.5)
        plt.tight_layout()
        
        print(f"[Success] Plotting. Close window to continue.")
        plt.show()

    except Exception as e:
        print(f"[Error] Failed to process file: {e}")


def run_plotter():
    print("\n--- NASA Data Plotter (Mentor Style) ---")
    missions = {"1": ("Moon", "Lunar Prospector"), "2": ("Ceres", "DAWN"), "3": ("Mars", "Mars Curiosity")}
    for k, v in missions.items():
        print(f"{k}: {v[1]}")

    choice = input("\nSelect mission (1-3): ").strip()
    if choice not in missions:
        return

    folder, label = missions[choice]
    data_dir = Path(folder) / "data"

    # We look for .xml files because that's what pds4_tools requires
    files = sorted([f for f in data_dir.iterdir() if f.suffix.lower() == ".xml"])

    if not files:
        print(f"No XML labels found in {data_dir}. Run the updated fetcher first.")
        return

    for i, f in enumerate(files):
        print(f"{i+1}: {f.name}")

    try:
        idx = int(input(f"\nSelect file (1-{len(files)}): "))
        plot_mentor_style(files[idx - 1], label)
    except (ValueError, IndexError):
        print("Invalid selection.")


if __name__ == "__main__":
    run_plotter()