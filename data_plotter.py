"""
NASA PDS Data Plotter - Standard Tools Version
Uses pds4_tools (Official) and pvl (Industry Standard)
Formatting: Black
"""

import matplotlib
matplotlib.use('QtAgg')  # Forces the interactive Qt window
import matplotlib.pyplot as plt
import sys
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt

# Standard PDS Parsing Libraries
try:
    import pds4_tools as pds4
    import pvl
except ImportError:
    print("Missing standard tools. Run: pip install pds4_tools pvl numpy matplotlib")
    sys.exit(11
    )


def plot_pds4(file_path: Path, label: str):
    """Uses the official NASA pds4_tools for DAWN data."""
    print(f"[Status] Opening PDS4 dataset: {file_path.name}")
    try:
        struct = pds4.read(str(file_path), quiet=True)
        # Usually, the second structure [1] contains the actual data table
        data = struct[1].data
        # Search for the column containing spectral counts
        col_name = next((n for n in data.dtype.names if "COUNT" in n.upper() or "SPEC" in n.upper()), data.dtype.names[0])
        spectrum = data[col_name]
        
        # If there are multiple rows, we average them into one spectrum
        if spectrum.ndim > 1:
            spectrum = np.mean(spectrum, axis=0)

        _generate_plot(spectrum, file_path.name, label, col_name)
    except Exception as e:
        print(f"[Error] pds4_tools failed: {e}")


def plot_pds3(file_path: Path, label_name: str):
    """Uses pvl to parse labels and numpy to read binary PDS3 data (LP/MSL)."""
    lbl_path = file_path.with_suffix(".lbl")
    if not lbl_path.exists():
        lbl_path = file_path.with_suffix(".LBL")

    print(f"[Status] Parsing PDS3 Label: {lbl_path.name}")
    try:
        # 1. Parse the label to find the structure
        meta = pvl.load(str(lbl_path))
        
        # 2. Get the object name (usually TABLE or GRS_SPECTRUM)
        # PDS3 labels often use a pointer like ^TABLE = "file.dat"
        data_ptr = next((k for k in meta.keys() if k.startswith("^")), "^TABLE")
        
        # 3. Read the raw binary data
        # Most LP and MSL GRS data is stored as 32-bit floats or 16-bit integers
        raw_data = np.fromfile(str(file_path), dtype=np.float32)
        
        _generate_plot(raw_data, file_path.name, label_name, "Counts")
    except Exception as e:
        print(f"[Error] PDS3 parsing failed: {e}")


def _generate_plot(data, filename, mission, y_label):
    """Standardized plotting logic for all missions."""
    plt.figure(figsize=(10, 6))
    plt.plot(data, drawstyle="steps-mid", color="royalblue", linewidth=1.5)
    
    plt.yscale("log") # Vital for seeing small elemental peaks
    plt.title(f"{mission} Spectrum\nFile: {filename}")
    plt.xlabel("Energy Channel (Bin)")
    plt.ylabel(f"{y_label} (Log Scale)")
    plt.grid(True, which="both", alpha=0.3, linestyle="--")
    plt.tight_layout()
    
    print(f"[Success] Plotting {filename}. Close window to continue.")
    plt.show()


def run_plotter():
    print("\n--- NASA PDS Data Plotter (Standard Tools) ---")
    missions = {
        "1": ("Moon", "Lunar Prospector"),
        "2": ("Ceres", "DAWN"),
        "3": ("Mars", "Mars Curiosity"),
    }

    for k, v in missions.items():
        print(f"{k}: {v[1]}")

    choice = input("\nSelect mission (1-3): ").strip()
    if choice not in missions:
        return

    folder, label = missions[choice]
    data_dir = Path(folder) / "data"

    if not data_dir.exists():
        print(f"[Error] Folder not found: {data_dir}")
        return

    # For PDS4 (Ceres), we select the .xml file; for PDS3, we select the .dat file
    ext = ".xml" if choice == "2" else ".dat"
    files = sorted([f for f in data_dir.iterdir() if f.suffix.lower() == ext])

    if not files:
        print(f"No {ext} files found in {data_dir}.")
        return

    for i, f in enumerate(files):
        print(f"{i+1}: {f.name}")

    try:
        idx = int(input(f"\nSelect file (1-{len(files)}): "))
        selected = files[idx - 1]
        
        if choice == "2":
            plot_pds4(selected, label)
        else:
            plot_pds3(selected, label)
    except (ValueError, IndexError):
        print("Invalid selection.")


if __name__ == "__main__":
    run_plotter()