"""
NASA PDS Data Plotter
"""

"""
Unified NASA PDS Data Plotter (Multi-file, Multi-mission)
Supports:
- Lunar Prospector (Moon): 1D spectrum, 2D lunar lat/lon, 3D orbit
- DAWN GRaND (Ceres): 1D spectrum / time series (no geometry for some products)
- Mars Curiosity (MSL DAN/GRS): 1D spectrum + rover lat/lon if available

Folders expected:
Moon/data/, Ceres/data/, Mars/data/
"""

import sys
import os
import re
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt

try:
    import pds4_tools as pds
except ImportError:
    print("Error: pip install pds4_tools")
    sys.exit(1)

import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "browser"


# ---------------- Utilities ----------------
def load_pds4_table(xml_file):
    struct = pds.read(str(xml_file), lazy_load=False)
    iden = struct[0].id
    data = struct[iden].data
    return data, data.dtype.names


def safe_counts(spectrum):
    return spectrum if spectrum.ndim == 1 else spectrum.sum(axis=1)


def find_col(names, candidates):
    for c in candidates:
        for n in names:
            if c.upper() in n.upper():
                return n
    return None


def parse_file_selection(sel_str, max_n):
    out = set()
    for part in sel_str.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-")
            for i in range(int(a), int(b) + 1):
                if 1 <= i <= max_n:
                    out.add(i - 1)
        else:
            i = int(part)
            if 1 <= i <= max_n:
                out.add(i - 1)
    return sorted(out)


# ---------------- Mission Parsers ----------------
def parse_lp(xml_file):
    data, names = load_pds4_table(xml_file)
    return {
        "spectrum": data["GROUP_0, Accepted Spectrum"],
        "lat": data["Subspacecraft_Latitude"],
        "lon": data["Subspacecraft_Longitude"],
        "alt": data["Spacecraft_Altitude"],
    }


def parse_dawn(xml_file):
    data, names = load_pds4_table(xml_file)
    spec_col = names[0]
    lat_col = find_col(names, ["LAT", "LATITUDE"])
    lon_col = find_col(names, ["LON", "LONGITUDE"])
    return {
        "spectrum": data[spec_col],
        "lat": data[lat_col] if lat_col else None,
        "lon": data[lon_col] if lon_col else None,
    }


def parse_msl(xml_file):
    data, names = load_pds4_table(xml_file)
    spec_col = names[0]
    lat_col = find_col(names, ["LAT", "LATITUDE"])
    lon_col = find_col(names, ["LON", "LONGITUDE"])
    return {
        "spectrum": data[spec_col],
        "lat": data[lat_col] if lat_col else None,
        "lon": data[lon_col] if lon_col else None,
    }


# ---------------- Plotting ----------------
def plot_spectrum_1d(spectrum, title):
    if spectrum.ndim > 1:
        spectrum = spectrum.sum(axis=0)
    plt.figure(figsize=(9, 5))
    plt.plot(spectrum, drawstyle="steps-mid")
    plt.yscale("log")
    plt.xlabel("Channel / Bin")
    plt.ylabel("Counts (log)")
    plt.title(title)
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def plot_latlon_2d(lat, lon, counts, title):
    fig = go.Figure(
        data=[
            go.Scatter(
                x=lon,
                y=lat,
                mode="markers",
                marker=dict(size=4, color=np.log10(counts + 1),
                            colorbar=dict(title="log10 Counts")),
            )
        ]
    )
    fig.update_layout(
        title=title,
        xaxis_title="Longitude (deg)",
        yaxis_title="Latitude (deg)",
        xaxis=dict(range=[-180, 180]),
        yaxis=dict(range=[-90, 90]),
    )
    fig.show()


def plot_lp_3d(lat, lon, alt, counts, title):
    fig = go.Figure(
        data=[
            go.Scatter3d(
                x=lon,
                y=lat,
                z=alt,
                mode="markers",
                marker=dict(size=3, color=np.log10(counts + 1),
                            colorbar=dict(title="log10 Counts")),
            )
        ]
    )
    fig.update_layout(
        title=title,
        scene=dict(
            xaxis_title="Longitude (deg)",
            yaxis_title="Latitude (deg)",
            zaxis_title="Altitude",
        ),
    )
    fig.show()


# ---------------- Main CLI ----------------
def run_plotter():
    print("\n--- Unified NASA Gamma-Ray Data Plotter ---")
    print("1: Moon (Lunar Prospector GRS)")
    print("2: Ceres (DAWN GRaND)")
    print("3: Mars (Curiosity DAN/GRS)")

    choice = input("\nSelect mission (1-3): ").strip()
    mission_dirs = {"1": "Moon", "2": "Ceres", "3": "Mars"}

    if choice not in mission_dirs:
        print("Invalid choice.")
        return

    mission = mission_dirs[choice]
    data_dir = Path(mission) / "data"
    files = sorted([f for f in data_dir.iterdir() if f.suffix.lower() == ".xml"])

    if not files:
        print(f"No XML files found in {data_dir}. Run data_fetcher.py first.")
        return

    for i, f in enumerate(files):
        print(f"{i+1}: {f.name}")

    sel = input("\nSelect files (e.g., 1,3,5-7): ").strip()
    idxs = parse_file_selection(sel, len(files))
    xml_files = [files[i] for i in idxs]

    if not xml_files:
        print("No valid files selected.")
        return

    if mission == "Moon":
        all_spec, all_lat, all_lon, all_alt, all_counts = [], [], [], [], []

        for xf in xml_files:
            parsed = parse_lp(xf)
            spec = parsed["spectrum"]
            all_spec.append(spec)

            c = safe_counts(spec)
            all_counts.append(c)
            all_lat.append(parsed["lat"])
            all_lon.append(parsed["lon"])
            all_alt.append(parsed["alt"])

        combined_spec = np.sum([s.sum(axis=0) if s.ndim > 1 else s for s in all_spec], axis=0)

        plot_spectrum_1d(combined_spec, f"LP GRS Combined Spectrum ({len(xml_files)} files)")
        plot_latlon_2d(np.concatenate(all_lat), np.concatenate(all_lon),
                       np.concatenate(all_counts),
                       f"LP GRS Combined Ground Track ({len(xml_files)} files)")
        plot_lp_3d(np.concatenate(all_lat), np.concatenate(all_lon),
                   np.concatenate(all_alt), np.concatenate(all_counts),
                   f"LP GRS Combined 3D Orbit ({len(xml_files)} files)")

    elif mission == "Ceres":
        all_spec = []
        for xf in xml_files:
            parsed = parse_dawn(xf)
            all_spec.append(parsed["spectrum"])

        combined_spec = np.sum([s if s.ndim == 1 else s.sum(axis=0) for s in all_spec], axis=0)
        plot_spectrum_1d(combined_spec, f"DAWN GRaND Combined Spectrum ({len(xml_files)} files)")
        print("[Info] DAWN geometry often not available; showing spectrum/time-series only.")

    elif mission == "Mars":
        all_spec, all_lat, all_lon, all_counts = [], [], [], []

        for xf in xml_files:
            parsed = parse_msl(xf)
            spec = parsed["spectrum"]
            all_spec.append(spec)

            if parsed["lat"] is not None:
                all_lat.append(parsed["lat"])
                all_lon.append(parsed["lon"])
                all_counts.append(safe_counts(spec))

        combined_spec = np.sum([s if s.ndim == 1 else s.sum(axis=0) for s in all_spec], axis=0)
        plot_spectrum_1d(combined_spec, f"MSL Combined Spectrum ({len(xml_files)} files)")

        if all_lat:
            plot_latlon_2d(np.concatenate(all_lat), np.concatenate(all_lon),
                           np.concatenate(all_counts),
                           f"MSL Combined Rover Traverse ({len(xml_files)} files)")
        else:
            print("[Info] MSL geometry not available in selected files.")

if __name__ == "__main__":
    run_plotter()
