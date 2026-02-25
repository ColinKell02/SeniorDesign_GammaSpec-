import os
import pandas as pd
import numpy as np
from pathlib import Path
from data_plotter import parse_lp, parse_dawn, parse_msl

# Configuration
MISSIONS = {
    "Moon": {"path": "Moon/data", "parser": parse_lp},
    #"Ceres": {"path": "Ceres/data", "parser": parse_dawn},
    #"Mars": {"path": "Mars/data", "parser": parse_msl}
}
OUTPUT_FILE = "spatial_library_full.csv"

def build_library():
    library_data = []

    print("Starting spatial indexing...")

    for mission_name, info in MISSIONS.items():
        data_dir = Path(info["path"])
        if not data_dir.exists():
            print(f"Skipping {mission_name}: Directory not found.")
            continue

        # Find all PDS4 XML labels
        xml_files = sorted([f for f in data_dir.iterdir() if f.suffix.lower() == ".xml"])
        print(f"Processing {len(xml_files)} files for {mission_name}...")

        for xml_file in xml_files:
            try:
                # Use your existing parsing logic
                parsed = info["parser"](xml_file)
                lats = parsed.get("lat")
                lons = parsed.get("lon")

                # Handle cases where geometry might be missing or mismatched
                if lats is None or lons is None or len(lats) == 0 or len(lats) != len(lons):
                    print(f"  [!] Missing or mismatched coordinates in {xml_file.name}")
                    continue

                # Iterate through all coordinates in the file and add them individually
                for idx, (lat, lon) in enumerate(zip(lats, lons)):
                    # Skip NaN (Not a Number) values if there's corrupted data
                    if np.isnan(lat) or np.isnan(lon):
                        continue
                        
                    library_data.append({
                        "mission": mission_name,
                        "filename": xml_file.name,
                        "record_index": idx,  # Helps Dash know WHICH spectrum to fetch
                        "lat": round(lat, 4),
                        "lon": round(lon, 4)
                    })

            except Exception as e:
                print(f"  [Error] Failed to parse {xml_file.name}: {e}")

    # Create the DataFrame and save to CSV
    if library_data:
        df = pd.DataFrame(library_data)
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"\nSuccess! Library built with {len(df)} individual coordinate entries.")
        print(f"Saved to: {OUTPUT_FILE}")
    else:
        print("\nNo data was indexed. Check your file paths and PDS4 formats.")

if __name__ == "__main__":
    build_library()