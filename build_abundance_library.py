"""
One time use script for creating the lunar Gelemental abundace library.
Requires all elemental abundace derived data files for the 100km dataset.
"""

import os
import sys
from pathlib import Path
import pandas as pd

try:
    import pds4_tools as pds
except ImportError:
    print("Error: pip install pds4_tools")
    sys.exit(1)

DATA_DIR = Path("Moon/data")
OUTPUT_CSV = "lunar_abundances.csv"

def build_library():
    print("--- Building Lunar Abundance Spatial Library ---")
    
    if not DATA_DIR.exists():
        print(f"Error: Directory {DATA_DIR} does not exist.")
        return

    # Find all PDS4 XML files related to elemental abundances
    abundance_files = list(DATA_DIR.glob("*elem_abundance*.xml"))
    
    if not abundance_files:
        print("No abundance XML files found in Moon/data/. Make sure you ran the fetcher.")
        return

    all_dataframes = []

    for xml_file in abundance_files:
        print(f"Processing: {xml_file.name}")
        try:
            # Read the PDS4 data
            struct = pds.read(str(xml_file), lazy_load=False, quiet=True)
            iden = struct[0].id
            data = struct[iden].data
            col_names = data.dtype.names
            
            # Convert the PDS array to a standard Pandas Dictionary
            df_dict = {}
            for col in col_names:
                # We want spatial coords and weight fractions (usually prefixed with W_ or w_)
                if "LAT" in col.upper() or "LON" in col.upper() or col.upper().startswith("W_"):
                    df_dict[col.upper()] = data[col]
            
            df = pd.DataFrame(df_dict)
            
            # Calculate Center Coordinates for the VTK Heatmap
            if 'MIN_LAT' in df.columns and 'MAX_LAT' in df.columns:
                df['CENTER_LAT'] = (df['MIN_LAT'] + df['MAX_LAT']) / 2.0
            if 'MIN_LON' in df.columns and 'MAX_LON' in df.columns:
                df['CENTER_LON'] = (df['MIN_LON'] + df['MAX_LON']) / 2.0
                
            # Keep track of the file source/resolution
            df['SOURCE_FILE'] = xml_file.name
            
            all_dataframes.append(df)
            print(f"  -> Extracted {len(df)} pixels.")

        except Exception as e:
            print(f"  -> Failed to parse {xml_file.name}: {e}")

    if all_dataframes:
        # Combine all resolutions (2deg, 5deg, etc.) into one master library
        master_df = pd.concat(all_dataframes, ignore_index=True)
        master_df.to_csv(OUTPUT_CSV, index=False)
        print(f"\nSuccess! Abundance library saved to {OUTPUT_CSV}")
        print(f"Total data points: {len(master_df)}")
        print(f"Available Elements: {[c for c in master_df.columns if c.startswith('W_')]}")
    else:
        print("\nFailed to extract any data.")

if __name__ == "__main__":
    build_library()