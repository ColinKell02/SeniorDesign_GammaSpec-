"""
NASA Data Fetcher - Downloads data from the PDS4 archives for various missions including:
Lunar Prospector GRS data
DAWN at Ceres data
Mars Curiosity DAN data
"""

import re
import sys
import csv
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Callable, List, Set
from urllib.parse import urljoin
from urllib.request import urlopen, Request
from html.parser import HTMLParser

# CONFIGURATION SETTINGS

# 1: Lunar Prospector GRS ("1")
# 2: Mars Curiosity DAN ("2")
# 3: Ceres DAWN GRaND ("3")
MISSION_ID = "1" 

# Filter data by either date, cooridnates, or no filter:
# Filter mode: Choose "date", "spatial", or "none"
FILTER_MODE = "spatial"

# Date Constraints (Used if FILTER_MODE = "date")
# Date range format: "YYYY-MM-DD". Set to None to use mission defaults.
START_DATE = None  
END_DATE   = None  

# Spatial Constraints (Used if FILTER_MODE = "spatial")
# Currently only supported for Lunar Prospector (Mission 1)
LAT_MIN = -30.0
LAT_MAX = 0.0
LON_MIN = 0.0
LON_MAX = 43.0

# The name of the local folder where data will be stored
# Select either Ceres, Moon, or Mars
BASE_OUTPUT_FOLDER = "Moon"

# END CONFIGURATION

class _LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        for k, v in attrs:
            if k.lower() == "href" and v:
                self.hrefs.append(v)

def _http_get_text(url: str, timeout: float = 30.0) -> str:
    req = Request(url, headers={"User-Agent": "nasagamma-fetcher/0.1"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")

def _list_directory(base_url: str) -> List[str]:
    html = _http_get_text(base_url)
    p = _LinkParser()
    p.feed(html)
    return [h for h in p.hrefs if not h.startswith("?")]

def _write_stream(url: str, dest: Path, chunk_size: int = 8192):
    req = Request(url, headers={"User-Agent": "nasagamma-fetcher/0.1"})
    with urlopen(req, timeout=30.0) as resp, open(dest, "wb") as f:
        while True:
            chunk = resp.read(chunk_size)
            if not chunk:
                break
            f.write(chunk)

@dataclass
class Record:
    date_start: date
    date_end: date
    files: List[str]

@dataclass
class DataSpec:
    name: str
    target_folder: str
    base_url: str
    list_records: Callable[[str], List[Record]]
    default_start: str = None
    default_end: str = None

def _lp_records(base_url: str) -> List[Record]:
    hrefs = _list_directory(base_url)
    xmls = [h for h in hrefs if h.lower().endswith(".xml")]
    dats = [h for h in hrefs if h.lower().endswith(".dat")]
    records = []
    for xml in xmls:
        base_name = xml[:-4]
        match_dat = f"{base_name}.dat"
        match_tab = f"{base_name}.tab"
        files = [xml]
        if match_dat in dats:
            files.append(match_dat)
        elif match_tab in hrefs:
            files.append(match_tab)
        match = re.search(r"(\d{4})(\d{3})", base_name)
        if match:
            year, doy = int(match.group(1)), int(match.group(2))
            dt = date.fromordinal(date(year, 1, 1).toordinal() + doy - 1)
            records.append(Record(date_start=dt, date_end=dt, files=files))
        else:
            records.append(Record(date_start=date(1998, 1, 1), date_end=date(1999, 12, 31), files=files))
    return records

def _dawn_records(base_url: str) -> List[Record]:
    hrefs = _list_directory(base_url)
    targets = [h for h in hrefs if h.lower().endswith((".xml", ".lbl", ".dat", ".tab"))]
    return [Record(date_start=date(2015, 1, 1), date_end=date(2018, 12, 31), files=targets)] if targets else []

def _msl_records(base_url: str) -> List[Record]:
    hrefs = _list_directory(base_url)
    targets = [h for h in hrefs if h.lower().endswith((".xml", ".lbl", ".dat", ".tab"))]
    return [Record(date_start=date(2012, 8, 6), date_end=date(2025, 12, 31), files=targets)] if targets else []

MISSIONS = {
    "1": DataSpec(
        name="Lunar Prospector GRS",
        target_folder="Moon",
        base_url="https://pds-geosciences.wustl.edu/lunar/lp-l-grs-3-rdr-v1/lp_2xxx/grs/",
        list_records=_lp_records,
        default_start="1998-01-01",
        default_end="1999-12-31"
    ),
    "2": DataSpec(
        name="Mars Curiosity DAN",
        target_folder="Mars",
        base_url="https://pds-geosciences.wustl.edu/msl/msl-m-dan-2-rdr-v1/msldan_1xxx/data/",
        list_records=_msl_records,
        default_start="2012-08-06",
        default_end="2025-12-31"
    ),
    "3": DataSpec(
        name="Ceres DAWN GRaND",
        target_folder="Ceres",
        base_url="https://sbnarchive.psi.edu/pds4/dawn/grand/data/",
        list_records=_dawn_records,
        default_start="2015-01-01",
        default_end="2018-12-31"
    ),
}

def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def _get_files_in_bounds(csv_path: Path, lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> Set[str]:
    """Reads the CSV and returns a set of lowercase filenames within bounds."""
    valid_files = set()
    # using utf-8-sig handles the invisible BOM character if the CSV was saved from Excel
    with open(csv_path, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                lat = float(row['lat'])
                lon = float(row['lon'])
                if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                    # Strip whitespace and force lowercase for bulletproof matching
                    clean_name = row['filename'].strip().lower()
                    valid_files.add(clean_name)
            except (ValueError, KeyError):
                # Skip rows with missing or invalid float data
                continue
    return valid_files

def main():
    if MISSION_ID not in MISSIONS:
        print(f"Error: MISSION_ID '{MISSION_ID}' is invalid. Use '1', '2', or '3'.")
        return

    spec = MISSIONS[MISSION_ID]
    print(f"--- Starting Download for {spec.name} ---")
    dest_dir = Path(BASE_OUTPUT_FOLDER) / spec.target_folder / "data"
    dest_dir.mkdir(parents=True, exist_ok=True)

    try:
        records = spec.list_records(spec.base_url)
    except Exception as e:
        print(f"Error connecting to NASA server: {e}")
        return

    filtered = []

    if FILTER_MODE == "spatial":
        if MISSION_ID != "1":
            print("Error: Spatial filtering is currently only supported for Lunar Prospector (Mission 1).")
            return
            
        print(f"Filtering by spatial bounds: Lat [{LAT_MIN}, {LAT_MAX}], Lon [{LON_MIN}, {LON_MAX}]")
        csv_path = Path("spatial_library_full.csv")
        
        if not csv_path.exists():
            print(f"Error: Could not find '{csv_path.name}' in the current directory.")
            return
            
        valid_filenames = _get_files_in_bounds(csv_path, LAT_MIN, LAT_MAX, LON_MIN, LON_MAX)
        print(f"Debug: Found {len(valid_filenames)} unique files in the CSV that match the coordinates.")
        
        # Keep records where any file in the record matches a valid filename from the CSV
        # We split by "/" to ensure we are only comparing the filename, not a URL path, and force lowercase.
        filtered = [
            r for r in records 
            if any(f.split("/")[-1].strip().lower() in valid_filenames for f in r.files)
        ]
        print(f"Found {len(filtered)} NASA server records matching spatial criteria.")

    elif FILTER_MODE == "date":
        start_dt = _parse_date(START_DATE) if START_DATE else _parse_date(spec.default_start)
        end_dt = _parse_date(END_DATE) if END_DATE else _parse_date(spec.default_end)
        
        filtered = [
            r for r in records
            if (r.date_end >= start_dt) and (r.date_start <= end_dt)
        ]
        print(f"Found {len(filtered)} records matching date criteria ({start_dt} to {end_dt}).")

    elif FILTER_MODE == "none":
        filtered = records
        print(f"Found {len(filtered)} total records. Downloading all (no filtering).")
        
    else:
        print(f"Error: Invalid FILTER_MODE '{FILTER_MODE}'. Choose 'date', 'spatial', or 'none'.")
        return

    for i, rec in enumerate(filtered):
        for remote_path in rec.files:
            url = urljoin(spec.base_url, remote_path)
            local_filename = remote_path.split("/")[-1]
            dest = dest_dir / local_filename

            if dest.exists():
                print(f"[{i+1}/{len(filtered)}] Skipping: {local_filename}")
                continue

            print(f"[{i+1}/{len(filtered)}] Downloading: {local_filename}")
            try:
                _write_stream(url, dest)
            except KeyboardInterrupt:
                print("\nDownload interrupted.")
                sys.exit(1)
            except Exception as e:
                print(f"Error: {e}")

if __name__ == "__main__":
    main()