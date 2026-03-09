"""
NASA Data Fetcher (Interactive Version) - Downloads data from the PDS4 archives for:
1: Lunar Prospector GRS data
2: Mars Curiosity DAN data
3: Ceres DAWN GRaND data
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
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        print(f"Warning: '{s}' is not a valid YYYY-MM-DD date. Falling back to default.")
        return None

def _get_files_in_bounds(csv_path: Path, lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> Set[str]:
    """Reads the CSV and returns a set of lowercase filenames within bounds."""
    valid_files = set()
    with open(csv_path, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                lat = float(row['lat'])
                lon = float(row['lon'])
                if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                    clean_name = row['filename'].strip().lower()
                    valid_files.add(clean_name)
            except (ValueError, KeyError):
                continue
    return valid_files

def get_float_input(prompt: str) -> float:
    """Helper to ensure the user enters a valid number."""
    while True:
        user_input = input(prompt).strip()
        try:
            return float(user_input)
        except ValueError:
            print("  -> Invalid input. Please enter a valid decimal number (e.g., -30.0).")

def main():
    print("="*40)
    print(" NASA PDS4 Data Fetcher ")
    print("="*40)

    # 1. Ask for Mission
    print("\nAvailable Missions:")
    print("  1: Lunar Prospector GRS")
    print("  2: Mars Curiosity DAN")
    print("  3: Ceres DAWN GRaND")
    
    mission_id = input("\nEnter the Mission ID (1, 2, or 3): ").strip()
    while mission_id not in MISSIONS:
        mission_id = input("Invalid choice. Please enter 1, 2, or 3: ").strip()
        
    spec = MISSIONS[mission_id]

    # 2. Ask for Filter Type
    print("\nHow would you like to filter the data?")
    if mission_id == "1":
        print("  1: Spatial (Coordinates)")
    print("  2: Date Range")
    print("  3: No Filter (Download Everything)")
    
    valid_filters = ["1", "2", "3"] if mission_id == "1" else ["2", "3"]
    filter_choice = input(f"Enter your choice ({'/'.join(valid_filters)}): ").strip()
    
    while filter_choice not in valid_filters:
        filter_choice = input(f"Invalid choice. Please enter {' or '.join(valid_filters)}: ").strip()

    # 3. Collect Filter Inputs
    lat_min = lat_max = lon_min = lon_max = None
    start_date_str = end_date_str = None

    if filter_choice == "1":
        print("\n--- Enter Spatial Bounds ---")
        lat_min = get_float_input("Minimum Latitude: ")
        lat_max = get_float_input("Maximum Latitude: ")
        lon_min = get_float_input("Minimum Longitude: ")
        lon_max = get_float_input("Maximum Longitude: ")
    elif filter_choice == "2":
        print("\n--- Enter Date Range (YYYY-MM-DD) ---")
        print("Press Enter without typing anything to use the mission's default date.")
        start_date_str = input(f"Start Date [Default: {spec.default_start}]: ").strip() or spec.default_start
        end_date_str = input(f"End Date   [Default: {spec.default_end}]: ").strip() or spec.default_end

    # Ask for an output folder
    outdir = input("\nEnter the base folder name to save data [Default: NASA_Data]: ").strip() or "NASA_Data"

    # Start the fetching process
    print(f"\n--- Connecting to NASA Servers for {spec.name} ---")
    dest_dir = Path(outdir) / spec.target_folder / "data"
    dest_dir.mkdir(parents=True, exist_ok=True)

    try:
        records = spec.list_records(spec.base_url)
    except Exception as e:
        print(f"Error connecting to NASA server: {e}")
        return

    # Apply the filters
    filtered = []
    if filter_choice == "1":
        print(f"Filtering by spatial bounds: Lat [{lat_min}, {lat_max}], Lon [{lon_min}, {lon_max}]")
        csv_path = Path("spatial_library_full.csv")
        
        if not csv_path.exists():
            print(f"Error: Could not find '{csv_path.name}' in the current directory.")
            return
            
        valid_filenames = _get_files_in_bounds(csv_path, lat_min, lat_max, lon_min, lon_max)
        print(f"Debug: Found {len(valid_filenames)} unique files in the CSV matching coordinates.")
        
        filtered = [
            r for r in records 
            if any(f.split("/")[-1].strip().lower() in valid_filenames for f in r.files)
        ]
        print(f"Found {len(filtered)} NASA server records matching spatial criteria.")

    elif filter_choice == "2":
        start_dt = _parse_date(start_date_str) or _parse_date(spec.default_start)
        end_dt = _parse_date(end_date_str) or _parse_date(spec.default_end)
        
        filtered = [
            r for r in records
            if (r.date_end >= start_dt) and (r.date_start <= end_dt)
        ]
        print(f"Found {len(filtered)} records matching date criteria ({start_dt} to {end_dt}).")

    else:
        filtered = records
        print(f"Found {len(filtered)} total records. Downloading all (no filtering).")

    if not filtered:
        print("No files matched your criteria. Exiting.")
        return

    # Download Loop
    print("\nStarting downloads...")
    for i, rec in enumerate(filtered):
        for remote_path in rec.files:
            url = urljoin(spec.base_url, remote_path)
            local_filename = remote_path.split("/")[-1]
            dest = dest_dir / local_filename

            if dest.exists():
                print(f"[{i+1}/{len(filtered)}] Skipping (already exists): {local_filename}")
                continue

            print(f"[{i+1}/{len(filtered)}] Downloading: {local_filename}")
            try:
                _write_stream(url, dest)
            except KeyboardInterrupt:
                print("\nDownload interrupted by user.")
                sys.exit(1)
            except Exception as e:
                print(f"Error downloading {local_filename}: {e}")

    print("\nFinished!")

if __name__ == "__main__":
    main()