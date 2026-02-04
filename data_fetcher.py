"""
NASA Data Fetcher - Webscraper for extracting data files for
Lunar Prospector GRS, Mars Curiosity DAN, and Ceres DAWN.
"""

import os
import re
import sys
import requests  # Added for Mars RDR
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Callable, List, Dict, Any, Optional
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
    return [h for h in p.hrefs if h and h not in ("../", "./", "/")]


def _write_stream(url: str, dest: Path, chunk: int = 1 << 14) -> None:
    req = Request(url, headers={"User-Agent": "nasagamma-fetcher/0.1"})
    with urlopen(req, timeout=60.0) as r, open(dest, "wb") as f:
        while True:
            b = r.read(chunk)
            if not b:
                break
            f.write(b)


@dataclass
class Record:
    key: str
    date_start: date
    date_end: date
    files: List[str]
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class MissionSpec:
    code: str
    label: str
    base_url: str
    list_records: Callable[[str], List[Record]]
    date_range: str
    target_folder: str
    data_format: str = "simple"  # "simple" or "rdr"


# ============================================================================
# Mars RDR Integration (from dan_vis2.py)
# ============================================================================

# Mars RDR Configuration
PDS_VOLUME_ROOT = "https://pds-geosciences.wustl.edu/msl/msl-m-dan-3_4-rdr-v1/msldan_1xxx"
PDS_INDEX_URL = f"{PDS_VOLUME_ROOT}/index/index.tab"

PDS_INDEX_COLS = {
    "VOLUME_ID":          (2,   12),
    "PATH_NAME":          (17,  13),
    "FILE_NAME":          (33,  40),
    "PRODUCT_ID":         (76,  40),
    "PRODUCT_VERSION_ID": (119, 12),
    "PRODUCT_TYPE":       (134, 12),
    "PRODUCT_CREATION":   (148, 23),
    "START_TIME":         (172, 23),
    "STOP_TIME":          (196, 23),
    "SCLK_START":         (221, 16),
    "SCLK_STOP":          (240, 16),
    "PLANET_DAY_NUMBER":  (258, 4),
    "RELEASE_ID":         (264, 4),
}

def _get_index_cache_path() -> Path:
    """Get path for cached Mars RDR index file."""
    cache_dir = Path.home() / ".cache" / "nasa_fetcher"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "mars_rdr_index.tab"

def _download_mars_index() -> List[str]:
    """Download and cache the Mars RDR index."""
    cache_path = _get_index_cache_path()
    
    # Use cache if fresh (less than 24 hours old)
    import time
    if cache_path.exists():
        mtime = cache_path.stat().st_mtime
        if (time.time() - mtime) < 86400:  # 24 hours
            with open(cache_path, 'r') as f:
                return f.read().splitlines()
    
    # Download fresh index
    print("  Downloading Mars RDR index...")
    resp = requests.get(PDS_INDEX_URL, stream=True, timeout=30)
    resp.raise_for_status()
    
    lines = []
    with open(cache_path, 'w', encoding='utf-8') as f:
        for line in resp.iter_lines(decode_unicode=True):
            if line:
                f.write(line + '\n')
                lines.append(line)
    
    print(f"  Cached {len(lines)} index entries")
    return lines

def _parse_mars_index_line(line: str) -> Dict[str, Any]:
    """Parse a line from Mars RDR INDEX.TAB."""
    result = {}
    for name, (start_byte, n_bytes) in PDS_INDEX_COLS.items():
        start = start_byte - 1
        end = start + n_bytes
        result[name] = line[start:end].strip()
    
    # Parse dates
    for time_key in ["START_TIME", "STOP_TIME"]:
        if result[time_key]:
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
                try:
                    result[time_key] = datetime.strptime(result[time_key], fmt)
                    break
                except ValueError:
                    continue
            else:
                result[time_key] = None
    
    # Parse sol number
    try:
        result["PLANET_DAY_NUMBER"] = int(result["PLANET_DAY_NUMBER"])
    except ValueError:
        result["PLANET_DAY_NUMBER"] = None
    
    return result

def _download_mars_rdr_file(url: str, dest: Path) -> bool:
    """Download a single Mars RDR file to Mars/data/."""
    if dest.exists():
        return True  # Already exists
    
    try:
        resp = requests.get(url, stream=True, timeout=60)
        resp.raise_for_status()
        
        with open(dest, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"    Error downloading {url.split('/')[-1]}: {e}")
        return False

def _list_mars_rdr_records(base_url: str) -> List[Record]:
    """
    List Mars RDR records using the dan_vis2 indexing approach.
    Returns Records with proper dates for filtering.
    """
    records = []
    index_lines = _download_mars_index()
    
    print(f"  Processing {len(index_lines)} index entries...")
    
    for line in index_lines:
        if not line.strip():
            continue
        
        try:
            row = _parse_mars_index_line(line)
            
            # Filter for active data products only
            if row.get("PRODUCT_TYPE") != "DAN_RDR_AC":
                continue
            
            start_time = row.get("START_TIME")
            if not start_time:
                continue
            
            file_name = row["FILE_NAME"]
            path_name = row["PATH_NAME"].lstrip("/")
            
            # Build URLs for label and data files
            lbl_url = f"{PDS_VOLUME_ROOT}/{path_name}/{file_name}"
            
            # We would need to fetch the label to find the data file,
            # but for simplicity, we'll assume .LBL files reference .DAT files
            # with the same basename
            data_file = file_name.replace('.LBL', '.DAT').replace('.lbl', '.dat')
            data_url = f"{PDS_VOLUME_ROOT}/{path_name}/{data_file}"
            
            # Create record
            rec = Record(
                key=f"mars_rdr_{row['PRODUCT_ID']}",
                date_start=start_time.date(),
                date_end=row.get("STOP_TIME", start_time).date() if row.get("STOP_TIME") else start_time.date(),
                files=[lbl_url, data_url],  # Store full URLs
                metadata={
                    "sol": row.get("PLANET_DAY_NUMBER"),
                    "product_id": row["PRODUCT_ID"],
                    "product_type": row["PRODUCT_TYPE"],
                    "lbl_filename": file_name,
                    "dat_filename": data_file,
                    "path": path_name
                }
            )
            records.append(rec)
            
        except Exception as e:
            # Skip parsing errors
            continue
    
    print(f"  Found {len(records)} Mars RDR active data records")
    return records

def _download_mars_rdr_record(record: Record, dest_dir: Path) -> int:
    """Download a Mars RDR record to Mars/data/."""
    downloaded = 0
    
    for url in record.files:
        filename = url.split("/")[-1]
        dest = dest_dir / filename
        
        if dest.exists():
            continue  # Skip existing
        
        print(f"    Downloading: {filename}")
        if _download_mars_rdr_file(url, dest):
            downloaded += 1
    
    return downloaded

# ============================================================================
# Existing Mission Functions (unchanged)
# ============================================================================

# --- MISSION: DAWN ---
_DAWN_DATE_RE = re.compile(r"(\d{6})-(\d{6})")

def _list_dawn_records(base_url: str) -> List[Record]:
    hrefs = _list_directory(base_url)
    xmls = [h for h in hrefs if h.lower().endswith(".xml")]
    tabs = {
        os.path.splitext(os.path.basename(h))[0]: h
        for h in hrefs
        if h.lower().endswith(".tab")
    }
    recs = []
    for x in xmls:
        stem = os.path.splitext(os.path.basename(x))[0]
        if stem in tabs:
            m = _DAWN_DATE_RE.search(stem)
            if m:
                d0 = datetime.strptime(m.group(1), "%y%m%d").date()
                d1 = datetime.strptime(m.group(2), "%y%m%d").date()
                recs.append(Record(stem, d0, d1, [x, tabs[stem]]))
    return recs

# --- MISSION: Lunar Prospector (PDS4 XML/DAT Version) ---
_LP_RE = re.compile(r"(\d{4})_(\d{3})_grs", re.IGNORECASE)

def _list_lp_records(base_url: str) -> List[Record]:
    """Lists PDS4 records (.xml + .dat) for LP, ignoring .lbl files."""
    hrefs = _list_directory(base_url)
    xmls = [h for h in hrefs if h.lower().endswith(".xml")]
    dats = {
        os.path.splitext(os.path.basename(h))[0].lower(): h
        for h in hrefs
        if h.lower().endswith(".dat")
    }

    recs = []
    for x in xmls:
        bn = os.path.basename(x)
        stem = os.path.splitext(bn)[0].lower()
        if stem in dats:
            m = _LP_RE.search(bn)
            if m:
                dt = datetime.strptime(f"{m.group(1)}-{m.group(2)}", "%Y-%j").date()
                recs.append(Record(stem, dt, dt, [x, dats[stem]]))
    return recs

# ============================================================================
# Mission Definitions (with integrated Mars RDR)
# ============================================================================

MISSIONS = {
    "1": MissionSpec(
        "LP",
        "Lunar Prospector GRS",
        "https://pds-geosciences.wustl.edu/lunar/lp-l-grs-3-rdr-v1/lp_2xxx/grs/",
        _list_lp_records,
        "1998-01-16 to 1999-07-28",
        "Moon",
        "simple"
    ),
    "2": MissionSpec(
        "DAWN",
        "DAWN GRAND CERES",
        "https://sbnarchive.psi.edu/pds4/dawn/grand/dawn-grand-ceres_1.0/data_calibrated/",
        _list_dawn_records,
        "2015-03-12 to 2018-11-01",
        "Ceres",
        "simple"
    ),
    "3": MissionSpec(
        "MSL_RDR",
        "Mars Curiosity (DAN RDR - Advanced)",
        PDS_VOLUME_ROOT,  # Mars RDR base URL
        _list_mars_rdr_records,  # Uses RDR indexing
        "2012-08-06 to Present",
        "Mars",  # Saves to Mars/data/
        "rdr"
    ),
}

# ============================================================================
# Enhanced run_fetcher with Mars RDR support
# ============================================================================

def run_fetcher():
    print("\n" + "="*50)
    print("NASA Data Fetcher CLI")
    print("="*50)
    
    for key, m in MISSIONS.items():
        format_note = " [RDR Format]" if m.data_format == "rdr" else ""
        print(f"{key}: {m.label}{format_note}")
    
    choice = input("\nSelect mission (1-3): ").strip()
    if choice not in MISSIONS:
        print("Invalid choice.")
        return
    
    spec = MISSIONS[choice]
    print(f"\n{'─'*40}")
    print(f"Mission: {spec.label}")
    
    if spec.data_format == "rdr":
        print("Format: Advanced RDR (binary tables)")
        print("Note: Uses INDEX.TAB for metadata lookup")
    
    print(f"Date Range: {spec.date_range}")
    print(f"Data will be saved to: {spec.target_folder}/data/")
    print('─'*40)
    
    # Date filtering
    start_str = input("\nStart Date (YYYY-MM-DD, or Enter for all): ").strip()
    end_str = input("End Date (YYYY-MM-DD, or Enter for all): ").strip()
    
    try:
        start_dt = date.fromisoformat(start_str) if start_str else None
        end_dt = date.fromisoformat(end_str) if end_str else None
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        return
    
    # Create destination directory
    dest_dir = Path(spec.target_folder) / "data"
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'─'*40}")
    print("Searching for records...")
    
    try:
        # Fetch records using appropriate method
        records = spec.list_records(spec.base_url)
        
        # Filter by date
        filtered = [
            r for r in records
            if (not start_dt or r.date_end >= start_dt)
            and (not end_dt or r.date_start <= end_dt)
        ]
        
        print(f"Found {len(records)} total records, {len(filtered)} match date filter.")
        
        if not filtered:
            print("No records match your criteria.")
            return
        
        # Confirm download for large batches
        if len(filtered) > 10:
            confirm = input(f"\nDownload {len(filtered)} file pairs? (y/n): ").strip().lower()
            if confirm != 'y':
                print("Download cancelled.")
                return
        
        # Download files
        print(f"\n{'─'*40}")
        print(f"Downloading to: {dest_dir.absolute()}")
        print('─'*40)
        
        total_downloaded = 0
        total_skipped = 0
        
        for i, rec in enumerate(filtered, 1):
            print(f"\n[{i}/{len(filtered)}] {rec.key}")
            
            if spec.data_format == "rdr":
                # Mars RDR format - download via HTTP
                downloaded = _download_mars_rdr_record(rec, dest_dir)
                total_downloaded += downloaded
                total_skipped += (2 - downloaded)  # 2 files per record
            else:
                # Simple format - use original downloader
                for remote_path in rec.files:
                    url = urljoin(spec.base_url, remote_path)
                    local_filename = remote_path.split("/")[-1]
                    dest = dest_dir / local_filename
                    
                    if dest.exists():
                        print(f"  [SKIP] {local_filename}")
                        total_skipped += 1
                        continue
                    
                    print(f"  [DOWNLOAD] {local_filename}")
                    try:
                        _write_stream(url, dest)
                        total_downloaded += 1
                    except Exception as e:
                        print(f"    Error: {e}")
        
        print(f"\n{'='*50}")
        print("DOWNLOAD COMPLETE!")
        print(f"Downloaded: {total_downloaded} files")
        print(f"Skipped (existing): {total_skipped} files")
        print(f"Saved to: {dest_dir.absolute()}")
        
        if spec.data_format == "rdr":
            print("\nNote: Mars RDR files are in advanced binary format.")
            print("Use dan_vis2.py functions to parse and plot the data.")
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

# ============================================================================

if __name__ == "__main__":
    run_fetcher()
