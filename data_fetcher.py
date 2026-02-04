"""
NASA Data Fetcher - Webscraper for extracting data files for
Lunar Prospector GRS, Mars Curiosity DAN, and Ceres DAWN.
"""

import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Callable, List
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


@dataclass
class MissionSpec:
    code: str
    label: str
    base_url: str
    list_records: Callable[[str], List[Record]]
    date_range: str
    target_folder: str


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
    # Create a mapping of stems to .dat files
    dats = {
        os.path.splitext(os.path.basename(h))[0].lower(): h
        for h in hrefs
        if h.lower().endswith(".dat")
    }

    recs = []
    for x in xmls:
        bn = os.path.basename(x)
        stem = os.path.splitext(bn)[0].lower()
        # Only include if there is a matching .dat file; .lbl files are skipped
        if stem in dats:
            m = _LP_RE.search(bn)
            if m:
                dt = datetime.strptime(f"{m.group(1)}-{m.group(2)}", "%Y-%j").date()
                recs.append(Record(stem, dt, dt, [x, dats[stem]]))
    return recs


# --- MISSION: Mars Curiosity (MSL) ---
_MSL_RE = re.compile(r"sol(\d{5})", re.IGNORECASE)


def _list_msl_records(base_url: str) -> List[Record]:
    hrefs = _list_directory(base_url)
    dat_files = [
        h for h in hrefs if h.lower().endswith(".dat") or h.lower().endswith(".tab")
    ]
    # Assuming MSL also uses XML labels for this specific fetcher logic
    lbl_files = {
        os.path.splitext(os.path.basename(h))[0]: h
        for h in hrefs
        if h.lower().endswith(".xml")
    }
    recs = []
    for f in dat_files:
        stem = os.path.splitext(os.path.basename(f))[0]
        if stem in lbl_files:
            mock_date = date(2012, 8, 6)
            recs.append(Record(stem, mock_date, mock_date, [f, lbl_files[stem]]))
    return recs


MISSIONS = {
    "1": MissionSpec(
        "LP",
        "Lunar Prospector GRS",
        "https://pds-geosciences.wustl.edu/lunar/lp-l-grs-3-rdr-v1/lp_2xxx/grs/",
        _list_lp_records,
        "1998-01-16 to 1999-07-28",
        "Moon",
    ),
    "2": MissionSpec(
        "DAWN",
        "DAWN GRAND CERES",
        "https://sbnarchive.psi.edu/pds4/dawn/grand/dawn-grand-ceres_1.0/data_calibrated/",
        _list_dawn_records,
        "2015-03-12 to 2018-11-01",
        "Ceres",
    ),
    "3": MissionSpec(
        "MSL",
        "Mars Curiosity (DAN/GRS)",
        "https://pds-geosciences.wustl.edu/msl/msl-m-dan-2-edr-v1/data/",
        _list_msl_records,
        "2012-08-06 to Present",
        "Mars",
    ),
}


def run_fetcher():
    print("\n--- NASA Data Fetcher CLI ---")
    for key, m in MISSIONS.items():
        print(f"{key}: {m.label}")

    choice = input("\nSelect mission (1-3): ").strip()
    if choice not in MISSIONS:
        print("Invalid choice.")
        return

    spec = MISSIONS[choice]
    print(f"\nConfiguration for {spec.label}: {spec.date_range}")
    start_str = input("Start Date (YYYY-MM-DD, or Enter for all): ").strip()
    end_str = input("End Date (YYYY-MM-DD, or Enter for all): ").strip()

    try:
        start_dt = date.fromisoformat(start_str) if start_str else None
        end_dt = date.fromisoformat(end_str) if end_str else None
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        return

    # Create hierarchical directory structure: Mission/data/
    dest_dir = Path(spec.target_folder) / "data"
    dest_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nSearching NASA PDS for records...")
    try:
        records = spec.list_records(spec.base_url)
    except Exception as e:
        print(f"Error connecting to NASA server: {e}")
        return

    filtered = [
        r
        for r in records
        if (not start_dt or r.date_end >= start_dt)
        and (not end_dt or r.date_start <= end_dt)
    ]

    print(f"Found {len(filtered)} records matching criteria. Starting download...\n")

    for i, rec in enumerate(filtered):
        for remote_path in rec.files:
            url = urljoin(spec.base_url, remote_path)
            local_filename = remote_path.split("/")[-1]
            dest = dest_dir / local_filename

            # Avoid re-downloading existing files
            if dest.exists():
                print(f"[{i+1}/{len(filtered)}] Skipping existing: {local_filename}")
                continue

            print(f"[{i+1}/{len(filtered)}] Downloading: {local_filename}")
            try:
                _write_stream(url, dest)
            except KeyboardInterrupt:
                print("\nDownload interrupted by user.")
                sys.exit(0)
            except Exception as e:
                print(f"  Error downloading {local_filename}: {e}")

    print(f"\nDone. Files saved to: {dest_dir.absolute()}")


if __name__ == "__main__":
    run_fetcher()