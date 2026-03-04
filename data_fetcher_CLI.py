"""
NASA Data Fetcher - Downloads data from the PDS4 archives for various missions including:
Lunar Prospector GRS data
DAWN at Ceres data
Mars Curiosity DAN data
This version contains a simple CLI for user accessibility.
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
    if not targets:
        return []
    return [Record(date_start=date(2015, 1, 1), date_end=date(2018, 12, 31), files=targets)]


def _msl_records(base_url: str) -> List[Record]:
    hrefs = _list_directory(base_url)
    targets = [h for h in hrefs if h.lower().endswith((".xml", ".lbl", ".dat", ".tab"))]
    if not targets:
        return []
    return [Record(date_start=date(2012, 8, 6), date_end=date(2025, 12, 31), files=targets)]


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


def download_mission(spec: DataSpec, start_dt: date = None, end_dt: date = None):
    print(f"\n--- Downloading {spec.name} Data ---")
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

            if dest.exists():
                print(f"[{i+1}/{len(filtered)}] Skipping existing: {local_filename}")
                continue

            print(f"[{i+1}/{len(filtered)}] Downloading: {local_filename}")
            try:
                _write_stream(url, dest)
            except KeyboardInterrupt:
                print("\nDownload interrupted by user.")
                sys.exit(1)
            except Exception as e:
                print(f"Error downloading {local_filename}: {e}")


def main():
    print("Available Missions:")
    for k, v in MISSIONS.items():
        print(f"  {k}: {v.name}")

    choice = input("\nEnter mission number: ").strip()
    if choice not in MISSIONS:
        print("Invalid choice.")
        return

    spec = MISSIONS[choice]

    start_str = input(f"Enter start date (YYYY-MM-DD) or press Enter for all [{spec.default_start}]: ").strip()
    end_str = input(f"Enter end date (YYYY-MM-DD) or press Enter for all [{spec.default_end}]: ").strip()

    start_dt = _parse_date(start_str) if start_str else _parse_date(spec.default_start)
    end_dt = _parse_date(end_str) if end_str else _parse_date(spec.default_end)

    download_mission(spec, start_dt, end_dt)


if __name__ == "__main__":
    main()