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


# ============================================================
# AUTOMATIC DAWN SPICE KERNEL DISCOVERY
# ============================================================

GENERIC_KERNEL_DIRS = {
    "lsk": "https://naif.jpl.nasa.gov/pub/naif/generic_kernels/lsk/",
    "pck": "https://naif.jpl.nasa.gov/pub/naif/generic_kernels/pck/",
    "spk": "https://naif.jpl.nasa.gov/pub/naif/generic_kernels/spk/planets/"
}

DAWN_KERNEL_DIRS = {
    "fk":   "https://naif.jpl.nasa.gov/pub/naif/DAWN/kernels/fk/",
    "sclk": "https://naif.jpl.nasa.gov/pub/naif/DAWN/kernels/sclk/",
    "spk":  "https://naif.jpl.nasa.gov/pub/naif/DAWN/kernels/spk/",
    "ck":   "https://naif.jpl.nasa.gov/pub/naif/DAWN/kernels/ck/"
}

# Mode → which directories to pull from. Each value is a list of (label, url)
# pairs.  "minimal" deliberately omits CK (camera-pointing kernels) because
# they're the largest single category and aren't needed for orbit tracks or
# spkezr position queries — only for instrument-pointing analysis.
KERNEL_MODE_DIRS = {
    "minimal": [
        ("lsk",        GENERIC_KERNEL_DIRS["lsk"]),
        ("pck",        GENERIC_KERNEL_DIRS["pck"]),
        ("planet-spk", GENERIC_KERNEL_DIRS["spk"]),
        ("fk",         DAWN_KERNEL_DIRS["fk"]),
        ("sclk",       DAWN_KERNEL_DIRS["sclk"]),
        ("dawn-spk",   DAWN_KERNEL_DIRS["spk"]),
        # NB: no "ck" entry — minimal mode skips pointing kernels
    ],
    "all": [
        ("lsk",        GENERIC_KERNEL_DIRS["lsk"]),
        ("pck",        GENERIC_KERNEL_DIRS["pck"]),
        ("planet-spk", GENERIC_KERNEL_DIRS["spk"]),
        ("fk",         DAWN_KERNEL_DIRS["fk"]),
        ("sclk",       DAWN_KERNEL_DIRS["sclk"]),
        ("dawn-spk",   DAWN_KERNEL_DIRS["spk"]),
        ("ck",         DAWN_KERNEL_DIRS["ck"]),
    ],
}

VALID_KERNEL_EXT = (".bsp", ".bc", ".tls", ".tpc", ".tf", ".tsc")

# ============================================================
# CERES MISSION TIME WINDOW
# ============================================================

CERES_START_YEAR = 2015
CERES_END_YEAR = 2018


# ============================================================
# HTML DIRECTORY PARSER
# ============================================================

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


def _write_stream(url: str, dest: Path, chunk_size: int = 65536,
                  cancel_flag=None,
                  progress_threshold_bytes: int = 1_000_000):
    """
    Download `url` to `dest`, streaming in chunks.

    Differences from a naive urlopen+read:

    * Connect-timeout is short (10 s) so a hung server fails fast instead of
      stalling the worker for the full 30 s default.
    * The read loop checks `cancel_flag` (a threading.Event) every chunk, so
      cancelling responds within ~1 chunk rather than waiting for the file
      to finish.  When cancelled mid-file, the partial output is removed.
    * For files larger than `progress_threshold_bytes` (1 MB by default),
      a one-line progress update is printed every ~2 s so the activity log
      doesn't appear frozen on a slow file.

    Raises on connect/HTTP error so the caller's try/except can record it.
    """
    import time as _time
    req = Request(url, headers={"User-Agent": "nasagamma-fetcher/0.1"})

    # Open the connection with a short connect timeout. The actual read
    # loop below uses its own per-chunk cadence and respects cancel_flag.
    resp = urlopen(req, timeout=10.0)
    try:
        # Try to learn the total size from Content-Length so we can render
        # a percentage. Header may be absent → progress shows raw bytes.
        try:
            total = int(resp.headers.get("Content-Length") or 0)
        except (TypeError, ValueError):
            total = 0

        downloaded = 0
        last_print = _time.monotonic()
        cancelled  = False

        with open(dest, "wb") as f:
            while True:
                if cancel_flag is not None and cancel_flag.is_set():
                    cancelled = True
                    break
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)

                # Progress line — only for sizable files, and at most once
                # every ~2 s so we don't spam the log.
                now = _time.monotonic()
                if total > progress_threshold_bytes and (now - last_print) > 2.0:
                    pct = (downloaded / total * 100) if total else 0
                    print(f"      … {downloaded/1e6:.1f}/{total/1e6:.1f} MB "
                          f"({pct:.0f}%)",
                          flush=True)
                    last_print = now
    finally:
        resp.close()

    if cancelled:
        # Don't leave a partial file on disk
        try:
            dest.unlink()
        except OSError:
            pass
        raise InterruptedError("download cancelled by user")


# ============================================================
# KERNEL FILTERS
# ============================================================

def kernel_in_ceres_window(filename: str) -> bool:
    """
    Keep only kernels relevant to the Ceres science phase (2015–2018).
    Handles both YYYY and YYMMDD style timestamps.
    """

    # full-year matches
    years = re.findall(r"20\d{2}", filename)

    for y in years:
        y = int(y)
        if 2015 <= y <= 2018:
            return True

    # YYMMDD-style dates
    short_dates = re.findall(r"\d{6}", filename)

    for d in short_dates:
        year = int(d[:2]) + 2000
        if 2015 <= year <= 2018:
            return True

    # generic kernels with no dates in the name (LSK, PCK, FK, etc.)
    if not years and not short_dates:
        return True

    return False


def is_minimal_kernel(filename: str, dir_label: str) -> bool:
    """
    Extra filter applied ONLY in minimal mode. Aggressive: keeps the smallest
    workable set that still produces orbit tracks in the viewer.
    """
    name = filename.lower()

    # Skip predicted (pre-mission) trajectories — minimal uses reconstructed only
    if name.startswith("dawn_pred_"):
        return False

    # Skip "ql" / quicklook variants (already handled in main fetcher but
    # repeated here for safety in case directory listings change)
    if name.startswith("dawn_ql"):
        return False

    return True


def pick_one_planet_spk(planet_files: List[str]) -> List[str]:
    """
    From a list of planetary SPK filenames (e.g. de440.bsp, de440s.bsp,
    de421.bsp, ...), pick exactly one — preferring de440 > de438 > de430 >
    de421.  This avoids downloading every DE revision in minimal mode.
    """
    candidates = [f for f in planet_files if f.lower().endswith(".bsp")]
    if not candidates:
        return []

    for prefix in ("de440s", "de440", "de438s", "de438", "de432s", "de430", "de421"):
        for f in candidates:
            if f.lower().startswith(prefix):
                return [f]

    # Fallback: alphabetically last (usually highest revision)
    return [sorted(candidates)[-1]]


# ============================================================
# SPICE DOWNLOAD FUNCTION
# ============================================================

def download_dawn_ceres_spice(outdir: Path, mode: str = "minimal"):
    """
    Download SPICE kernels for the DAWN-at-Ceres mission phase.

    Parameters
    ----------
    outdir : Path
        Top-level mission output directory (e.g. NASA_Data/Ceres). The
        kernels are placed in <outdir>/spice/.
    mode : {"none", "minimal", "all"}
        - "none"    : do not download any kernels. Returns an empty list.
                      Useful for users who only want to view spectra.
        - "minimal" : the smallest set that still produces orbit tracks
                      and lets the viewer derive lat/lon from ET. Skips
                      CK (camera-pointing) kernels and predicted trajectory
                      SPKs, and keeps only one planetary SPK revision.
                      Typically ~200 MB total.
        - "all"     : every kernel in the Ceres time window across all
                      categories — the original fetcher behaviour.
                      Multi-GB.
    """
    if mode == "none":
        print("\n--- Skipping SPICE kernel download (mode=none) ---")
        print("    The viewer will run in spectrum-only mode.")
        print("    Orbit tracks and TAB heatmap will be unavailable until")
        print("    SPICE kernels are downloaded.")
        return []

    if mode not in KERNEL_MODE_DIRS:
        print(f"\nUnknown SPICE mode '{mode}', falling back to 'minimal'.")
        mode = "minimal"

    print(f"\n--- Downloading SPICE Kernels for DAWN at Ceres (mode={mode}) ---")
    if mode == "minimal":
        print("    Minimal set: LSK + PCK + FK + SCLK + reconstructed SPKs.")
        print("    No CK (pointing) kernels.  Approx. 200 MB total.")
    else:
        print("    Full set: every kernel in the 2015-2018 window, ")
        print("    including CK pointing kernels.  Multi-GB download.")

    spice_dir = outdir / "spice"
    spice_dir.mkdir(parents=True, exist_ok=True)

    kernels = []  # list of (url, filename) pairs that we eventually keep

    # --- Pass 1: list every requested directory, build the candidate set
    for dir_label, base_url in KERNEL_MODE_DIRS[mode]:
        try:
            files = _list_directory(base_url)
        except Exception as e:
            print(f"  [{dir_label}] could not list {base_url}: {e}")
            continue

        # Common filtering shared by all modes
        files = [
            f for f in files
            if f.lower().endswith(VALID_KERNEL_EXT)
            and not f.startswith("mailto")
            and not f.startswith("dawn_ql")
            and kernel_in_ceres_window(f)
        ]

        # Mode-specific filtering
        if mode == "minimal":
            files = [f for f in files if is_minimal_kernel(f, dir_label)]

            # Special case: planetary SPK directory — pick exactly ONE de4xx
            if dir_label == "planet-spk":
                files = pick_one_planet_spk(files)

        if not files:
            print(f"  [{dir_label}] no files matched after filtering")
            continue

        print(f"  [{dir_label}] {len(files)} file(s) selected")
        for f in files:
            kernels.append((urljoin(base_url, f), f))

    # --- Pass 2: download
    n_skipped = 0
    n_downloaded = 0
    n_failed = 0
    for url, filename in kernels:
        dest = spice_dir / filename

        if dest.exists():
            n_skipped += 1
            continue

        print(f"Downloading SPICE kernel: {filename}")
        try:
            _write_stream(url, dest)
            n_downloaded += 1
        except Exception as e:
            print(f"  Error downloading {filename}: {e}")
            n_failed += 1

    print(f"\n  SPICE summary: {n_downloaded} downloaded, "
          f"{n_skipped} already present, {n_failed} failed.")

    return kernels


def create_meta_kernel(spice_dir: Path, kernels):
    """
    Write a SPICE meta-kernel that lists every file we just downloaded.
    Skipped if no kernels were downloaded (e.g. mode=none).
    """
    if not kernels:
        print("\nNo kernels to include in meta-kernel — skipping.")
        return

    mk_path = spice_dir / "dawn_ceres.tm"

    print("\nCreating SPICE meta-kernel...")

    with open(mk_path, "w") as f:
        f.write("KPL/MK\n\n")
        f.write("\\begindata\n\n")
        f.write("KERNELS_TO_LOAD = (\n")
        for _, filename in kernels:
            f.write(f"   '{filename}',\n")
        f.write(")\n\n")
        f.write("\\begintext\n")

    print(f"Meta-kernel created: {mk_path}")


# ============================================================
# DATA STRUCTURES
# ============================================================

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


# ============================================================
# RECORD LISTING FUNCTIONS
# ============================================================

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

            records.append(Record(date_start=date(1998,1,1),
                                  date_end=date(1999,12,31),
                                  files=files))

    return records


# ============================================================
# CERES DAWN GRaND — RECURSIVE FILE LISTING
# ============================================================
# The PDS4 Ceres GRaND archive at
#   https://sbnarchive.psi.edu/pds4/dawn/grand/dawn-grand-ceres_1.0/data_calibrated/
# is organized by science phase / collection (not by year) and the layout
# has changed over time.  Rather than baking in any specific subdirectory
# names, we simply walk whatever directory tree is there and pick up every
# .xml / .tab / .dat / .lbl file we find, paired by basename.
#
# Filenames embed the YYMMDD-YYMMDD date range of the contained data
# (Dawn's standard naming convention), so we can extract real per-record
# dates for filtering.

_DAWN_DATE_RE = re.compile(r"(\d{6})-(\d{6})")
_DAWN_FALLBACK_START = date(2015, 1, 1)
_DAWN_FALLBACK_END   = date(2018, 12, 31)
_DAWN_MAX_DEPTH      = 4   # safety cap: stop recursing past this depth


def _dawn_extract_dates(stem: str) -> tuple:
    """
    Try to pull a YYMMDD-YYMMDD date range out of a Dawn filename stem.
    Returns (date_start, date_end), or the mission-window fallback if no
    date pattern is found.
    """
    m = _DAWN_DATE_RE.search(stem)
    if not m:
        return _DAWN_FALLBACK_START, _DAWN_FALLBACK_END
    try:
        d0 = datetime.strptime(m.group(1), "%y%m%d").date()
        d1 = datetime.strptime(m.group(2), "%y%m%d").date()
        return d0, d1
    except ValueError:
        return _DAWN_FALLBACK_START, _DAWN_FALLBACK_END


# Patterns of filenames to exclude — these are PDS archive packaging
# artifacts that show up in directory listings but aren't science data.
# The URLs the walker constructs for them often 404 because they live at
# the registry root rather than inside any specific bundle.
_DAWN_EXCLUDE_PATTERNS = (
    "_sip_v",                # Submission Information Package manifests
    "_checksum_manifest",    # MD5 manifests
    "_transfer_manifest",    # PDS transfer manifests
    "_aip_v",                # Archive Information Package manifests
    "bundle_",               # bundle-level XML labels
    "collection_",           # collection-level table-of-contents files
)


def _dawn_skip_filename(basename: str) -> bool:
    """Return True if this filename is a bundle-manifest artifact, not science data."""
    low = basename.lower()
    return any(p in low for p in _DAWN_EXCLUDE_PATTERNS)


def _dawn_walk(base_url: str, rel_prefix: str = "", depth: int = 0,
               root_url: str = None):
    """
    Recursively walk a PDS Apache directory tree under `root_url`, yielding
    (relative_path, is_directory) pairs for every link found.

    Stays inside `root_url` — links that resolve outside of it (e.g. into
    a sibling Vesta bundle, or back up into the parent registry index)
    are skipped.  Stops at _DAWN_MAX_DEPTH.
    """
    if depth > _DAWN_MAX_DEPTH:
        return

    if root_url is None:
        root_url = base_url

    try:
        hrefs = _list_directory(base_url)
    except Exception as e:
        print(f"  could not list {base_url}: {e}")
        return

    for h in hrefs:
        # Skip parent links and query-string sort links
        if h in ("../", "./", "/") or h.startswith("?") or h.startswith("#"):
            continue

        # Resolve the href to an absolute URL.  Handles all of:
        #   relative file: "foo.tab"        → base_url + foo.tab
        #   relative dir:  "subdir/"        → base_url + subdir/
        #   parent-relative: "../other/"    → resolves up one level
        #   absolute: "https://other/..."   → unchanged
        resolved = urljoin(base_url, h)

        # Stay inside the original bundle.  Anything that resolves above or
        # sideways out of root_url is a parent-index link leading to other
        # bundles' content — those URLs won't work for us anyway.
        if not resolved.startswith(root_url):
            continue

        # Apache lists directories with a trailing slash; files without.
        if h.endswith("/"):
            sub_rel = f"{rel_prefix}{h}"
            yield (sub_rel, True)
            yield from _dawn_walk(resolved, sub_rel, depth + 1, root_url)
        else:
            yield (f"{rel_prefix}{h}", False)


def _dawn_records(base_url: str) -> List[Record]:
    """
    Walk the Dawn-Ceres GRaND archive recursively and build one Record per
    science-data file.  Pairs each .xml label with its matching .tab/.dat
    by basename so they're downloaded together.

    Layout-agnostic: works whether the archive uses year subdirs, phase
    subdirs (HAMO/LAMO/XMO*), collection subdirs, or a flat layout.
    """
    print(f"  walking {base_url}")

    files_by_stem = {}   # stem (lowercased) -> {"xml": path, "tab": path, "dat": path}
    n_dirs        = 0
    n_files       = 0
    n_excluded    = 0

    for rel, is_dir in _dawn_walk(base_url):
        if is_dir:
            n_dirs += 1
            continue
        n_files += 1

        ext = rel.rsplit(".", 1)[-1].lower() if "." in rel else ""
        if ext not in ("xml", "tab", "dat", "lbl"):
            continue

        # Use the basename (no path) and lower it for case-insensitive pairing.
        # Keep the original-cased relative path for the actual download URL.
        basename = rel.rsplit("/", 1)[-1]

        # Skip bundle-level manifests / SIP packaging files — they pass the
        # extension check but aren't science data, and the URLs we'd build
        # for them don't actually resolve.
        if _dawn_skip_filename(basename):
            n_excluded += 1
            continue

        stem = basename.rsplit(".", 1)[0].lower()

        slot = files_by_stem.setdefault(stem, {})
        # If we somehow see two files with the same stem+ext, keep the first
        if ext not in slot:
            slot[ext] = rel

    print(f"  found {n_dirs} directories, {n_files} files under data_calibrated/")
    if n_excluded:
        print(f"  excluded {n_excluded} bundle-manifest / SIP file(s)")
    print(f"  matched {len(files_by_stem)} unique stems with .xml/.tab/.dat/.lbl")

    records = []
    n_data_pairs = 0
    n_orphan_xml = 0

    for stem, slot in files_by_stem.items():
        # Prefer xml as the label, but fall back to lbl (some PDS3 leftovers)
        label = slot.get("xml") or slot.get("lbl")
        data  = slot.get("tab") or slot.get("dat")

        # We require at least one data file (tab/dat). Standalone labels
        # without data are usually documentation pointers — skip them.
        if data is None:
            if label is not None:
                n_orphan_xml += 1
            continue

        files = []
        if label is not None:
            files.append(label)
        files.append(data)

        d0, d1 = _dawn_extract_dates(stem)
        records.append(Record(date_start=d0, date_end=d1, files=files))
        n_data_pairs += 1

    if n_orphan_xml:
        print(f"  skipped {n_orphan_xml} label(s) with no matching data file")
    print(f"  built {n_data_pairs} downloadable records")

    return records


# ============================================================
# MARS CURIOSITY DAN — INDEX-BASED RECORD LISTING
# ============================================================
# DAN data is published with a fixed-width INDEX.TAB at the volume root that
# lists every product in the archive along with its sol, start/stop time, and
# product type.  Pulling that one file is dramatically faster and more robust
# than crawling thousands of per-Sol HTTP directory listings — and it gives
# us real per-file timestamps for date-range filtering.
#
# Source for column layout: PDS MSL DAN RDR Archive Volume SIS, Section 4.
# We only care about a handful of columns; the rest are ignored.

PDS_VOLUME_ROOT_MSL = ("https://pds-geosciences.wustl.edu/msl/"
                       "msl-m-dan-3_4-rdr-v1/msldan_1xxx")
PDS_INDEX_URL_MSL   = f"{PDS_VOLUME_ROOT_MSL}/index/index.tab"

# (start_byte_1based, n_bytes) — fixed-width offsets per the SIS
PDS_INDEX_COLS_MSL = {
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

# DAN produces five RDR product types.  Filter to the active-mode counts
# unless told otherwise — that's what the Mars viewer reads.
MSL_DEFAULT_PRODUCT_TYPES = {"DAN_RDR_AC"}

# Cache the index for a day.  The file is ~1.5 MB and rarely changes.
_MSL_INDEX_CACHE_TTL_SEC = 24 * 3600


def _msl_index_cache_path() -> Path:
    cache_dir = Path.home() / ".cache" / "nasa_fetcher"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "mars_rdr_index.tab"


def _msl_download_index(force_refresh: bool = False) -> List[str]:
    """
    Fetch and cache the Mars DAN PDS index.  Returns a list of lines.
    Re-uses the cached copy when it's less than a day old.
    """
    cache = _msl_index_cache_path()

    if not force_refresh and cache.exists():
        age = _time_now() - cache.stat().st_mtime
        if age < _MSL_INDEX_CACHE_TTL_SEC:
            with open(cache, "r", encoding="utf-8", errors="replace") as f:
                return f.read().splitlines()

    print(f"  Downloading Mars DAN index from {PDS_INDEX_URL_MSL}…")
    # Stream straight to the cache file using the same streamer the rest of
    # the fetcher uses, so we get the short connect timeout, chunked reads,
    # and error handling for free.
    try:
        _write_stream(PDS_INDEX_URL_MSL, cache, chunk_size=65536)
    except Exception as e:
        # Fall back to a stale cache if one exists — better than no data
        if cache.exists():
            print(f"  Index download failed ({e}); using stale cached copy.")
            with open(cache, "r", encoding="utf-8", errors="replace") as f:
                return f.read().splitlines()
        raise

    with open(cache, "r", encoding="utf-8", errors="replace") as f:
        lines = f.read().splitlines()
    print(f"  Cached {len(lines):,} index entries")
    return lines


def _time_now() -> float:
    # Indirect import so the file's top-of-module section stays clean
    import time as _t
    return _t.time()


def _msl_parse_index_line(line: str):
    """Parse one fixed-width INDEX.TAB row into a dict.

    Returns None if any required field is malformed (so callers can `if x is
    None: continue` instead of try/except'ing every field individually).
    """
    if len(line) < max(start + n - 1 for start, n in PDS_INDEX_COLS_MSL.values()):
        return None

    out = {}
    for name, (start_byte, n_bytes) in PDS_INDEX_COLS_MSL.items():
        s = start_byte - 1
        out[name] = line[s : s + n_bytes].strip().strip('"')

    # Parse the timestamp fields — try microsecond then second precision
    for key in ("START_TIME", "STOP_TIME"):
        raw = out.get(key)
        if raw:
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
                try:
                    out[key] = datetime.strptime(raw, fmt)
                    break
                except ValueError:
                    continue
            else:
                out[key] = None
        else:
            out[key] = None

    try:
        out["PLANET_DAY_NUMBER"] = int(out["PLANET_DAY_NUMBER"])
    except (ValueError, KeyError):
        out["PLANET_DAY_NUMBER"] = None

    return out


def _msl_records(base_url: str) -> List[Record]:
    """
    Build records for Mars Curiosity DAN by reading PDS INDEX.TAB.

    Each record is one (.lbl + .dat) pair so the existing download loop
    treats them as a unit.  URLs are absolute, so urljoin in the main
    loop will pass them through unchanged.

    `base_url` is ignored — the index URL is hardcoded since the index
    lives at a fixed location in the volume.
    """
    # Pull and cache the index
    try:
        lines = _msl_download_index()
    except Exception as e:
        print(f"\n[ERROR] Could not fetch the DAN index file:")
        print(f"  {PDS_INDEX_URL_MSL}")
        print(f"\nServer said: {e}")
        return []

    if not lines:
        print("  Index file was empty — archive layout may have changed.")
        return []

    # Filter to active-mode products (or whichever types we want).  Each
    # surviving row gets one Record holding the .lbl URL and the .dat URL.
    records       = []
    n_skip_type   = 0
    n_skip_parse  = 0
    n_skip_notime = 0
    types_seen    = {}
    for line in lines:
        if not line.strip():
            continue
        row = _msl_parse_index_line(line)
        if row is None:
            n_skip_parse += 1
            continue

        ptype = row.get("PRODUCT_TYPE", "")
        types_seen[ptype] = types_seen.get(ptype, 0) + 1

        if ptype not in MSL_DEFAULT_PRODUCT_TYPES:
            n_skip_type += 1
            continue

        start_dt = row.get("START_TIME")
        if start_dt is None:
            n_skip_notime += 1
            continue
        stop_dt = row.get("STOP_TIME") or start_dt

        path = row["PATH_NAME"].lstrip("/")
        lbl_name = row["FILE_NAME"]
        # DAN-RDR-AC data files share the basename with the label; this is
        # called out as an assumption in the SIS but holds for every record
        # in the current archive.  If PDS ever changes the convention, the
        # label parser would need to read POINTERS from each .lbl.
        dat_name = re.sub(r"\.LBL$", ".DAT", lbl_name, flags=re.IGNORECASE)

        lbl_url = f"{PDS_VOLUME_ROOT_MSL}/{path}/{lbl_name}"
        dat_url = f"{PDS_VOLUME_ROOT_MSL}/{path}/{dat_name}"

        records.append(Record(
            date_start = start_dt.date(),
            date_end   = stop_dt.date(),
            files      = [lbl_url, dat_url],
        ))

    print(f"  Index parsed: {len(records):,} records of type "
          f"{sorted(MSL_DEFAULT_PRODUCT_TYPES)} kept, "
          f"{n_skip_type:,} other-type skipped, "
          f"{n_skip_parse:,} parse errors, "
          f"{n_skip_notime:,} missing timestamps")
    if types_seen:
        # Helpful diagnostic — shows what's actually in the archive in case
        # someone wants to broaden the filter
        breakdown = ", ".join(f"{t or '?'}={n:,}" for t, n in
                              sorted(types_seen.items()))
        print(f"  Product-type breakdown: {breakdown}")

    return records


# ============================================================
# MISSION DEFINITIONS
# ============================================================

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
        base_url="https://pds-geosciences.wustl.edu/msl/msl-m-dan-3_4-rdr-v1/msldan_1xxx/data/",
        list_records=_msl_records,
        default_start="2012-08-06",
        default_end="2025-12-31"
    ),

    "3": DataSpec(
        name="Ceres DAWN GRaND",
        target_folder="Ceres",
        base_url="https://sbnarchive.psi.edu/pds4/dawn/grand/dawn-grand-ceres_1.0/data_calibrated/",
        list_records=_dawn_records,
        default_start="2015-01-01",
        default_end="2018-12-31"
    ),
}


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def _parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        print(f"Warning: '{s}' is not valid. Using default.")
        return None



# ============================================================
# DASH WEB UI — matches the viewer's color palette and widget style
# ============================================================

import contextlib
import queue
import threading
import time
import webbrowser
from datetime import datetime as _dt

import dash
from dash import Dash, dcc, html, Input, Output, State, no_update, ctx


# ── Color palette pulled from the Dash viewer ─────────────────────────────
_PAL = dict(
    BG_DEEP   = "#0a0e1a",
    BG_PANEL  = "#0d1424",
    BG_INPUT  = "#1a2438",
    FG_TEXT   = "#e0e8ff",
    FG_DIM    = "#7a8aa8",
    FG_CYAN   = "#7ecfff",
    FG_YELLOW = "#ffd166",
    FG_GREEN  = "#88ff88",
    FG_PURPLE = "#c97bff",
    FG_RED    = "#ff7777",
    BORDER    = "#1e2a3a",
)

_LABEL_STYLE = {"fontSize": "0.7rem", "opacity": 0.6,
                "letterSpacing": "0.15em", "color": _PAL["FG_DIM"]}
_INPUT_STYLE = {"background": _PAL["BG_INPUT"], "color": _PAL["FG_TEXT"],
                "border": f"1px solid {_PAL['BORDER']}",
                "padding": "6px 10px", "fontFamily": "'Courier New', monospace",
                "fontSize": "0.85rem", "borderRadius": "3px"}
_CARD_STYLE  = {"background": _PAL["BG_PANEL"],
                "border": f"1px solid {_PAL['BORDER']}",
                "borderRadius": "4px", "padding": "12px 14px",
                "marginBottom": "10px"}


# ── Job state — single active download at a time, module-level for simplicity
_JOB = {
    "worker":      None,    # threading.Thread or None
    "log_queue":   None,    # queue.Queue
    "cancel_flag": None,    # threading.Event
    "status":      "idle",  # "idle" | "running" | "done" | "cancelled" | "failed"
    "lines":       [],      # accumulated log lines (list of (text, color))
    "params_at_start": None,
}


class _SignalStream:
    """File-like that emits each completed line into a Queue."""

    def __init__(self, q: queue.Queue):
        self._q = q
        self._buf = ""

    def write(self, s):
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            self._q.put(line + "\n")

    def flush(self):
        if self._buf:
            self._q.put(self._buf)
            self._buf = ""


def _classify_line(text: str) -> str:
    """Pick a color for a log line based on its content."""
    low = text.lower()
    if "error" in low or "failed" in low or "[error]" in low:
        return _PAL["FG_RED"]
    if "downloading spice" in low or text.strip().startswith("---"):
        return _PAL["FG_PURPLE"]
    if "warning" in low or "skipping" in low or "cancel" in low:
        return _PAL["FG_YELLOW"]
    if "complete" in low or "summary" in low:
        return _PAL["FG_GREEN"]
    return _PAL["FG_TEXT"]


def _do_fetch(params: dict, cancel_flag: threading.Event):
    """The actual download workflow. Runs in the worker thread.
    All output goes through print() (which is redirected upstream)."""
    spec   = MISSIONS[params["mission_id"]]
    outdir = params["outdir"]
    dest_dir = Path(outdir) / spec.target_folder / "data"
    dest_dir.mkdir(parents=True, exist_ok=True)

    print(f"Output folder: {Path(outdir).resolve()}")
    print(f"Mission: {spec.name}")
    if params["filter"] == "2":
        print(f"Date range: {params['start']} → {params['end']}")
    else:
        print("Date range: all available data")
    print()

    print("Connecting to server…")
    try:
        records = spec.list_records(spec.base_url)
    except Exception as e:
        msg = str(e)
        print(f"\n[ERROR] Could not list the archive at:")
        print(f"  {spec.base_url}")
        print(f"\nServer said: {msg}")
        if "404" in msg or "Not Found" in msg:
            print("\nThis usually means PDS reorganized the archive and the")
            print("URL in the script is now stale. Check the current path at:")
            print("  https://pds-geosciences.wustl.edu/missions/")
            print("and update the matching DataSpec entry's `base_url` field.")
        elif "timed out" in msg.lower() or "timeout" in msg.lower():
            print("\nLooks like a network timeout — the PDS server may be slow")
            print("or temporarily unavailable.  Try again in a few minutes.")
        return
    print(f"  {len(records)} records discovered")

    if not records:
        print("\nNo files found at this path. The archive may be empty or")
        print("the directory layout may have changed since the script was written.")
        return

    filtered = records
    if params["filter"] == "2" and params["start"] and params["end"]:
        d0 = _parse_date(params["start"]) or _parse_date(spec.default_start)
        d1 = _parse_date(params["end"])   or _parse_date(spec.default_end)
        if d0 and d1:
            filtered = [r for r in records
                        if r.date_end >= d0 and r.date_start <= d1]
            print(f"  filtered to {len(filtered)} records by date range")

    print("\nStarting downloads…")
    n_dl = n_skip = n_fail = 0
    for rec in filtered:
        if cancel_flag.is_set():
            print("\n[cancelled by user]")
            return
        for remote_path in rec.files:
            if cancel_flag.is_set():
                print("\n[cancelled by user]", flush=True)
                return
            url = urljoin(spec.base_url, remote_path)
            local_filename = remote_path.split("/")[-1]
            dest = dest_dir / local_filename
            if dest.exists():
                n_skip += 1
                continue
            print(f"  Downloading {local_filename}", flush=True)
            try:
                _write_stream(url, dest, cancel_flag=cancel_flag)
                n_dl += 1
            except InterruptedError:
                # Mid-download cancel — _write_stream already removed the
                # partial file. Bail out of both loops.
                print("\n[cancelled by user]", flush=True)
                return
            except Exception as e:
                print(f"    error: {e}", flush=True)
                n_fail += 1

    print(f"\nScience data: {n_dl} downloaded, {n_skip} already present, "
          f"{n_fail} failed.", flush=True)

    if params["mission_id"] == "3":
        if cancel_flag.is_set():
            print("\n[cancelled before SPICE step]")
            return
        print(f"\nPreparing SPICE kernels for DAWN at Ceres "
              f"(mode={params['spice_mode']})…")
        ceres_dir = Path(outdir) / spec.target_folder
        kernels = download_dawn_ceres_spice(ceres_dir, mode=params["spice_mode"])
        create_meta_kernel(ceres_dir / "spice", kernels)
        if params["spice_mode"] == "none":
            print("\nSetup complete (no SPICE downloaded).")
            print("Re-run and pick Minimal or All to add SPICE later.")
        else:
            print(f"\nDAWN Ceres SPICE setup complete (mode={params['spice_mode']}).")


def _worker_main(params: dict, q: queue.Queue, cancel_flag: threading.Event):
    """Thread body. Bridges print() into the queue."""
    stream = _SignalStream(q)
    try:
        with contextlib.redirect_stdout(stream):
            _do_fetch(params, cancel_flag)
        stream.flush()
        if cancel_flag.is_set():
            q.put(("__finish__", "cancelled"))
        else:
            q.put(("__finish__", "done"))
    except Exception as e:
        import traceback
        stream.flush()
        q.put(f"\n[ERROR] {e}\n")
        q.put(traceback.format_exc() + "\n")
        q.put(("__finish__", "failed"))


# ──────────────────────────────────────────────────────────────────────────
# Build the Dash app
# ──────────────────────────────────────────────────────────────────────────

app = Dash(__name__, title="NASA PDS4 Data Fetcher")

# Mission options
_MISSION_OPTIONS = [
    {"label": f"  {mid}: {spec.name}", "value": mid}
    for mid, spec in MISSIONS.items()
]

# SPICE mode options. The label includes the row's accent color via inline
# HTML — Dash lets us pass HTML elements as labels.
_SPICE_OPTIONS = [
    {"label": html.Span(
        "  Minimal  ·  ~200 MB  ·  orbit tracks work, no CK pointing",
        style={"color": _PAL["FG_CYAN"]}),
     "value": "minimal"},
    {"label": html.Span(
        "  All  ·  multi-GB  ·  every kernel in 2015–2018 window",
        style={"color": _PAL["FG_PURPLE"]}),
     "value": "all"},
    {"label": html.Span(
        "  None  ·  skip SPICE  ·  viewer will show spectra only",
        style={"color": _PAL["FG_YELLOW"]}),
     "value": "none"},
]

_FILTER_OPTIONS = [
    {"label": "  All available data",   "value": "3"},
    {"label": "  Specific date range",  "value": "2"},
]


def _section(title: str, body):
    return html.Div([
        html.Div(title.upper(), style={**_LABEL_STYLE, "marginBottom": "6px"}),
        html.Div(body, style=_CARD_STYLE),
    ])


app.layout = html.Div(
    style={"background": _PAL["BG_DEEP"], "color": _PAL["FG_TEXT"],
           "minHeight": "100vh", "padding": "20px",
           "fontFamily": "'Courier New', monospace"},
    children=[

        # Title bar
        html.Div(
            style={"display": "flex", "alignItems": "center", "gap": "20px",
                   "marginBottom": "16px"},
            children=[
                html.H1("NASA · PDS4 DATA FETCHER",
                        style={"margin": 0, "letterSpacing": "0.12em",
                               "fontSize": "1.4rem", "color": _PAL["FG_CYAN"]}),
                html.Span("DOWNLOAD CONTROL",
                          style={"opacity": 0.45, "fontSize": "0.75rem",
                                 "letterSpacing": "0.2em",
                                 "color": _PAL["FG_DIM"]}),
            ]
        ),

        # Mission section
        _section("Mission", dcc.RadioItems(
            id="mission",
            options=_MISSION_OPTIONS,
            value="3",
            style={"fontSize": "0.85rem", "color": _PAL["FG_TEXT"]},
            inputStyle={"marginRight": "6px"},
            labelStyle={"display": "block", "padding": "4px 0",
                        "cursor": "pointer", "color": _PAL["FG_TEXT"]},
        )),

        # Date filter section
        _section("Date Filter", html.Div([
            dcc.RadioItems(
                id="filter-type",
                options=_FILTER_OPTIONS,
                value="3",
                style={"fontSize": "0.85rem", "display": "flex", "gap": "20px",
                       "color": _PAL["FG_TEXT"]},
                inputStyle={"marginRight": "6px"},
                labelStyle={"cursor": "pointer", "color": _PAL["FG_TEXT"]},
            ),
            html.Div(
                style={"display": "flex", "gap": "16px", "marginTop": "10px",
                       "alignItems": "center"},
                children=[
                    html.Span("Start (YYYY-MM-DD):",
                              style={**_LABEL_STYLE, "minWidth": "140px"}),
                    dcc.Input(id="start-date", type="text", value="",
                              style={**_INPUT_STYLE, "width": "140px"}),
                    html.Span("End (YYYY-MM-DD):",
                              style={**_LABEL_STYLE, "minWidth": "140px",
                                     "marginLeft": "16px"}),
                    dcc.Input(id="end-date", type="text", value="",
                              style={**_INPUT_STYLE, "width": "140px"}),
                ],
            ),
        ])),

        # SPICE section
        _section("SPICE Kernels (Ceres only)", dcc.RadioItems(
            id="spice-mode",
            options=_SPICE_OPTIONS,
            value="minimal",
            style={"fontSize": "0.85rem"},
            inputStyle={"marginRight": "6px"},
            labelStyle={"display": "block", "padding": "4px 0",
                        "cursor": "pointer"},
        )),

        # Output folder
        _section("Output Folder", dcc.Input(
            id="outdir", type="text", value="NASA_Data",
            style={**_INPUT_STYLE, "width": "100%", "boxSizing": "border-box"},
        )),

        # Action row
        html.Div(
            style={"display": "flex", "gap": "10px", "alignItems": "center",
                   "marginBottom": "12px"},
            children=[
                html.Button("▶  Start Download", id="start-btn", n_clicks=0,
                            style={"background": "#1e3a5f",
                                   "color": _PAL["FG_CYAN"],
                                   "border": f"1px solid {_PAL['FG_CYAN']}",
                                   "padding": "8px 22px", "cursor": "pointer",
                                   "fontFamily": "'Courier New', monospace",
                                   "fontSize": "0.85rem", "fontWeight": "bold",
                                   "letterSpacing": "0.1em",
                                   "borderRadius": "3px"}),
                html.Button("■  Cancel", id="cancel-btn", n_clicks=0,
                            style={"background": "#3a1e1e",
                                   "color": "#ff9999",
                                   "border": "1px solid #ff9999",
                                   "padding": "8px 22px", "cursor": "pointer",
                                   "fontFamily": "'Courier New', monospace",
                                   "fontSize": "0.85rem",
                                   "letterSpacing": "0.1em",
                                   "borderRadius": "3px"}),
                html.Div(style={"flex": 1}),  # spacer
                html.Span(id="status", children="Idle",
                          style={"color": _PAL["FG_DIM"],
                                 "fontSize": "0.85rem"}),
            ],
        ),

        # Activity log
        html.Div(
            style={**_LABEL_STYLE, "marginBottom": "6px"},
            children="ACTIVITY LOG",
        ),
        html.Div(
            id="log-box",
            style={"background": _PAL["BG_DEEP"],
                   "border": f"1px solid {_PAL['BORDER']}",
                   "borderRadius": "3px",
                   "padding": "10px", "height": "350px",
                   "overflowY": "auto", "fontFamily": "'Courier New', monospace",
                   "fontSize": "0.78rem", "whiteSpace": "pre-wrap"},
            children=[],
        ),

        # Polling timer + memory of accumulated lines
        dcc.Interval(id="log-poll", interval=500, n_intervals=0,
                     disabled=True),
    ]
)


# ──────────────────────────────────────────────────────────────────────────
# Callbacks
# ──────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("start-date", "disabled"),
    Output("end-date",   "disabled"),
    Input("filter-type", "value"),
)
def _toggle_date_inputs(filter_type):
    disabled = filter_type != "2"
    return disabled, disabled


@app.callback(
    Output("start-date",  "value"),
    Output("end-date",    "value"),
    Output("start-date",  "placeholder"),
    Output("end-date",    "placeholder"),
    Input("mission", "value"),
)
def _sync_date_range_to_mission(mission):
    """When the mission changes, replace the date-range values with that
    mission's full data window so the user immediately sees what's valid."""
    spec  = MISSIONS.get(mission or "3", MISSIONS["3"])
    start = spec.default_start or ""
    end   = spec.default_end   or ""
    return start, end, start, end


@app.callback(
    Output("spice-mode", "options"),
    Input("mission", "value"),
)
def _gate_spice_options(mission):
    """Visually disable SPICE options for non-Ceres missions by greying them."""
    if mission == "3":
        return _SPICE_OPTIONS
    return [
        {"label": html.Span(o["label"].children,
                            style={"color": _PAL["FG_DIM"], "opacity": 0.5}),
         "value": o["value"], "disabled": True}
        for o in _SPICE_OPTIONS
    ]


@app.callback(
    Output("log-poll", "disabled", allow_duplicate=True),
    Output("status",   "children", allow_duplicate=True),
    Output("start-btn", "disabled"),
    Output("log-box",  "children", allow_duplicate=True),
    Input("start-btn", "n_clicks"),
    State("mission", "value"),
    State("filter-type", "value"),
    State("start-date", "value"),
    State("end-date", "value"),
    State("outdir", "value"),
    State("spice-mode", "value"),
    prevent_initial_call=True,
)
def _on_start(n_clicks, mission, filter_type, start_date, end_date,
              outdir, spice_mode):
    if not n_clicks:
        return no_update, no_update, no_update, no_update

    # Don't start a second job if one is running
    if _JOB["worker"] and _JOB["worker"].is_alive():
        return no_update, no_update, True, no_update

    # Snapshot the params
    params = {
        "mission_id": mission or "3",
        "filter":     filter_type or "3",
        "start":      (start_date or "").strip() or None,
        "end":        (end_date   or "").strip() or None,
        "outdir":     (outdir     or "NASA_Data").strip(),
        "spice_mode": spice_mode or "minimal",
    }

    # Reset job state
    _JOB["log_queue"]   = queue.Queue()
    _JOB["cancel_flag"] = threading.Event()
    _JOB["status"]      = "running"
    _JOB["lines"]       = []
    _JOB["params_at_start"] = params

    # Header lines for the log
    spec = MISSIONS[params["mission_id"]]
    sep = "─" * 60
    _JOB["lines"].append((sep + "\n",                 _PAL["FG_CYAN"]))
    _JOB["lines"].append((f"Starting fetch: {spec.name}\n", _PAL["FG_CYAN"]))
    _JOB["lines"].append((sep + "\n\n",               _PAL["FG_CYAN"]))

    # Spawn worker
    worker = threading.Thread(
        target=_worker_main,
        args=(params, _JOB["log_queue"], _JOB["cancel_flag"]),
        daemon=True,
    )
    _JOB["worker"] = worker
    worker.start()

    return False, "Running…", True, _render_log_lines()


@app.callback(
    Output("status", "children", allow_duplicate=True),
    Input("cancel-btn", "n_clicks"),
    prevent_initial_call=True,
)
def _on_cancel(n_clicks):
    if not n_clicks:
        return no_update
    if _JOB["worker"] and _JOB["worker"].is_alive():
        _JOB["cancel_flag"].set()
        _JOB["lines"].append(
            ("\n[cancel requested — finishing current file then stopping]\n",
             _PAL["FG_YELLOW"])
        )
        return "Cancel requested…"
    return no_update


def _render_log_lines():
    """Convert _JOB['lines'] into a list of styled html.Span's."""
    return [html.Span(text, style={"color": color})
            for text, color in _JOB["lines"]]


@app.callback(
    Output("log-box",  "children"),
    Output("status",   "children"),
    Output("log-poll", "disabled"),
    Output("start-btn", "disabled", allow_duplicate=True),
    Input("log-poll", "n_intervals"),
    prevent_initial_call=True,
)
def _drain_log_queue(n):
    """Pull every available line from the queue and append to the log."""
    if _JOB["log_queue"] is None:
        return no_update, no_update, True, no_update

    job_finished_status = None
    drained_any = False
    try:
        while True:
            item = _JOB["log_queue"].get_nowait()
            drained_any = True
            if isinstance(item, tuple) and item[0] == "__finish__":
                # status code: "done" / "cancelled" / "failed"
                job_finished_status = item[1]
                break
            color = _classify_line(item if isinstance(item, str) else "")
            _JOB["lines"].append((item, color))
    except queue.Empty:
        pass

    if job_finished_status is not None:
        _JOB["status"] = job_finished_status
        status_label = {"done": "Done", "cancelled": "Cancelled",
                        "failed": "Failed"}[job_finished_status]
        return _render_log_lines(), status_label, True, False

    if drained_any:
        return _render_log_lines(), no_update, no_update, no_update
    return no_update, no_update, no_update, no_update


# ──────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────

def main():
    PORT = 8051   # 8050 is reserved for the viewer
    print(f"Starting NASA PDS4 Data Fetcher GUI on http://127.0.0.1:{PORT}")
    print("Press Ctrl+C in this terminal to stop the server.")
    threading.Timer(
        1.5, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}")
    ).start()
    # debug=False so a stray reload won't kill an in-flight download
    app.run(debug=False, port=PORT, host="127.0.0.1")


if __name__ == "__main__":
    main()