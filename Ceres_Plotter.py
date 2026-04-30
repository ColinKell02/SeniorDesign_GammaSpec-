import numpy as np
import pandas as pd
from pathlib import Path
import spiceypy as spice
from scipy.ndimage import gaussian_filter
from scipy.signal import savgol_filter
from scipy.spatial import KDTree


import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from PIL import Image
import plotly.graph_objects as go
import functools
import json
import io
import zipfile
import base64
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os as _os
import webbrowser
import threading

import dash
from dash import Dash, dcc, html, Input, Output, State
import dash_vtk

# ------------------------------------------------
# CONFIG
# ------------------------------------------------

INDEX_FILE   = "spatial_library_ceres.csv"
DATA_DIR     = Path("Ceres/data")
SPICE_DIR    = Path("NASA_Data/Ceres/spice")
TEXTURE_FILE = "Ceres_Dawn_FC_DLR_global_20ppd_Oct2015.tif"

R_CERES  = 473
LAT_BINS = 90
LON_BINS = 180

# Minimum valid row signal & per-cell hit count
MIN_ROW_COUNTS = 500
MIN_CELL_HITS  = 3

# ------------------------------------------------
# ORBITAL PHASES (Dawn at Ceres mission phases)
# ------------------------------------------------

ORBITAL_PHASES = {
    "all":    {"label": "All Phases",       "start": "2015-04-23", "end": "2018-10-31"},
    "rc3":    {"label": "RC3 (Survey)",     "start": "2015-04-23", "end": "2015-05-09"},
    "survey": {"label": "Survey Orbit",     "start": "2015-06-06", "end": "2015-06-30"},
    "hamo":   {"label": "HAMO",             "start": "2015-08-17", "end": "2015-10-23"},
    "lamo":   {"label": "LAMO",             "start": "2015-12-16", "end": "2016-09-02"},
    "ext1":   {"label": "Extended Mission", "start": "2016-10-01", "end": "2017-11-01"},
    "ext2":   {"label": "GEO Orbit",        "start": "2017-11-01", "end": "2018-10-31"},
}

# ------------------------------------------------
# ORBITAL PHASE KERNEL MAPPING
# ------------------------------------------------

PHASE_KERNELS = {
    "all": [
        "dawn_rec_141230-150509_150603_v1.bsp",
        "dawn_rec_150509-150630_150730_v1.bsp",
        "dawn_rec_150630-151023_160804_v1.bsp",
        "dawn_rec_151023-160202_160412_v1.bsp",
        "dawn_rec_160202-160319_160531_v1.bsp",
        "dawn_rec_160319-160410_160809_v1.bsp",
        "dawn_rec_160410-160505_160809_v1.bsp",
        "dawn_rec_160505-160527_160810_v1.bsp",
        "dawn_rec_160527-160617_160809_v1.bsp",
        "dawn_rec_160617-160902_161109_v1.bsp",
        "dawn_rec_160902-161104_170124_v1.bsp",
        "dawn_rec_161104-170222_170713_v1.bsp",
        "dawn_rec_170222-170603_170714_v1.bsp",
        "dawn_rec_170603-180416_180703_v1.bsp",
        "dawn_rec_180416-180609_180703_v1.bsp",
        "dawn_rec_180609-180805_180828_v1.bsp",
        "dawn_rec_180805-181031_181129_v1.bsp",
    ],
    "rc3": [
        "dawn_rec_141230-150509_150603_v1.bsp",
    ],
    "survey": [
        "dawn_rec_150509-150630_150730_v1.bsp",
    ],
    "hamo": [
        "dawn_rec_150630-151023_160804_v1.bsp",
        "dawn_rec_151023-160202_160412_v1.bsp",
    ],
    "lamo": [
        "dawn_rec_151023-160202_160412_v1.bsp",
        "dawn_rec_160202-160319_160531_v1.bsp",
        "dawn_rec_160319-160410_160809_v1.bsp",
        "dawn_rec_160410-160505_160809_v1.bsp",
        "dawn_rec_160505-160527_160810_v1.bsp",
        "dawn_rec_160527-160617_160809_v1.bsp",
        "dawn_rec_160617-160902_161109_v1.bsp",
    ],
    "ext1": [
        "dawn_rec_160902-161104_170124_v1.bsp",
        "dawn_rec_161104-170222_170713_v1.bsp",
        "dawn_rec_170222-170603_170714_v1.bsp",
        "dawn_rec_170603-180416_180703_v1.bsp",
    ],
    "ext2": [
        "dawn_rec_170603-180416_180703_v1.bsp",
        "dawn_rec_180416-180609_180703_v1.bsp",
        "dawn_rec_180609-180805_180828_v1.bsp",
        "dawn_rec_180805-181031_181129_v1.bsp",
    ],
}

# ------------------------------------------------
# LOAD DATA
# ------------------------------------------------

print("Loading spatial library")
df = pd.read_csv(INDEX_FILE)
df["lon_norm"] = (df["lon"] + 180) % 360 - 180

# ------------------------------------------------
# SPICE SETUP
# ------------------------------------------------

# ------------------------------------------------
# SPICE SETUP  (auto-detect; falls back gracefully if kernels missing)
# ------------------------------------------------

print("Loading SPICE kernels from", SPICE_DIR)

# SPICE_AVAILABLE is the master switch read by every site that would otherwise
# call spice.spkezr / utc2et / et2utc. When False, those sites skip cleanly
# and fall back to using the CSV's pre-computed lat/lon for whatever they can.
SPICE_AVAILABLE = False
_loaded_kernels = []

if not SPICE_DIR.exists():
    print(f"  SPICE directory '{SPICE_DIR}' not found — running without SPICE.")
    print("  Orbit tracks and TAB-derived lat/lon are disabled.")
    print("  Spectrum viewing will still work using the CSV (lat/lon already there).")
else:
    _orig_dir = _os.getcwd()

    def _furnsh_safe(kfile):
        try:
            spice.furnsh(str(Path(kfile).resolve()))
            return True
        except Exception as e:
            msg = str(e)
            if "NOSUCHFILE" not in msg and "DAFNOSUCHADDR" not in msg:
                print(f"  Skipping {Path(kfile).name}: {msg[:100]}")
            return False

    _meta_kernels = sorted(SPICE_DIR.glob("*.tm")) + sorted(SPICE_DIR.glob("*.mk"))
    _meta_loaded  = False
    for mk in _meta_kernels:
        try:
            _os.chdir(str(mk.parent.resolve()))
            spice.furnsh(mk.name)
            _os.chdir(_orig_dir)
            _loaded_kernels.append(mk)
            _meta_loaded = True
            print(f"  Meta-kernel loaded: {mk.name}")
            break
        except Exception as e:
            _os.chdir(_orig_dir)
            print(f"  Meta-kernel failed ({mk.name}): {str(e)[:100]}")

    if not _meta_loaded:
        print("  No meta-kernel — looking for minimal support kernels…")
        _MINIMAL = [
            next(iter(sorted(SPICE_DIR.glob("naif*.tls")) +
                      sorted(SPICE_DIR.glob("*.tls"))), None),
            next(iter(sorted(SPICE_DIR.glob("pck*.tpc")) +
                      sorted(SPICE_DIR.glob("*.tpc"))), None),
            next(iter(sorted(SPICE_DIR.glob("dawn_ceres*.tf")) +
                      sorted(SPICE_DIR.glob("*.tf"))), None),
            next(iter(sorted(SPICE_DIR.glob("DAWN_203*.tsc")) +
                      sorted(SPICE_DIR.glob("*.tsc"))), None),
            next(iter(sorted(SPICE_DIR.glob("sb_ceres*.bsp")) +
                      sorted(SPICE_DIR.glob("*ceres*.bsp"))), None),
            next(iter(sorted(SPICE_DIR.glob("de4*.bsp")) +
                      sorted(SPICE_DIR.glob("*.bsp"))), None),
        ]
        for kf in _MINIMAL:
            if kf and _furnsh_safe(kf):
                _loaded_kernels.append(kf)

    # Real SPICE only counts as available if at least the LSK + a body SPK
    # got in — otherwise utc2et / spkezr will throw and we should fall back.
    if _loaded_kernels:
        try:
            spice.utc2et("2016-01-01T00:00:00")  # smoke-test
            SPICE_AVAILABLE = True
        except Exception as e:
            print(f"  SPICE smoke-test failed ({e}) — running without SPICE.")
            SPICE_AVAILABLE = False
    else:
        print("  No kernels loaded — running without SPICE.")
        print("  Orbit tracks and TAB-derived lat/lon are disabled.")
        print("  Spectrum viewing will still work using the CSV (lat/lon already there).")

print(f"  SPICE_AVAILABLE = {SPICE_AVAILABLE}  "
      f"({len(_loaded_kernels)} support-kernel file(s) loaded)")

# ------------------------------------------------
# HELPER FUNCTIONS
# ------------------------------------------------

def spherical(lat, lon, r=R_CERES):
    lat = np.radians(lat)
    lon = np.radians(lon)
    x = r * np.cos(lat) * np.cos(lon)
    y = r * np.cos(lat) * np.sin(lon)
    z = r * np.sin(lat)
    return x, y, z


def latlon(x, y, z, r=R_CERES):
    val = np.clip(z / r, -1, 1)
    lat = np.degrees(np.arcsin(val))
    lon = np.degrees(np.arctan2(y, x))
    return lat, lon


def channel_to_energy(ch, n_channels=None):
    ch = np.asarray(ch)
    if n_channels is not None and n_channels <= 32:
        return None
    if n_channels is not None and n_channels == 1024:
        return 0.0 + 0.0089 * ch        # BGO: 8.9 keV/channel
    return 0.0 + 0.02 * ch              # GRS 512-ch default: 20 keV/channel


def is_grs_file(filepath: str) -> bool:
    stem = Path(filepath).stem.upper()
    GRS_CODES  = {"GRS", "GRSUM", "GR", "BGOC", "BGO"}
    SKIP_CODES = {"EPG", "NS", "NSS", "NSD", "HN", "LP"}
    parts = stem.split("-")
    code  = parts[-1] if parts else ""
    if code in SKIP_CODES:
        return False
    if code in GRS_CODES:
        return True
    return True


# ================================================
# NOISE & BACKGROUND FILTERING
# ================================================

def flag_cosmic_ray_rows(counts_matrix, sigma=4.0):
    row_totals = counts_matrix.sum(axis=1)
    med = np.median(row_totals)
    mad = np.median(np.abs(row_totals - med)) + 1e-9
    z   = (row_totals - med) / (1.4826 * mad)
    return z < sigma


def sigma_clip_spectrum(C, sigma=3.5, window=15):
    C_clean = C.copy().astype(float)
    cr_mask = np.zeros(len(C), dtype=bool)
    for i in range(len(C)):
        lo = max(0, i - window)
        hi = min(len(C), i + window)
        neighbourhood = np.concatenate([C[lo:i], C[i + 1:hi]])
        if len(neighbourhood) < 3:
            continue
        local_med = np.median(neighbourhood)
        local_mad = np.median(np.abs(neighbourhood - local_med)) + 1e-9
        z = (C[i] - local_med) / (1.4826 * local_mad)
        if z > sigma:
            C_clean[i] = local_med
            cr_mask[i] = True
    return C_clean, cr_mask


def snip_background(C, iterations=14):
    C_safe = np.clip(C, 1, None)
    v = np.log(np.log(np.sqrt(C_safe) + 1) + 1)
    n = len(v)
    for width in range(1, iterations + 1):
        v_new = v.copy()
        for i in range(width, n - width):
            avg = (v[i - width] + v[i + width]) / 2.0
            v_new[i] = min(v[i], avg)
        v = v_new
    background = (np.exp(np.exp(v - 1) - 1) - 1) ** 2 - 1
    return np.clip(background, 0, None)


def choose_snip_iter(C):
    total = np.sum(C)
    if total < 5e3:
        return 10
    elif total < 5e4:
        return 14
    else:
        return 18


def subtract_continuum(C, iterations=24):
    bg    = snip_background(C, iterations)
    C_net = np.clip(C - bg, 0, None)
    return C_net, bg


def apply_spectral_filters(C, apply_cr=True, apply_bg=True,
                           cr_sigma=3.5, cr_window=15, snip_iter=24):
    raw     = np.asarray(C, dtype=float)
    cr_mask = np.zeros(len(raw), dtype=bool)
    if apply_cr:
        clipped, cr_mask = sigma_clip_spectrum(raw, sigma=cr_sigma, window=cr_window)
    else:
        clipped = raw.copy()
    if apply_bg:
        snip_iter = choose_snip_iter(clipped)
        net, bg = subtract_continuum(clipped, iterations=snip_iter)
    else:
        net = clipped.copy()
        bg  = np.zeros_like(clipped)
    return {
        "raw":        raw,
        "cr_clipped": clipped,
        "background": bg,
        "net":        net,
        "cr_mask":    cr_mask,
        "n_spikes":   int(cr_mask.sum()),
    }


# ------------------------------------------------
# PEAK INTEGRATION  (box sum on net spectrum)
# ------------------------------------------------

def integrate_peak(E, C, e1, e2, apply_cr=True, apply_bg=True):
    """
    Sum net counts in [e1, e2] (MeV) after applying the requested filters.
    Returns 0 if the energy axis is unavailable (e.g. wrong-detector files).
    """
    if E is None:
        return 0.0
    result = apply_spectral_filters(C, apply_cr=apply_cr, apply_bg=apply_bg)
    C_net  = result["net"]
    mask   = (E >= e1) & (E <= e2)
    if np.sum(mask) < 3:
        return 0.0
    return float(np.sum(np.clip(C_net[mask], 0, None)))


# ------------------------------------------------
# LOAD SPECTRUM (cached)
# ------------------------------------------------

@functools.lru_cache(maxsize=2048)
def load_spectrum(filepath: str):
    path = Path(filepath)
    if not path.exists():
        return None

    ext = path.suffix.lower()
    if ext in (".xml", ".lbl", ".fmt", ".cat"):
        return None

    try:
        if ext == ".tab":
            raw = pd.read_csv(
                path, sep=r"\s+", header=None,
                engine="python", on_bad_lines="skip",
            )

            et_col     = 2
            spec_start = 3

            ets         = pd.to_numeric(raw.iloc[:, et_col], errors="coerce").values
            utcs        = raw.iloc[:, 1].astype(str).tolist()
            count_block = raw.iloc[:, spec_start:].apply(
                pd.to_numeric, errors="coerce"
            ).values.astype(float)

            good        = ~np.isnan(ets)
            ets         = ets[good]
            utcs        = [utcs[i] for i in range(len(utcs)) if good[i]]
            count_block = np.nan_to_num(count_block[good])

            if count_block.shape[0] == 0:
                return None

            with np.errstate(invalid="ignore"):
                keep_cols   = np.any(np.nan_to_num(count_block) != 0, axis=0)
            count_block = count_block[:, keep_cols]

            if count_block.shape[1] == 0:
                return None

            n_ch = count_block.shape[1]

            if n_ch <= 32:
                print(f"  Skipping {path.name}: only {n_ch} channels "
                      f"(EPG/NS detector — wrong energy scale)")
                return None

            lats, lons = [], []
            if SPICE_AVAILABLE:
                for et in ets:
                    try:
                        state, _ = spice.spkezr(
                            "DAWN", float(et), "IAU_CERES", "NONE", "CERES"
                        )
                        la, lo = latlon(*state[:3])
                    except Exception:
                        la, lo = 0.0, 0.0
                    lats.append(la)
                    lons.append(lo)
            else:
                # Without SPICE we can't derive per-row lat/lon from ET. Set to
                # zeros — TAB indexing/heatmap will be skipped at the module level.
                lats = [0.0] * len(ets)
                lons = [0.0] * len(ets)
            lats = np.array(lats)
            lons = np.array(lons)

            good_rows = flag_cosmic_ray_rows(count_block, sigma=4.0)
            n_flagged = int((~good_rows).sum())
            if n_flagged:
                print(f"  CR row filter: dropped {n_flagged}/{len(good_rows)} "
                      f"rows in {path.name}")

            count_block = count_block[good_rows]
            lats        = lats[good_rows]
            lons        = lons[good_rows]
            ets         = ets[good_rows]
            utcs        = [utcs[i] for i, g in enumerate(good_rows) if g]

            if count_block.shape[0] == 0:
                return None

            return {
                "counts":     count_block,
                "lat":        lats,
                "lon":        lons,
                "utc":        utcs,
                "et":         ets,
                "n_channels": n_ch,
            }

        else:
            data = np.loadtxt(path)
            if data.ndim < 2:
                return None
            counts = data[:, 3:]
            counts = counts[:, np.any(counts != 0, axis=0)]
            if counts.shape[1] == 0:
                return None
            n = counts.shape[0]
            return {
                "counts":     counts,
                "lat":        np.zeros(n),
                "lon":        np.zeros(n),
                "utc":        [],
                "et":         np.zeros(n),
                "n_channels": counts.shape[1],
            }

    except Exception as e:
        print(f"  load_spectrum error ({path.name}): {e}")
        return None


# ------------------------------------------------
# WORKER: process one file for element grid
# ------------------------------------------------

# Energy windows (MeV) for box-sum integration on the net spectrum.
PEAK_WINDOWS = {
    "Fe": (7.40, 7.80),  # ⁵⁶Fe(n,γ) 7.6 MeV
    "K":  (1.40, 1.52),  # ⁴⁰K 1.461 MeV
    "Th": (2.52, 2.70),  # ²⁰⁸Tl 2.614 MeV
}


def _process_row(args):
    """
    Process a single spectrum file for the elemental map builder.
    Returns a list of (lat_i, lon_i, Fe, K, Th, H, w) tuples — one per
    surviving row. Element values are box-integrated net counts in their
    energy windows; H is the fast/epithermal neutron ratio.
    """
    filepath, lat_hint, lon_hint, altitude = args
    spec = load_spectrum(filepath)
    if spec is None:
        return None

    counts_matrix = spec["counts"]
    lats_arr      = spec["lat"]
    lons_arr      = spec["lon"]
    n_rows        = counts_matrix.shape[0]
    n_ch          = counts_matrix.shape[1]

    E = channel_to_energy(np.arange(n_ch), n_channels=n_ch)
    if E is None:
        return None

    low_ch_mask = E < 0.1   # MeV

    # H ratio windows
    mask_fast = (E >= 0.9) & (E <= 2.0)
    mask_epi  = (E >= 0.3) & (E < 0.9)

    alt = altitude if altitude > 0 else 375

    results = []
    for i in range(n_rows):
        C = counts_matrix[i].astype(float)

        # Zero out sub-100 keV electronic noise floor
        C_clean = C.copy()
        C_clean[low_ch_mask] = 0.0

        total_signal = float(C_clean.sum())
        if total_signal < MIN_ROW_COUNTS:
            continue

        lat      = float(lats_arr[i]) if n_rows > 1 else lat_hint
        lon_norm = ((float(lons_arr[i]) + 180) % 360 - 180) if n_rows > 1 else lon_hint

        # SNIP-subtract once, then box-sum each window
        filt  = apply_spectral_filters(C_clean, apply_cr=False, apply_bg=True)
        C_net = filt["net"]

        def _box_sum(lo, hi):
            m = (E >= lo) & (E <= hi)
            return float(np.sum(np.clip(C_net[m], 0, None))) if np.any(m) else 0.0

        Fe = _box_sum(*PEAK_WINDOWS["Fe"])
        K  = _box_sum(*PEAK_WINDOWS["K"])
        Th = _box_sum(*PEAK_WINDOWS["Th"])

        fast_counts = float(C_clean[mask_fast].sum())
        epi_counts  = float(C_clean[mask_epi].sum())
        H = fast_counts / (epi_counts + 1e-9)

        if Fe == 0 and K == 0 and Th == 0 and fast_counts < 10:
            continue

        lat_i = int(np.clip((lat      + 90)  / 180 * LAT_BINS, 0, LAT_BINS - 1))
        lon_i = int(np.clip((lon_norm + 180) / 360 * LON_BINS, 0, LON_BINS - 1))

        # Weight by signal quality and altitude
        w = total_signal / (alt ** 2)

        results.append((lat_i, lon_i, Fe, K, Th, H, w))

    return results if results else None


# ------------------------------------------------
# FILE RESOLVER
# ------------------------------------------------

def _resolve_filepath(stem_or_name: str):
    candidates = [stem_or_name]
    p   = Path(stem_or_name)
    ext = p.suffix.lower()
    if ext == ".dat":
        candidates += [p.with_suffix(".tab").name, p.with_suffix(".TAB").name]
    elif ext == ".tab":
        candidates += [p.with_suffix(".dat").name, p.with_suffix(".DAT").name]
    candidates += [c.upper() for c in candidates]

    for name in candidates:
        full = DATA_DIR / name
        if full.exists():
            return str(full)
        hits = list(DATA_DIR.glob(f"**/{name}"))
        if hits:
            return str(hits[0])
    return None


# ------------------------------------------------
# BUILD ELEMENT MAPS (parallel) — now with uncertainty maps
# ------------------------------------------------

def build_element_maps(sub_df, et_start=None, et_end=None):
    Fe_grid    = np.zeros((LAT_BINS, LON_BINS))
    K_grid     = np.zeros((LAT_BINS, LON_BINS))
    Th_grid    = np.zeros((LAT_BINS, LON_BINS))
    H_grid     = np.zeros((LAT_BINS, LON_BINS))
    weights    = np.zeros((LAT_BINS, LON_BINS))
    hit_counts = np.zeros((LAT_BINS, LON_BINS), dtype=int)

    args_list = []
    n_missing = 0
    for _, row in sub_df.iterrows():
        fp = _resolve_filepath(row.filename)
        if fp is None:
            n_missing += 1
            continue
        alt = row.get("altitude", 375) if hasattr(row, "get") else \
              (row["altitude"] if "altitude" in row.index else 375)
        args_list.append((fp, row.lat, row.lon_norm, alt))

    if n_missing:
        print(f"  build_element_maps: {n_missing} CSV entries had no matching file")

    if not args_list and TAB_RECORDS:
        print("  build_element_maps: CSV→file lookup found nothing — "
              "falling back to indexed .tab files")
        seen = {}
        for rec in TAB_RECORDS:
            if et_start is not None and et_end is not None:
                et = rec.get("et")
                if et is None or not (et_start <= et <= et_end):
                    continue
            fp = rec["filepath"]
            if fp not in seen:
                seen[fp] = (rec["lat"], rec["lon"])
        for fp, (lat, lon) in seen.items():
            lon_norm = (lon + 180) % 360 - 180
            args_list.append((fp, lat, lon_norm, 375))

    print(f"  build_element_maps: processing {len(args_list)} files "
          f"(box-sum integration)")

    def _accum(result):
        if result is None:
            return
        for (lat_i, lon_i, Fe, K, Th, H, w) in result:
            Fe_grid[lat_i, lon_i]    += Fe * w
            K_grid[lat_i, lon_i]     += K  * w
            Th_grid[lat_i, lon_i]    += Th * w
            H_grid[lat_i, lon_i]     += H  * w
            weights[lat_i, lon_i]    += w
            hit_counts[lat_i, lon_i] += 1

    with ThreadPoolExecutor() as ex:
        for fut in as_completed({ex.submit(_process_row, a): a for a in args_list}):
            _accum(fut.result())

    n_filled  = int((weights > 0).sum())
    n_trusted = int(((weights > 0) & (hit_counts >= MIN_CELL_HITS)).sum())
    print(f"  build_element_maps: {n_filled} grid cells filled, "
          f"{n_trusted} with >= {MIN_CELL_HITS} observations (trusted)")

    fill_mask   = weights > 0
    sparse_mask = fill_mask & (hit_counts < MIN_CELL_HITS)

    for g in (Fe_grid, K_grid, Th_grid, H_grid):
        g[fill_mask] /= weights[fill_mask]
        gaussian_filter(g, 1, output=g)
        g[sparse_mask] = 0.0

    return {
        "values":     (Fe_grid, K_grid, Th_grid, H_grid),
        "hit_counts": hit_counts,
        "n_trusted":  n_trusted,
        "n_filled":   n_filled,
    }


# ------------------------------------------------
# SPICE KERNEL COVERAGE HELPER
# ------------------------------------------------

def get_spk_coverage(target_id: int = -203):
    if not SPICE_AVAILABLE:
        return None
    count = spice.ktotal("SPK")
    cover = spice.stypes.SPICEDOUBLE_CELL(10000)
    spice.scard(0, cover)
    for i in range(count):
        try:
            fname, ftype, ksrc, handle = spice.kdata(i, "SPK", 256, 32, 256)
        except Exception:
            continue
        try:
            spice.spkcov(fname, target_id, cover)
        except Exception:
            pass
    n = spice.wncard(cover)
    if n == 0:
        return None
    et_min =  1e18
    et_max = -1e18
    for i in range(n):
        left, right = spice.wnfetd(cover, i)
        et_min = min(et_min, left)
        et_max = max(et_max, right)
    return et_min, et_max


def clamp_phase_times(phase_key: str, coverage):
    if not SPICE_AVAILABLE:
        return None
    phase = ORBITAL_PHASES[phase_key]
    t0 = spice.utc2et(phase["start"])
    t1 = spice.utc2et(phase["end"])
    if coverage is None:
        return t0, t1
    cov_start, cov_end = coverage
    t0 = max(t0, cov_start)
    t1 = min(t1, cov_end)
    if t0 >= t1:
        return None
    return t0, t1


# ------------------------------------------------
# PHASE KERNEL LOADER
# ------------------------------------------------

_LOADED_PHASE_KERNELS = []

# Kernel-loading mode. "minimal" = load only the rec SPKs needed for the
# selected phase (fast, clean tracks). "all" = load every rec SPK that covers
# any part of the mission (slower, but lets phase windows still be queried
# against the full trajectory; the orbit track will visually layer for the
# "all phases" view because that *is* the data spanning years).
KERNEL_MODE = "minimal"


def load_phase_kernels(phase_key: str):
    """
    Load the SPICE rec SPKs needed for the selected orbital phase.
    Always unloads anything previously loaded by this function first, so
    switching phases gives a clean slate.

    Behaviour depends on the module-level KERNEL_MODE:
        "minimal" — load only the rec SPKs listed for this phase
        "all"     — load every dawn_rec_*.bsp in SPICE_DIR

    No-op when SPICE is unavailable.
    """
    global _LOADED_PHASE_KERNELS, _SPK_COVERAGE

    if not SPICE_AVAILABLE:
        _SPK_COVERAGE = None
        return

    # Unload everything we loaded last time
    for kpath in _LOADED_PHASE_KERNELS:
        try:
            spice.unload(str(kpath))
        except Exception as e:
            print(f"  Could not unload {Path(kpath).name}: {e}")
    if _LOADED_PHASE_KERNELS:
        print(f"  Unloaded {len(_LOADED_PHASE_KERNELS)} prior phase kernel(s)")
    _LOADED_PHASE_KERNELS = []

    if KERNEL_MODE == "all":
        # Load every rec SPK on disk
        rec_paths = sorted(SPICE_DIR.glob("dawn_rec_*.bsp"))
        print(f"  KERNEL_MODE='all' — loading {len(rec_paths)} rec SPK(s)")
        for kpath in rec_paths:
            try:
                spice.furnsh(str(kpath))
                _LOADED_PHASE_KERNELS.append(str(kpath))
            except Exception as e:
                print(f"  Failed to load {kpath.name}: {str(e)[:100]}")
    else:
        # "minimal" — load only the rec SPKs the phase explicitly needs
        names = PHASE_KERNELS.get(phase_key, [])
        print(f"  KERNEL_MODE='minimal' — loading {len(names)} rec SPK(s) "
              f"for phase '{phase_key}'")
        for name in names:
            kpath = SPICE_DIR / name
            if not kpath.exists():
                matches = list(SPICE_DIR.glob(f"{name[:25]}*.bsp"))
                if matches:
                    kpath = matches[0]
                    print(f"  Fuzzy matched: {kpath.name}")
                else:
                    print(f"  WARNING: kernel not found: {name}")
                    continue
            try:
                spice.furnsh(str(kpath))
                _LOADED_PHASE_KERNELS.append(str(kpath))
            except Exception as e:
                print(f"  Failed to load {kpath.name}: {e}")

    # Refresh SPK coverage cache after the swap
    _SPK_COVERAGE = get_spk_coverage(-203)
    if _SPK_COVERAGE:
        try:
            utc0 = spice.et2utc(_SPK_COVERAGE[0], "ISOC", 0)[:10]
            utc1 = spice.et2utc(_SPK_COVERAGE[1], "ISOC", 0)[:10]
            print(f"  Coverage now: {utc0} → {utc1}  "
                  f"({len(_LOADED_PHASE_KERNELS)} rec SPK(s) active)")
        except Exception:
            print(f"  Coverage: ET {_SPK_COVERAGE[0]:.1f} → {_SPK_COVERAGE[1]:.1f}")
    else:
        print("  Warning: no SPK coverage found after kernel load")


# ------------------------------------------------
# ORBIT TRACK BUILDER
# ------------------------------------------------

print("Querying SPK coverage for DAWN (-203)…")
_SPK_COVERAGE = get_spk_coverage(-203)
if _SPK_COVERAGE:
    print(f"  Kernel covers ET {_SPK_COVERAGE[0]:.1f} → {_SPK_COVERAGE[1]:.1f}")
else:
    print("  Warning: could not determine SPK coverage.")


def build_orbit_track(phase_key: str):
    if not SPICE_AVAILABLE:
        return [], []
    times_range = clamp_phase_times(phase_key, _SPK_COVERAGE)
    if times_range is None:
        print(f"  Phase '{phase_key}' is outside loaded SPK coverage — skipping track.")
        return [], []

    t0, t1   = times_range
    duration = t1 - t0

    n_samples = max(2000, int(duration / 45))
    n_samples = min(n_samples, 15000)
    times     = np.linspace(t0, t1, n_samples)

    positions = []
    for et in times:
        try:
            state, _ = spice.spkezr("DAWN", et, "IAU_CERES", "NONE", "CERES")
            positions.append(state[:3])
        except spice.utils.exceptions.SpiceSPKINSUFFDATA:
            pass
        except Exception as exc:
            print(f"  SPICE warning at ET {et:.1f}: {exc}")

    if len(positions) < 2:
        return [], []

    positions = np.array(positions)

    mean_r   = float(np.linalg.norm(positions, axis=1).mean())
    mean_alt = mean_r - R_CERES
    lift_frac = 0.06 + 0.04 * np.clip((mean_alt - 375) / 4500, 0, 1.5)
    TRACK_R   = R_CERES * (1.0 + lift_frac)

    norms    = np.linalg.norm(positions, axis=1, keepdims=True)
    unit_pos = positions / norms
    pts_scaled = unit_pos * TRACK_R

    final_pts = [pts_scaled[0]]
    for i in range(1, len(pts_scaled)):
        p0 = pts_scaled[i - 1]
        p1 = pts_scaled[i]
        chord_mid   = (p0 + p1) * 0.5
        chord_mid_r = float(np.linalg.norm(chord_mid))
        if chord_mid_r < TRACK_R * 0.998:
            arc_mid = chord_mid / chord_mid_r * TRACK_R
            final_pts.append(arc_mid)
        final_pts.append(p1)

    final_pts = np.array(final_pts)

    norms2 = np.linalg.norm(final_pts, axis=1, keepdims=True)
    unit2  = final_pts / norms2
    lons_check = np.degrees(np.arctan2(unit2[:, 1], unit2[:, 0]))

    track_points = final_pts.flatten().tolist()

    track_lines = []
    seg_start   = 0
    for i in range(1, len(lons_check)):
        if abs(lons_check[i] - lons_check[i - 1]) > 170:
            seg = list(range(seg_start, i))
            if len(seg) >= 2:
                track_lines += [len(seg)] + seg
            seg_start = i
    seg = list(range(seg_start, len(lons_check)))
    if len(seg) >= 2:
        track_lines += [len(seg)] + seg

    print(f"  Orbit track '{phase_key}': {len(final_pts)} pts, "
          f"lift {lift_frac*100:.1f}%, track_r {TRACK_R:.1f} km")
    return track_points, track_lines


print("Generating initial orbit track…")
# Load the rec SPKs for the default phase before sampling the track.
# This keeps phase-isolation guarantees even on the very first render —
# nothing is preloaded outside load_phase_kernels' bookkeeping.
load_phase_kernels("all")
TRACK_POINTS, TRACK_LINES = build_orbit_track("all")

# ------------------------------------------------
# PLANET MESH
# ------------------------------------------------

lon_res = 1024
lat_res = 512

phi   = np.linspace(-np.pi,     np.pi,     lon_res)
theta = np.linspace(-np.pi / 2, np.pi / 2, lat_res)
phi, theta = np.meshgrid(phi, theta)

X = (R_CERES * np.cos(theta) * np.cos(phi)).flatten()
Y = (R_CERES * np.cos(theta) * np.sin(phi)).flatten()
Z = (R_CERES * np.sin(theta)).flatten()

CERES_PTS   = np.column_stack((X, Y, Z)).flatten().tolist()
CERES_VERTS = [len(X)] + list(range(len(X)))


# ------------------------------------------------
# PROJECT GRID TO SURFACE
# ------------------------------------------------

def project_to_surface(grid, vmin=None, vmax=None, cmap="plasma"):
    floor  = float(grid.max()) * 1e-4
    filled = grid[grid > floor]

    if filled.size < 10:
        grey = np.full((lat_res * lon_res * 3,), 80, dtype=np.uint8)
        return grey.tolist()

    lo = float(np.percentile(filled, 2))  if vmin is None else vmin
    hi = float(np.percentile(filled, 98)) if vmax is None else vmax
    if hi <= lo:
        hi = lo + 1e-9

    v  = np.clip((grid - lo) / (hi - lo), 0, 1)
    lat_index = np.clip(((np.degrees(theta) + 90)  / 180 * (LAT_BINS - 1)).astype(int), 0, LAT_BINS - 1)
    lon_index = np.clip(((np.degrees(phi)   + 180) / 360 * (LON_BINS - 1)).astype(int), 0, LON_BINS - 1)
    vals   = v[lat_index, lon_index]
    colors = (plt.get_cmap(cmap)(vals)[:, :, :3] * 255).astype(np.uint8)
    return colors.reshape(-1, 3).flatten().tolist()


# ------------------------------------------------
# TEXTURE
# ------------------------------------------------

try:
    img = Image.open(TEXTURE_FILE).convert("RGB")
    img = img.resize((lon_res, lat_res))
    tex = np.flipud(np.array(img))
    BASE_COLORS = tex.reshape(-1, 3)
except Exception:
    BASE_COLORS = np.ones((lat_res * lon_res, 3), dtype=np.uint8) * 180

BASE_COLORS_FLAT = BASE_COLORS.flatten().tolist()

# ------------------------------------------------
# LOAD ALL .tab FILES → observation point cloud
# ------------------------------------------------

TAB_MAX_DISPLAY_PTS = 5_000

if SPICE_AVAILABLE:
    _tab_files = [
        f for f in
        sorted(DATA_DIR.glob("**/*.tab")) + sorted(DATA_DIR.glob("**/*.TAB"))
        if f.suffix.lower() not in (".xml", ".lbl", ".fmt", ".cat")
    ]
    print(f"Indexing {len(_tab_files)} .tab files…")
else:
    _tab_files = []
    print("Skipping .tab indexing (SPICE unavailable — TAB heatmap disabled).")

_all_lats      = []
_all_lons      = []
_all_fps       = []
_all_row_idx   = []
_all_utcs      = []
_all_intensity = []
_all_ets       = []

_tab_heatmap  = np.zeros((LAT_BINS, LON_BINS))
_tab_hweights = np.zeros((LAT_BINS, LON_BINS))

for tf in _tab_files:
    fp   = str(tf)
    spec = load_spectrum(fp)
    if spec is None:
        continue

    lats    = spec["lat"]
    lons    = (spec["lon"] + 180) % 360 - 180
    counts  = spec["counts"]
    utcs    = spec["utc"]
    ets_arr = spec.get("et", np.zeros(len(lats)))
    N       = len(lats)

    if N == 0:
        continue

    if len(ets_arr) != N:
        ets_arr = np.zeros(N)

    intensity = counts.sum(axis=1)

    _all_lats.append(lats)
    _all_lons.append(lons)
    _all_fps.extend([fp] * N)
    _all_row_idx.extend(range(N))
    _all_utcs.extend(utcs if len(utcs) == N else [""] * N)
    _all_intensity.append(intensity)
    _all_ets.append(ets_arr)

    li = np.clip(((lats + 90)  / 180 * LAT_BINS).astype(int), 0, LAT_BINS - 1)
    lj = np.clip(((lons + 180) / 360 * LON_BINS).astype(int), 0, LON_BINS - 1)
    np.add.at(_tab_heatmap,  (li, lj), intensity)
    np.add.at(_tab_hweights, (li, lj), 1)

    print(f"  {tf.name}: {N:,} rows")

if _all_lats:
    _tab_lats_all = np.concatenate(_all_lats)
    _tab_lons_all = np.concatenate(_all_lons)
    _tab_int_all  = np.concatenate(_all_intensity)
    _tab_ets_all  = np.concatenate(_all_ets)
    n_total       = len(_tab_lats_all)

    print(f"  Total: {n_total:,} tab rows from {len(_tab_files)} files")
    print(f"  Array lengths — lats:{len(_tab_lats_all)} lons:{len(_tab_lons_all)} "
          f"ets:{len(_tab_ets_all)} fps:{len(_all_fps)} utcs:{len(_all_utcs)}")

    _tx_all, _ty_all, _tz_all = spherical(_tab_lats_all, _tab_lons_all, R_CERES * 1.01)
    TAB_TREE = KDTree(np.column_stack((_tx_all, _ty_all, _tz_all)))

    TAB_RECORDS = [
        {"lat":      float(_tab_lats_all[i]),
         "lon":      float(_tab_lons_all[i]),
         "filepath": _all_fps[i],
         "row_idx":  _all_row_idx[i],
         "utc":      _all_utcs[i],
         "et":       float(_tab_ets_all[i])}
        for i in range(n_total)
    ]

    if n_total > TAB_MAX_DISPLAY_PTS:
        stride   = n_total // TAB_MAX_DISPLAY_PTS
        disp_idx = np.arange(0, n_total, stride)[:TAB_MAX_DISPLAY_PTS]
    else:
        disp_idx = np.arange(n_total)

    _tx_d  = _tx_all[disp_idx]
    _ty_d  = _ty_all[disp_idx]
    _tz_d  = _tz_all[disp_idx]
    _int_d = _tab_int_all[disp_idx]

    TAB_PTS   = np.column_stack((_tx_d, _ty_d, _tz_d)).flatten().tolist()
    TAB_VERTS = [len(disp_idx)] + list(range(len(disp_idx)))

    _imin, _imax   = _tab_int_all.min(), _tab_int_all.max() + 1e-9
    _norm_d        = (_int_d - _imin) / (_imax - _imin)
    _rgba_d        = (plt.get_cmap("plasma")(_norm_d)[:, :3] * 255).astype(np.uint8)
    TAB_DOT_COLORS = _rgba_d.flatten().tolist()

    _msk = _tab_hweights > 0
    _tab_heatmap[_msk] /= _tab_hweights[_msk]
    gaussian_filter(_tab_heatmap, 1.5, output=_tab_heatmap)
    TAB_HEATMAP_COLORS = project_to_surface(_tab_heatmap, cmap="plasma")

else:
    TAB_RECORDS        = []
    TAB_TREE           = None
    TAB_PTS            = []
    TAB_VERTS          = []
    TAB_DOT_COLORS     = []
    TAB_HEATMAP_COLORS = BASE_COLORS_FLAT
    print("  No .tab files found or all failed to load.")

# ------------------------------------------------
# BUILD INITIAL ELEMENT MAPS
# ------------------------------------------------

print("Computing GRaND elemental grids (used for hotspot dot detection)…")
_map_results = build_element_maps(df)
Fe_grid, K_grid, Th_grid, H_grid = _map_results["values"]
HIT_COUNTS = _map_results["hit_counts"]

for _name, _g in [("Fe", Fe_grid), ("K", K_grid), ("Th", Th_grid), ("H", H_grid)]:
    _nz = _g[_g > 0]
    if len(_nz):
        print(f"  {_name} grid — nonzero cells: {len(_nz)},  "
              f"min: {_nz.min():.4f},  max: {_nz.max():.4f},  mean: {_nz.mean():.4f}")
    else:
        print(f"  {_name} grid — no nonzero cells (all data filtered out)")

# Element grids feed the hotspot-detection logic. They are no longer rendered
# as a surface texture (per user preference) but are kept available so the
# significance-dot toggles still work.
ELEMENT_GRIDS = {
    "Fe":       Fe_grid,
    "K":        K_grid,
    "Th":       Th_grid,
    "H":        H_grid,
    "tab_heat": _tab_heatmap,
}

# ------------------------------------------------
# ELEMENTAL SIGNIFICANCE DOT CLOUDS
# ------------------------------------------------

ELEM_SIGMA_THRESH = 1.5
ELEM_DOT_RADIUS   = R_CERES * 1.03

ELEM_DOT_CFG = {
    "Fe": {"rgb": [1.0,  0.42, 0.42], "size": 7, "label": "Fe"},
    "K":  {"rgb": [1.0,  0.82, 0.40], "size": 7, "label": "K"},
    "Th": {"rgb": [0.78, 0.48, 1.0],  "size": 7, "label": "Th"},
    "H":  {"rgb": [0.02, 0.84, 0.63], "size": 7, "label": "H"},
}

def _build_elem_dots(grid):
    observed = grid[grid > 0]
    if len(observed) < 5:
        return [], []
    mu        = observed.mean()
    sigma     = observed.std() + 1e-9
    threshold = mu + ELEM_SIGMA_THRESH * sigma
    lat_idx, lon_idx = np.where(grid >= threshold)
    if len(lat_idx) == 0:
        return [], []
    lats  = (lat_idx / LAT_BINS) * 180 - 90 + (90  / LAT_BINS)
    lons  = (lon_idx / LON_BINS) * 360 - 180 + (180 / LON_BINS)
    dx, dy, dz = spherical(lats, lons, ELEM_DOT_RADIUS)
    pts   = np.column_stack((dx, dy, dz)).flatten().tolist()
    n     = len(lats)
    verts = [n] + list(range(n))
    return pts, verts

print("Building elemental significance dot clouds…")
ELEM_GRIDS_DOTS = {"Fe": Fe_grid, "K": K_grid, "Th": Th_grid, "H": H_grid}
ELEM_DOTS       = {}
for _el, _grid in ELEM_GRIDS_DOTS.items():
    _pts, _verts = _build_elem_dots(_grid)
    ELEM_DOTS[_el] = {"pts": _pts, "verts": _verts}
    print(f"  {_el}: {len(_verts) - 1 if _verts else 0} significant cells "
          f"(>{ELEM_SIGMA_THRESH}σ above mean)")

# ------------------------------------------------
# OBSERVATION KD TREE
# ------------------------------------------------

obs_x, obs_y, obs_z = spherical(df.lat.values, df.lon_norm.values, R_CERES * 1.02)
obs_xyz = np.column_stack((obs_x, obs_y, obs_z))
TREE    = KDTree(obs_xyz)

# ------------------------------------------------
# METADATA / SIDECAR HELPERS
# ------------------------------------------------

# Track global state needed for sidecars
_CURRENT_PHASE_KEY = "all"

def _loaded_kernel_names():
    names = []
    try:
        n = spice.ktotal("ALL")
        for i in range(n):
            try:
                fname, ftype, ksrc, handle = spice.kdata(i, "ALL", 256, 32, 256)
                names.append(Path(fname).name)
            except Exception:
                pass
    except Exception:
        pass
    return names


def _build_metadata(kind, phase_key, filter_opts, extra=None):
    """Build a metadata dict for the JSON sidecar."""
    phase = ORBITAL_PHASES.get(phase_key, ORBITAL_PHASES["all"])
    apply_cr = "cr" in (filter_opts or [])
    apply_bg = "bg" in (filter_opts or [])
    md = {
        "schema": "grand-viewer-sidecar/v1",
        "kind":                 kind,
        "generated_utc":        datetime.utcnow().isoformat() + "Z",
        "phase_key":            phase_key,
        "phase_label":          phase["label"],
        "phase_start":          phase["start"],
        "phase_end":            phase["end"],
        "filters": {
            "cosmic_ray_clip":        apply_cr,
            "snip_background_subtract": apply_bg,
            "cr_sigma_threshold":     3.5,
            "cr_neighborhood_window": 15,
            "min_row_counts":         MIN_ROW_COUNTS,
            "min_cell_hits":          MIN_CELL_HITS,
        },
        "peak_windows_MeV":     {k: v for k, v in PEAK_WINDOWS.items()},
        "grid": {
            "lat_bins":           LAT_BINS,
            "lon_bins":           LON_BINS,
            "ceres_radius_km":    R_CERES,
            "n_trusted_cells":    int(_map_results["n_trusted"]),
            "n_filled_cells":     int(_map_results["n_filled"]),
        },
        "kernels_loaded":       _loaded_kernel_names(),
        "notes": (
            "Peak integrals are analytic Gaussian areas from curve_fit on "
            "SNIP-subtracted net spectra. Fallback to box-sum if fit fails. "
            "Uncertainty grids are relative (σ/value) derived from the "
            "weighted-mean variance Σ(wᵢ²σᵢ²)/(Σwᵢ)² with σᵢ from the fit "
            "covariance matrix (Poisson for the H ratio)."
        ),
    }
    if extra:
        md.update(extra)
    return md


def _zip_png_and_json(png_bytes, metadata, png_name):
    """Bundle a PNG and its JSON sidecar into a zip in memory."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(png_name, png_bytes)
        zf.writestr(
            png_name.rsplit(".", 1)[0] + ".json",
            json.dumps(metadata, indent=2),
        )
    buf.seek(0)
    return buf.read()


# ------------------------------------------------
# DASH APP
# ------------------------------------------------

app = Dash(__name__)

_data_files = sorted(
    list(DATA_DIR.glob("**/*.dat")) + list(DATA_DIR.glob("**/*.tab")) +
    list(DATA_DIR.glob("**/*.DAT")) + list(DATA_DIR.glob("**/*.TAB"))
)
_data_files = [f for f in _data_files
               if f.suffix.lower() not in (".xml", ".lbl", ".fmt", ".cat")]

_SKIP_DETECTOR_CODES = {"EPG", "NS", "NSS", "NSD", "HN", "LP"}
def _looks_like_grs(path):
    code = path.stem.upper().split("-")[-1]
    return code not in _SKIP_DETECTOR_CODES

_grs_files  = [f for f in _data_files if _looks_like_grs(f)]
_skip_count = len(_data_files) - len(_grs_files)
if _skip_count:
    print(f"  File picker: hiding {_skip_count} non-GRS file(s) (EPG/NS detectors)")

_FILE_PICKER_OPTIONS = [{"label": f.name, "value": str(f)} for f in _grs_files]
print(f"File browser: {len(_FILE_PICKER_OPTIONS)} GRS data files found in {DATA_DIR}")

_label_style = {"fontSize": "0.7rem", "opacity": 0.6, "letterSpacing": "0.15em"}
_dd_style    = {"color": "#000", "marginTop": "4px"}

app.layout = html.Div(
    style={"background": "#0a0e1a", "color": "#e0e8ff", "minHeight": "100vh",
           "padding": "20px", "fontFamily": "'Courier New', monospace"},
    children=[

        html.Div(
            style={"display": "flex", "alignItems": "center", "gap": "20px",
                   "marginBottom": "16px"},
            children=[
                html.H1("CERES · GRaND Elemental Viewer",
                        style={"margin": 0, "letterSpacing": "0.12em",
                               "fontSize": "1.4rem", "color": "#7ecfff"}),
                html.Span("DAWN MISSION DATA",
                          style={"opacity": 0.45, "fontSize": "0.75rem",
                                 "letterSpacing": "0.2em"}),
            ]
        ),

        html.Div(
            style={"display": "flex", "gap": "16px", "marginBottom": "12px",
                   "flexWrap": "wrap", "alignItems": "flex-end"},
            children=[
                html.Div([
                    html.Label("Surface Layer", style=_label_style),
                    dcc.Dropdown(
                        id="layer", style={**_dd_style, "width": "220px"},
                        options=(
                            [{"label": "Surface (Texture)",  "value": "surface"}] +
                            ([{"label": "TAB Counts Heatmap", "value": "tab_heat"}]
                             if SPICE_AVAILABLE else [])
                        ),
                        value="surface", clearable=False,
                    ),
                ]),
                html.Div([
                    html.Label("Orbital Phase", style=_label_style),
                    dcc.Dropdown(
                        id="phase", style={**_dd_style, "width": "220px"},
                        options=[{"label": v["label"], "value": k}
                                 for k, v in ORBITAL_PHASES.items()],
                        value="all", clearable=False,
                    ),
                ]),
                html.Div([
                    html.Label("SPICE Kernels", style=_label_style),
                    dcc.Dropdown(
                        id="kernel-mode-dd", style={**_dd_style, "width": "210px"},
                        options=[
                            {"label": "Minimal (current phase only)",
                             "value": "minimal"},
                            {"label": "All kernels (full mission)",
                             "value": "all"},
                        ],
                        # Initial value gets overridden from localStorage by
                        # the kernel-mode-bootstrap callback on app load.
                        value="minimal", clearable=False,
                    ),
                ]),
                html.Div([
                    html.Label("Browse Data File", style=_label_style),
                    dcc.Dropdown(
                        id="file-picker", style={**_dd_style, "width": "340px"},
                        options=_FILE_PICKER_OPTIONS, value=None,
                        placeholder="Select a .dat / .tab file…",
                        clearable=True, optionHeight=24,
                    ),
                ]),
                html.Div([
                    html.Label("Element Hotspots (1.5σ)", style=_label_style),
                    dcc.Checklist(
                        id="elem-dots-opts",
                        options=[
                            {"label": " Fe", "value": "Fe"},
                            {"label": " K",  "value": "K"},
                            {"label": " Th", "value": "Th"},
                            {"label": " H",  "value": "H"},
                        ],
                        value=[], inline=True,
                        style={"fontSize": "0.75rem", "color": "#e0e8ff",
                               "marginTop": "6px", "display": "flex", "gap": "10px"},
                        labelStyle={"cursor": "pointer"},
                    ),
                ]),
                html.Div([
                    html.Label("Noise Filters", style=_label_style),
                    dcc.Checklist(
                        id="filter-opts",
                        options=[
                            {"label": " CR Spike Clip",      "value": "cr"},
                            {"label": " BG Subtract (SNIP)", "value": "bg"},
                        ],
                        value=["cr", "bg"], inline=True,
                        style={"fontSize": "0.75rem", "color": "#7ecfff",
                               "marginTop": "6px", "display": "flex", "gap": "12px"},
                        labelStyle={"cursor": "pointer"},
                    ),
                ]),
                html.Div([
                    html.Label("\u00a0", style={"fontSize": "0.7rem"}),
                    html.Button(
                        "Recompute Maps", id="recompute-btn",
                        style={"display": "block", "background": "#1e3a5f",
                               "color": "#7ecfff", "border": "1px solid #7ecfff",
                               "padding": "6px 14px", "cursor": "pointer",
                               "letterSpacing": "0.1em", "fontSize": "0.8rem"},
                    ),
                ]),
                html.Div(id="phase-label",
                         style={"fontSize": "0.72rem", "opacity": 0.5,
                                "maxWidth": "320px"}),
            ]
        ),

        html.Div(id="filter-status",
                 style={"fontSize": "0.68rem", "opacity": 0.55, "marginBottom": "8px",
                        "color": "#ffd166", "letterSpacing": "0.1em", "minHeight": "1em"}),

        dash_vtk.View(
            id="vtk",
            style={"height": "52vh"},
            background=[0.04, 0.055, 0.1],
            cameraPosition=[R_CERES * 4, 0, 0],
            pickingModes=["click"],
            children=[
                dash_vtk.GeometryRepresentation(
                    children=[
                        dash_vtk.PolyData(
                            points=CERES_PTS, verts=CERES_VERTS,
                            children=[
                                dash_vtk.PointData([
                                    dash_vtk.DataArray(
                                        id="colors",
                                        registration="setScalars",
                                        values=BASE_COLORS_FLAT,
                                        type="Uint8Array",
                                        numberOfComponents=3,
                                    )
                                ])
                            ],
                        )
                    ]
                ),
                dash_vtk.GeometryRepresentation(
                    id="orbit-rep",
                    property={"color": [0.2, 1.0, 0.6], "lineWidth": 2},
                    children=[
                        dash_vtk.PolyData(
                            id="orbit-track",
                            points=TRACK_POINTS,
                            lines=TRACK_LINES,
                        )
                    ],
                ),
                dash_vtk.GeometryRepresentation(
                    id="tab-dots-rep",
                    property={"pointSize": 4, "opacity": 0.85},
                    children=[
                        dash_vtk.PolyData(
                            id="tab-dots",
                            points=TAB_PTS, verts=TAB_VERTS,
                            children=[
                                dash_vtk.PointData([
                                    dash_vtk.DataArray(
                                        id="tab-dot-colors",
                                        registration="setScalars",
                                        values=TAB_DOT_COLORS,
                                        type="Uint8Array",
                                        numberOfComponents=3,
                                    )
                                ])
                            ] if TAB_DOT_COLORS else [],
                        )
                    ],
                ),
                dash_vtk.GeometryRepresentation(
                    id="elem-dots-Fe",
                    property={"color": ELEM_DOT_CFG["Fe"]["rgb"],
                              "pointSize": ELEM_DOT_CFG["Fe"]["size"], "opacity": 0.92},
                    children=[dash_vtk.PolyData(id="elem-poly-Fe", points=[], verts=[])],
                ),
                dash_vtk.GeometryRepresentation(
                    id="elem-dots-K",
                    property={"color": ELEM_DOT_CFG["K"]["rgb"],
                              "pointSize": ELEM_DOT_CFG["K"]["size"], "opacity": 0.92},
                    children=[dash_vtk.PolyData(id="elem-poly-K", points=[], verts=[])],
                ),
                dash_vtk.GeometryRepresentation(
                    id="elem-dots-Th",
                    property={"color": ELEM_DOT_CFG["Th"]["rgb"],
                              "pointSize": ELEM_DOT_CFG["Th"]["size"], "opacity": 0.92},
                    children=[dash_vtk.PolyData(id="elem-poly-Th", points=[], verts=[])],
                ),
                dash_vtk.GeometryRepresentation(
                    id="elem-dots-H",
                    property={"color": ELEM_DOT_CFG["H"]["rgb"],
                              "pointSize": ELEM_DOT_CFG["H"]["size"], "opacity": 0.92},
                    children=[dash_vtk.PolyData(id="elem-poly-H", points=[], verts=[])],
                ),
            ],
        ),

        html.Div(
            style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                   "gap": "12px", "marginTop": "12px"},
            children=[
                html.Div([
                    html.Div(
                        style={"display": "flex", "alignItems": "flex-start",
                               "justifyContent": "space-between", "marginBottom": "4px"},
                        children=[
                            html.Div([
                                html.Span("GRaND ENERGY SPECTRUM",
                                          style={"fontSize": "0.75rem", "color": "#7ecfff",
                                                 "letterSpacing": "0.18em", "fontWeight": "bold"}),
                                html.Span("  ·  Net counts after CR clip & SNIP background subtraction",
                                          style={"fontSize": "0.62rem", "opacity": 0.5,
                                                 "letterSpacing": "0.05em"}),
                                html.Br(),
                                html.Span(id="spectrum-label",
                                          children="Click the globe or select a file to load a spectrum",
                                          style={"fontSize": "0.62rem", "color": "#ffd166",
                                                 "opacity": 0.75, "letterSpacing": "0.08em"}),
                            ]),
                            html.Div(
                                style={"display": "flex", "alignItems": "center",
                                       "gap": "6px", "flexShrink": "0", "marginLeft": "10px"},
                                children=[
                                    html.Div([
                                        html.Label("W px", style={"fontSize": "0.55rem",
                                                                   "opacity": 0.5, "display": "block"}),
                                        dcc.Input(id="spec-export-w", type="number", value=2400,
                                                  min=800, max=7200, step=100,
                                                  style={"width": "62px", "fontSize": "0.7rem",
                                                         "background": "#0d1a2e", "color": "#e0e8ff",
                                                         "border": "1px solid #334466", "padding": "2px 4px"}),
                                    ]),
                                    html.Div([
                                        html.Label("H px", style={"fontSize": "0.55rem",
                                                                   "opacity": 0.5, "display": "block"}),
                                        dcc.Input(id="spec-export-h", type="number", value=1000,
                                                  min=400, max=4000, step=100,
                                                  style={"width": "62px", "fontSize": "0.7rem",
                                                         "background": "#0d1a2e", "color": "#e0e8ff",
                                                         "border": "1px solid #334466", "padding": "2px 4px"}),
                                    ]),
                                    html.Div([
                                        html.Label("Scale", style={"fontSize": "0.55rem",
                                                                    "opacity": 0.5, "display": "block"}),
                                        dcc.Dropdown(
                                            id="spec-export-scale",
                                            options=[{"label": "1× (72 DPI)",  "value": 1},
                                                     {"label": "2× (144 DPI)", "value": 2},
                                                     {"label": "3× (216 DPI)", "value": 3},
                                                     {"label": "4× (288 DPI)", "value": 4}],
                                            value=3, clearable=False,
                                            style={"width": "120px", "fontSize": "0.7rem",
                                                   "color": "#000"},
                                        ),
                                    ]),
                                    html.Div([
                                        html.Label("\u00a0", style={"fontSize": "0.55rem",
                                                                     "display": "block"}),
                                        html.Button("⬇ PNG+JSON", id="export-spectrum-btn",
                                                    style={"background": "#1e3a5f",
                                                           "color": "#7ecfff",
                                                           "border": "1px solid #7ecfff",
                                                           "padding": "3px 10px",
                                                           "cursor": "pointer",
                                                           "fontSize": "0.7rem",
                                                           "letterSpacing": "0.08em"}),
                                    ]),
                                    dcc.Download(id="download-spectrum"),
                                ]
                            ),
                        ]
                    ),
                    dcc.Graph(id="spectrum", style={"height": "32vh"},
                              config={"displayModeBar": False}),
                ]),
                html.Div([
                    html.Div(
                        style={"display": "flex", "alignItems": "flex-start",
                               "justifyContent": "space-between", "marginBottom": "4px"},
                        children=[
                            html.Div([
                                html.Span("COUNTS vs CHANNEL  (Bin Detail)",
                                          style={"fontSize": "0.75rem", "color": "#ffd166",
                                                 "letterSpacing": "0.18em", "fontWeight": "bold"}),
                                html.Span("  ·  Click a point in the spectrum to zoom",
                                          style={"fontSize": "0.62rem", "opacity": 0.5,
                                                 "letterSpacing": "0.05em"}),
                                html.Br(),
                                html.Span(id="bin-label",
                                          children="Awaiting spectrum selection",
                                          style={"fontSize": "0.62rem", "color": "#ffd166",
                                                 "opacity": 0.75, "letterSpacing": "0.08em"}),
                            ]),
                            html.Div(
                                style={"display": "flex", "alignItems": "center",
                                       "gap": "6px", "flexShrink": "0", "marginLeft": "10px"},
                                children=[
                                    html.Div([
                                        html.Label("W px", style={"fontSize": "0.55rem",
                                                                   "opacity": 0.5, "display": "block"}),
                                        dcc.Input(id="bin-export-w", type="number", value=2400,
                                                  min=800, max=7200, step=100,
                                                  style={"width": "62px", "fontSize": "0.7rem",
                                                         "background": "#0d1a2e", "color": "#e0e8ff",
                                                         "border": "1px solid #334466", "padding": "2px 4px"}),
                                    ]),
                                    html.Div([
                                        html.Label("H px", style={"fontSize": "0.55rem",
                                                                   "opacity": 0.5, "display": "block"}),
                                        dcc.Input(id="bin-export-h", type="number", value=1000,
                                                  min=400, max=4000, step=100,
                                                  style={"width": "62px", "fontSize": "0.7rem",
                                                         "background": "#0d1a2e", "color": "#e0e8ff",
                                                         "border": "1px solid #334466", "padding": "2px 4px"}),
                                    ]),
                                    html.Div([
                                        html.Label("Scale", style={"fontSize": "0.55rem",
                                                                    "opacity": 0.5, "display": "block"}),
                                        dcc.Dropdown(
                                            id="bin-export-scale",
                                            options=[{"label": "1× (72 DPI)",  "value": 1},
                                                     {"label": "2× (144 DPI)", "value": 2},
                                                     {"label": "3× (216 DPI)", "value": 3},
                                                     {"label": "4× (288 DPI)", "value": 4}],
                                            value=3, clearable=False,
                                            style={"width": "120px", "fontSize": "0.7rem",
                                                   "color": "#000"},
                                        ),
                                    ]),
                                    html.Div([
                                        html.Label("\u00a0", style={"fontSize": "0.55rem",
                                                                     "display": "block"}),
                                        html.Button("⬇ PNG+JSON", id="export-bin-btn",
                                                    style={"background": "#2a2a10",
                                                           "color": "#ffd166",
                                                           "border": "1px solid #ffd166",
                                                           "padding": "3px 10px",
                                                           "cursor": "pointer",
                                                           "fontSize": "0.7rem",
                                                           "letterSpacing": "0.08em"}),
                                    ]),
                                    dcc.Download(id="download-bin"),
                                ]
                            ),
                        ]
                    ),
                    dcc.Graph(id="bin-graph", style={"height": "32vh"},
                              config={"displayModeBar": False}),
                ]),
            ]
        ),

        dcc.Store(id="current-file"),
        dcc.Store(id="current-counts"),
        dcc.Store(id="current-row-counts"),
        dcc.Store(id="current-source"),
        dcc.Store(id="current-layer", data="surface"),
        dcc.Store(id="current-phase", data="all"),
        # Persistent kernel-mode setting — survives page reloads via
        # browser localStorage. Default "minimal" on first ever visit.
        dcc.Store(id="kernel-mode-store", storage_type="local", data="minimal"),
    ]
)


# ────────────────────────────────────────────────────────────────────────────
# CALLBACK 1 — layer switcher
# ────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("colors", "values"),
    Output("current-layer", "data"),
    Input("layer", "value"),
)
def update_layer(layer):
    # Only Surface and TAB Heatmap remain in the dropdown.
    # Anything else falls back to the texture so unexpected values don't
    # leave the globe blank.
    if layer == "tab_heat":
        return TAB_HEATMAP_COLORS, "tab_heat"
    return BASE_COLORS_FLAT, "surface"


# ────────────────────────────────────────────────────────────────────────────
# CALLBACK 1a — track the current phase selection
# ────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("current-phase", "data"),
    Input("phase", "value"),
)
def remember_phase(phase_key):
    return phase_key or "all"


# ────────────────────────────────────────────────────────────────────────────
# CALLBACK 1b — element hotspot dots toggle
# ────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("elem-poly-Fe", "points"), Output("elem-poly-Fe", "verts"),
    Output("elem-poly-K",  "points"), Output("elem-poly-K",  "verts"),
    Output("elem-poly-Th", "points"), Output("elem-poly-Th", "verts"),
    Output("elem-poly-H",  "points"), Output("elem-poly-H",  "verts"),
    Input("elem-dots-opts", "value"),
)
def toggle_elem_dots(selected):
    selected = selected or []
    results  = []
    for el in ("Fe", "K", "Th", "H"):
        if el in selected:
            results.append(ELEM_DOTS[el]["pts"])
            results.append(ELEM_DOTS[el]["verts"])
        else:
            results.append([])
            results.append([])
    return tuple(results)



# ────────────────────────────────────────────────────────────────────────────
# MATPLOTLIB EXPORT HELPERS
# ────────────────────────────────────────────────────────────────────────────

import matplotlib.pyplot as _mplt
import matplotlib.ticker as _ticker

_BG     = "#0d1424"
_FG     = "#e0e8ff"
_GRID   = "#1e2a3a"
_NET    = "#7ecfff"
_SMOOTH = "#ff9f43"
_RAW    = "#4a6fa5"
_BG_LN  = "#888888"
_BAR    = "#ffd166"

_ROI_BANDS = [
    (460,  560,  "#00e5ff"),
    (2180, 2260, "#00e5ff"),
    (1440, 1480, "#ffd166"),
    (2580, 2640, "#c97bff"),
    (7550, 7720, "#ff4444"),
]


def _mpl_spectrum(C_raw, title, label, width_px, height_px, dpi,
                  apply_cr=True, apply_bg=True):
    """Re-render the energy spectrum with matplotlib for PNG export."""
    result = apply_spectral_filters(
        np.asarray(C_raw, dtype=float), apply_cr=apply_cr, apply_bg=apply_bg
    )
    n_ch  = len(result["raw"])
    E_MeV = channel_to_energy(np.arange(n_ch), n_channels=n_ch)
    E_keV = E_MeV * 1000

    mask  = E_keV >= 100
    E_d   = E_keV[mask]
    raw_d = result["raw"][mask]
    net_d = result["net"][mask]
    bg_d  = result["background"][mask]

    figw = width_px  / dpi
    figh = height_px / dpi
    fig, ax = _mplt.subplots(figsize=(figw, figh), dpi=dpi)
    fig.patch.set_facecolor(_BG)
    ax.set_facecolor(_BG)

    for x0, x1, col in _ROI_BANDS:
        if x1 > E_d[0] and x0 < E_d[-1]:
            ax.axvspan(x0, x1, alpha=0.10, color=col, linewidth=0)

    ax.plot(E_d, raw_d, color=_RAW,   lw=0.9, alpha=0.4, label="Raw")
    if apply_bg:
        ax.plot(E_d, bg_d, color=_BG_LN, lw=0.9, ls="--", alpha=0.6,
                label="SNIP background")
    ax.plot(E_d, net_d, color=_NET,   lw=1.8, label="Net (C − BG)")
    if len(net_d) >= 21:
        sm = savgol_filter(net_d, 21, 3)
        ax.plot(E_d, sm, color=_SMOOTH, lw=2.2, label="Smoothed")

    for e_kev, lbl, col in GAMMA_LINES:
        if E_d[0] <= e_kev <= E_d[-1]:
            ax.axvline(e_kev, color=col, lw=1.0, ls=":", alpha=0.9)
            ax.text(e_kev + 25, ax.get_ylim()[1] if ax.get_ylim()[1] > 1 else 1,
                    lbl, color=col, fontsize=max(7, int(figw * 0.55)),
                    rotation=90, va="top", ha="left")

    ax.set_yscale("log")
    ax.set_xlim(E_d[0], E_d[-1])

    fs_label = max(11, int(figw * 0.75))
    fs_tick  = max(9,  int(figw * 0.62))
    ax.set_xlabel("Energy (keV)", color=_FG, fontsize=fs_label, labelpad=8)
    ax.set_ylabel("Counts (log scale)", color=_FG, fontsize=fs_label, labelpad=8)
    ax.tick_params(colors=_FG, labelsize=fs_tick, which="both")
    ax.xaxis.set_major_formatter(_ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for spine in ax.spines.values():
        spine.set_edgecolor(_GRID)
    ax.grid(True, color=_GRID, linewidth=0.6, which="major")
    ax.grid(True, color=_GRID, linewidth=0.3, which="minor", alpha=0.4)

    fs_title = max(13, int(figw * 0.9))
    fig.text(0.5, 0.97, title,  ha="center", va="top",
             color=_NET, fontsize=fs_title, fontweight="bold")
    fig.text(0.5, 0.93, label,  ha="center", va="top",
             color=_BAR, fontsize=max(9, int(figw * 0.62)))
    fig.text(0.5, 0.005,
             "Dawn Mission · GRaND BGO Detector · LAMO/HAMO Phase · Ceres",
             ha="center", va="bottom",
             color=_FG, fontsize=max(8, int(figw * 0.55)), alpha=0.55)

    ax.legend(facecolor=_BG, edgecolor=_GRID, labelcolor=_FG,
              fontsize=max(8, int(figw * 0.58)), loc="upper right")

    fig.tight_layout(rect=[0, 0.02, 1, 0.91])

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi,
                facecolor=fig.get_facecolor(), bbox_inches="tight")
    _mplt.close(fig)
    buf.seek(0)
    return buf.read()


def _mpl_bin(C_net, C_sum, title, label, width_px, height_px, dpi,
             source="tab", n_channels=None):
    bins  = np.arange(len(C_net))
    E_keV = channel_to_energy(bins, n_channels=n_channels or len(C_net)) * 1000

    figw = width_px  / dpi
    figh = height_px / dpi
    fig, ax = _mplt.subplots(figsize=(figw, figh), dpi=dpi)
    fig.patch.set_facecolor(_BG)
    ax.set_facecolor(_BG)

    bar_col = _BAR if source == "tab" else _NET
    ax.bar(bins, C_net, color=bar_col, linewidth=0, alpha=0.80,
           width=1.0, label="Counts (net)")

    if C_sum is not None and source == "tab":
        ax.plot(bins, C_sum[:len(bins)], color=_NET, lw=1.6, label="File sum")

    if len(C_net) >= 11:
        sg = min(21, len(C_net) if len(C_net) % 2 else len(C_net) - 1)
        ax.plot(bins, savgol_filter(C_net, sg, 3),
                color="#ff6b6b", lw=2.0, label="Smoothed")

    for e_kev, lbl, col in GAMMA_LINES:
        if E_keV[0] <= e_kev <= E_keV[-1]:
            ch_idx = int(np.argmin(np.abs(E_keV - e_kev)))
            ax.axvline(ch_idx, color=col, lw=0.9, ls=":", alpha=0.9)
            ax.text(ch_idx + 0.3, ax.get_ylim()[1] if ax.get_ylim()[1] > 0 else 1,
                    lbl, color=col, fontsize=max(7, int(figw * 0.55)),
                    rotation=90, va="top", ha="left")

    ax2 = ax.twiny()
    ax2.set_facecolor(_BG)
    tick_step = max(1, len(bins) // 8)
    tick_ch   = bins[::tick_step]
    tick_kev  = E_keV[::tick_step]
    ax2.set_xlim(ax.get_xlim())
    ax2.set_xticks(tick_ch)
    ax2.set_xticklabels([f"{int(v):,}" for v in tick_kev],
                        fontsize=max(8, int(figw * 0.58)), color=_FG)
    ax2.set_xlabel("Energy (keV)", color=_FG,
                   fontsize=max(10, int(figw * 0.70)), labelpad=6)
    ax2.tick_params(colors=_FG)
    for spine in ax2.spines.values():
        spine.set_edgecolor(_GRID)

    fs_label = max(11, int(figw * 0.75))
    fs_tick  = max(9,  int(figw * 0.62))
    ax.set_xlabel("Channel (Bin)", color=_FG, fontsize=fs_label, labelpad=8)
    ax.set_ylabel("Counts",        color=_FG, fontsize=fs_label, labelpad=8)
    ax.tick_params(colors=_FG, labelsize=fs_tick)
    for spine in ax.spines.values():
        spine.set_edgecolor(_GRID)
    ax.grid(True, color=_GRID, linewidth=0.6, axis="y")

    fs_title = max(13, int(figw * 0.9))
    fig.text(0.5, 0.97, title, ha="center", va="top",
             color=_BAR, fontsize=fs_title, fontweight="bold")
    fig.text(0.5, 0.93, label, ha="center", va="top",
             color=_BAR, fontsize=max(9, int(figw * 0.62)), alpha=0.8)
    fig.text(0.5, 0.005,
             "Dawn Mission · GRaND BGO Detector · LAMO/HAMO Phase · Ceres",
             ha="center", va="bottom",
             color=_FG, fontsize=max(8, int(figw * 0.55)), alpha=0.55)

    ax.legend(facecolor=_BG, edgecolor=_GRID, labelcolor=_FG,
              fontsize=max(8, int(figw * 0.58)), loc="upper right")

    fig.tight_layout(rect=[0, 0.02, 1, 0.91])

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi,
                facecolor=fig.get_facecolor(), bbox_inches="tight")
    _mplt.close(fig)
    buf.seek(0)
    return buf.read()


# ────────────────────────────────────────────────────────────────────────────
# CALLBACK — export spectrum (PNG + JSON bundle)
# ────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("download-spectrum", "data"),
    Input("export-spectrum-btn",  "n_clicks"),
    State("current-row-counts",   "data"),
    State("current-file",         "data"),
    State("current-phase",        "data"),
    State("spec-export-w",        "value"),
    State("spec-export-h",        "value"),
    State("spec-export-scale",    "value"),
    State("spectrum-label",       "children"),
    State("filter-opts",          "value"),
    prevent_initial_call=True,
)
def export_spectrum(n_clicks, row_counts, filepath, phase_key,
                    width, height, scale, label, filter_opts):
    if not n_clicks or row_counts is None:
        return dash.no_update

    width  = int(width  or 2400)
    height = int(height or 1000)
    scale  = int(scale  or 3)
    dpi    = 72 * scale

    apply_cr = "cr" in (filter_opts or [])
    apply_bg = "bg" in (filter_opts or [])
    C_raw    = np.array(row_counts, dtype=float)
    title    = "GRaND Energy Spectrum  —  Dawn Mission at Ceres"
    lbl      = str(label or "")

    try:
        png_bytes = _mpl_spectrum(C_raw, title, lbl,
                                  width_px=width, height_px=height, dpi=dpi,
                                  apply_cr=apply_cr, apply_bg=apply_bg)
    except Exception as e:
        print(f"  Matplotlib export error (spectrum): {e}")
        return dash.no_update

    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in lbl)[:60]
    png_name = f"GRaND_spectrum_{safe}.png" if safe else "GRaND_spectrum.png"
    zip_name = png_name.replace(".png", "_bundle.zip")

    extra = {
        "source_file": Path(filepath).name if filepath else None,
        "label": lbl,
        "export_dpi": dpi,
        "export_dimensions_px": [width, height],
    }
    md = _build_metadata(kind="spectrum",
                         phase_key=(phase_key or "all"),
                         filter_opts=filter_opts, extra=extra)
    zip_bytes = _zip_png_and_json(png_bytes, md, png_name)
    b64 = base64.b64encode(zip_bytes).decode()
    return dict(content=b64, filename=zip_name, base64=True,
                type="application/zip")


# ────────────────────────────────────────────────────────────────────────────
# CALLBACK — export bin plot (PNG + JSON bundle)
# ────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("download-bin", "data"),
    Input("export-bin-btn",       "n_clicks"),
    State("current-row-counts",   "data"),
    State("current-counts",       "data"),
    State("current-source",       "data"),
    State("current-file",         "data"),
    State("current-phase",        "data"),
    State("bin-export-w",         "value"),
    State("bin-export-h",         "value"),
    State("bin-export-scale",     "value"),
    State("bin-label",            "children"),
    State("filter-opts",          "value"),
    prevent_initial_call=True,
)
def export_bin(n_clicks, row_counts, sum_counts, source, filepath, phase_key,
               width, height, scale, label, filter_opts):
    if not n_clicks or row_counts is None:
        return dash.no_update

    width  = int(width  or 2400)
    height = int(height or 1000)
    scale  = int(scale  or 3)
    dpi    = 72 * scale

    apply_cr = "cr" in (filter_opts or [])
    apply_bg = "bg" in (filter_opts or [])
    C_row    = np.array(row_counts, dtype=float)
    C_sum    = np.array(sum_counts, dtype=float) if sum_counts else None
    n_ch     = len(C_row)

    filt_row = apply_spectral_filters(C_row, apply_cr=apply_cr, apply_bg=apply_bg)
    C_net    = filt_row["net"]
    C_sum_net = None
    if C_sum is not None:
        filt_sum  = apply_spectral_filters(C_sum, apply_cr=apply_cr, apply_bg=apply_bg)
        C_sum_net = filt_sum["net"]

    title = "GRaND Counts vs Channel  —  Dawn Mission at Ceres"
    lbl   = str(label or "")

    try:
        png_bytes = _mpl_bin(C_net, C_sum_net, title, lbl,
                             width_px=width, height_px=height, dpi=dpi,
                             source=source or "tab", n_channels=n_ch)
    except Exception as e:
        print(f"  Matplotlib export error (bin): {e}")
        return dash.no_update

    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in lbl)[:60]
    png_name = f"GRaND_bin_{safe}.png" if safe else "GRaND_bin.png"
    zip_name = png_name.replace(".png", "_bundle.zip")

    extra = {
        "source_file": Path(filepath).name if filepath else None,
        "label":       lbl,
        "source":      source,
        "export_dpi":  dpi,
        "export_dimensions_px": [width, height],
        "n_channels":  n_ch,
    }
    md = _build_metadata(kind="counts_vs_channel",
                         phase_key=(phase_key or "all"),
                         filter_opts=filter_opts, extra=extra)
    zip_bytes = _zip_png_and_json(png_bytes, md, png_name)
    b64 = base64.b64encode(zip_bytes).decode()
    return dict(content=b64, filename=zip_name, base64=True,
                type="application/zip")


# ────────────────────────────────────────────────────────────────────────────
# CALLBACK 2 — orbital phase OR kernel mode → orbit track + label
# ────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("orbit-track", "points"),
    Output("orbit-track", "lines"),
    Output("phase-label", "children"),
    Input("phase",          "value"),
    Input("kernel-mode-dd", "value"),
)
def update_orbit(phase_key, kernel_mode):
    """Reload SPICE kernels for the chosen phase + mode, then resample track.
    Either input firing triggers a fresh, isolated kernel reload — no carry-
    over from previous selections."""
    global KERNEL_MODE
    KERNEL_MODE = kernel_mode if kernel_mode in ("minimal", "all") else "minimal"

    if not SPICE_AVAILABLE:
        # No SPICE → no orbit track. Show a single hidden vertex so VTK is happy.
        dummy = spherical([0], [0], R_CERES * 1.15)
        pts   = np.column_stack(dummy).flatten().tolist()
        lns   = [1, 0]
        phase = ORBITAL_PHASES[phase_key]
        label = (f"{phase['start']} → {phase['end']}  ·  "
                 f"orbit track disabled (SPICE kernels not available)")
        return pts, lns, label

    load_phase_kernels(phase_key)

    phase       = ORBITAL_PHASES[phase_key]
    times_range = clamp_phase_times(phase_key, get_spk_coverage(-203))

    if times_range is None:
        label = (f"⚠ {phase['start']} → {phase['end']}  "
                 f"(outside kernel coverage in '{KERNEL_MODE}' mode)")
        dummy = spherical([0], [0], R_CERES * 1.15)
        pts   = np.column_stack(dummy).flatten().tolist()
        lns   = [1, 0]
        return pts, lns, label

    pts, lns = build_orbit_track(phase_key)

    try:
        utc0  = spice.et2utc(times_range[0], "ISOC", 0)
        utc1  = spice.et2utc(times_range[1], "ISOC", 0)
        label = (f"{utc0[:10]}  →  {utc1[:10]}   "
                 f"·  kernel mode: {KERNEL_MODE}  "
                 f"·  {len(_LOADED_PHASE_KERNELS)} rec SPK(s) loaded")
        if times_range[0] > spice.utc2et(phase["start"]) or \
           times_range[1] < spice.utc2et(phase["end"]):
            label += "  (clamped to kernel coverage)"
    except Exception:
        label = f"{phase['start']}  →  {phase['end']}  (mode: {KERNEL_MODE})"

    return pts, lns, label


# ────────────────────────────────────────────────────────────────────────────
# CALLBACK 2a — persist kernel mode to localStorage when dropdown changes
# ────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("kernel-mode-store", "data"),
    Input("kernel-mode-dd", "value"),
    prevent_initial_call=True,
)
def persist_kernel_mode(value):
    return value if value in ("minimal", "all") else "minimal"


# ────────────────────────────────────────────────────────────────────────────
# CALLBACK 2b — restore kernel mode from localStorage on first page load
# ────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("kernel-mode-dd", "value"),
    Input("kernel-mode-store", "data"),
)
def restore_kernel_mode(stored_value):
    """Fires once on app load. If the user has visited before, this pulls
    their last choice out of localStorage and sets the dropdown to match.
    Subsequent changes flow dropdown → store via persist_kernel_mode."""
    if stored_value in ("minimal", "all"):
        return stored_value
    return "minimal"


# ────────────────────────────────────────────────────────────────────────────
# CALLBACK 3 — recompute elemental grids for hotspot dots
# (Element value grids feed the 1.5σ hotspot detector. They're no longer
# offered as a surface-texture layer.)
# ────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("colors", "values", allow_duplicate=True),
    Input("recompute-btn", "n_clicks"),
    State("phase", "value"),
    State("layer", "value"),
    prevent_initial_call=True,
)
def recompute_maps(n_clicks, phase_key, layer):
    if not n_clicks:
        return dash.no_update

    phase  = ORBITAL_PHASES[phase_key]
    sub_df = df.copy()

    if SPICE_AVAILABLE:
        # Filter the CSV by phase date window using SPICE for ET conversion
        et_start = spice.utc2et(phase["start"])
        et_end   = spice.utc2et(phase["end"])
        if "utc" in df.columns:
            sub_df["et"] = sub_df["utc"].apply(lambda u: spice.utc2et(str(u)))
            sub_df = sub_df[(sub_df["et"] >= et_start) & (sub_df["et"] <= et_end)]
        res = build_element_maps(sub_df, et_start=et_start, et_end=et_end)
    else:
        # No SPICE → can't filter by phase, just rebuild from the full CSV
        print("  Recompute: no SPICE — phase filtering disabled, "
              "using full CSV.")
        res = build_element_maps(sub_df)

    Fe, K, Th, H = res["values"]
    ELEMENT_GRIDS.update({"Fe": Fe, "K": K, "Th": Th, "H": H})

    # The visible surface only switches between Surface texture and TAB heatmap.
    if layer == "tab_heat" and SPICE_AVAILABLE:
        return TAB_HEATMAP_COLORS
    return BASE_COLORS_FLAT


# ────────────────────────────────────────────────────────────────────────────
# FIGURE BUILDERS
# ────────────────────────────────────────────────────────────────────────────

GAMMA_LINES = [
    (511,   "H 511",    "#00e5ff"),
    (1461,  "K",        "#ffd166"),
    (1779,  "Si",       "#aaaaaa"),
    (2223,  "H 2.2",    "#00e5ff"),
    (2614,  "Th",       "#c97bff"),
    (4945,  "Fe 4.9",   "#ff7f7f"),
    (6129,  "O",        "#88ff88"),
    (7631,  "Fe 7.6",   "#ff4444"),
    (8579,  "Ni",       "#ffaa44"),
]


def _make_spectrum_fig(C_raw, title_str, apply_cr=True, apply_bg=True):
    """Build the inline plotly spectrum figure. Returns (fig, filter_result)."""
    result = apply_spectral_filters(
        np.asarray(C_raw, dtype=float), apply_cr=apply_cr, apply_bg=apply_bg,
    )
    n_ch  = len(result["raw"])
    E_MeV = channel_to_energy(np.arange(n_ch), n_channels=n_ch)
    E     = E_MeV * 1000  # keV

    display_mask = E >= 100
    E_d   = E[display_mask]
    raw_d = result["raw"][display_mask]
    net_d = result["net"][display_mask]
    bg_d  = result["background"][display_mask]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=E_d, y=raw_d, mode="lines", name="Raw",
        line=dict(color="#4a6fa5", width=1), opacity=0.4,
    ))

    if apply_bg:
        fig.add_trace(go.Scatter(
            x=E_d, y=bg_d, mode="lines", name="SNIP background",
            line=dict(color="#888888", width=1, dash="dash"), opacity=0.6,
        ))

    fig.add_trace(go.Scatter(
        x=E_d, y=net_d, mode="lines", name="Net",
        line=dict(color="#7ecfff", width=2),
    ))

    if len(net_d) >= 11:
        sg_w   = min(21, len(net_d) if len(net_d) % 2 else len(net_d) - 1)
        smooth = savgol_filter(net_d, sg_w, 3)
        fig.add_trace(go.Scatter(
            x=E_d, y=smooth, mode="lines", name="Smoothed net",
            line=dict(color="#ff9f43", width=2),
        ))

    for e_kev, label, color in GAMMA_LINES:
        if E_d[0] <= e_kev <= E_d[-1]:
            fig.add_vline(
                x=e_kev,
                line=dict(color=color, width=1, dash="dot"),
                annotation=dict(
                    text=f"<b>{label}</b>",
                    font=dict(size=9, color=color),
                    textangle=-90,
                    yanchor="bottom",
                    showarrow=False,
                ),
                annotation_position="top",
            )

    ROI_BANDS = [
        (460,  560,  "#00e5ff", "H 511 ROI"),
        (2180, 2260, "#00e5ff", "H 2.2 MeV ROI"),
        (1440, 1480, "#ffd166", "K ROI"),
        (2580, 2640, "#c97bff", "Th ROI"),
        (7550, 7720, "#ff4444", "Fe ROI"),
    ]
    for x0, x1, color, name in ROI_BANDS:
        if x1 > E_d[0] and x0 < E_d[-1]:
            fig.add_vrect(
                x0=x0, x1=x1,
                fillcolor=color, opacity=0.08,
                line_width=0,
                annotation_text="", name=name,
            )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0d1424",
        plot_bgcolor="#0d1424",
        title=dict(text=title_str, font=dict(size=11, color="#c8d8ff")),
        xaxis=dict(
            title="Energy (keV)",
            showgrid=True, gridcolor="#1e2a3a", gridwidth=1,
            tickformat=",d",
        ),
        yaxis=dict(
            title="Counts",
            type="log",
            showgrid=True, gridcolor="#1e2a3a", gridwidth=1,
        ),
        legend=dict(font=dict(size=9), bgcolor="rgba(0,0,0,0)"),
        margin=dict(l=55, r=20, t=40, b=45),
        hovermode="x unified",
    )
    return fig, result


def _make_bin_fig(C, title_str, source="dat", C_sum=None, n_channels=None):
    bins = np.arange(len(C))
    E_arr = channel_to_energy(bins, n_channels=n_channels or len(C)) * 1000
    tick_step = max(1, len(bins) // 8)
    tick_ch   = bins[::tick_step]
    tick_kev  = E_arr[::tick_step]
    tick_lbl  = [f"{int(v)}" for v in tick_kev]

    sg     = min(11, len(C) if len(C) % 2 else len(C) - 1)
    smooth = savgol_filter(C, sg, 3) if len(C) >= 11 else C.copy()

    fig     = go.Figure()
    bar_col = "#ffd166" if source == "tab" else "#7ecfff"

    fig.add_trace(go.Bar(
        x=bins, y=C,
        marker_color=bar_col, marker_line_width=0,
        name="Counts", opacity=0.75,
    ))

    if C_sum is not None and source == "tab":
        fig.add_trace(go.Scatter(
            x=bins, y=np.asarray(C_sum)[:len(bins)],
            mode="lines", line=dict(color="#7ecfff", width=2),
            name="File sum",
        ))

    fig.add_trace(go.Scatter(
        x=bins, y=smooth,
        mode="lines", line=dict(color="#ff6b6b", width=2),
        name="Smoothed",
    ))

    for e_kev, label, color in GAMMA_LINES:
        if E_arr[0] <= e_kev <= E_arr[-1]:
            ch_idx = int(np.argmin(np.abs(E_arr - e_kev)))
            fig.add_vline(
                x=ch_idx,
                line=dict(color=color, width=1, dash="dot"),
                annotation=dict(
                    text=f"<b>{label}</b>",
                    font=dict(size=8, color=color),
                    textangle=-90,
                    yanchor="bottom",
                    showarrow=False,
                ),
                annotation_position="top",
            )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0d1424",
        plot_bgcolor="#0d1424",
        title=dict(text=title_str, font=dict(size=11, color="#ffd166")),
        xaxis=dict(
            title="Channel",
            showgrid=True, gridcolor="#1e2a3a",
            tickmode="array",
            tickvals=tick_ch.tolist(),
            ticktext=[f"{ch}<br><span style='font-size:8px;color:#888'>{kev} keV</span>"
                      for ch, kev in zip(tick_ch, tick_lbl)],
        ),
        yaxis=dict(
            title="Counts",
            showgrid=True, gridcolor="#1e2a3a",
        ),
        bargap=0.02,
        legend=dict(font=dict(size=9), bgcolor="rgba(0,0,0,0)"),
        margin=dict(l=55, r=20, t=40, b=55),
        hovermode="x unified",
    )
    return fig


def _filter_status_str(filt_result, apply_cr, apply_bg):
    parts = []
    if apply_cr:
        parts.append(f"CR clip: {filt_result['n_spikes']} spike(s) removed")
    else:
        parts.append("CR clip: OFF")
    if apply_bg:
        bg_pct = (filt_result["background"].sum() /
                  (filt_result["raw"].sum() + 1e-9) * 100)
        parts.append(f"BG subtract (SNIP): {bg_pct:.1f}% continuum removed")
    else:
        parts.append("BG subtract: OFF")
    return "  |  ".join(parts)


# ────────────────────────────────────────────────────────────────────────────
# CALLBACK 4 — globe click → energy spectrum
# ────────────────────────────────────────────────────────────────────────────

def _empty_fig():
    f = go.Figure()
    f.update_layout(template="plotly_dark",
                    paper_bgcolor="#0a0e1a", plot_bgcolor="#0a0e1a")
    return f


@app.callback(
    Output("spectrum",           "figure"),
    Output("bin-graph",          "figure"),
    Output("current-file",       "data"),
    Output("current-counts",     "data"),
    Output("current-row-counts", "data"),
    Output("current-source",     "data"),
    Output("spectrum-label",     "children"),
    Output("bin-label",          "children"),
    Output("filter-status",      "children"),
    Input("vtk", "clickInfo"),
    State("filter-opts", "value"),
)
def show_spectrum(click, filter_opts):
    no_data = (_empty_fig(), _empty_fig(), None, None, None, None,
               "Click the globe or select a file to load a spectrum",
               "Awaiting spectrum selection", "")

    if click is None or "worldPosition" not in click:
        return no_data

    apply_cr = "cr" in (filter_opts or [])
    apply_bg = "bg" in (filter_opts or [])
    pos      = click["worldPosition"]
    lat, lon = latlon(*pos)
    x, y, z  = spherical(lat, lon)

    if TAB_TREE is not None:
        _, idx_tab = TAB_TREE.query([x, y, z])
        rec  = TAB_RECORDS[idx_tab]
        spec = load_spectrum(rec["filepath"])
        if spec is not None:
            row_C   = spec["counts"][rec["row_idx"]]
            sum_C   = np.sum(spec["counts"], axis=0)
            fname   = Path(rec["filepath"]).name
            utc_str = rec["utc"] or ""
            spec_title = (f"GRaND  {rec['lat']:.2f}°, {rec['lon']:.2f}°  "
                          f"|  {fname}  {utc_str[:19]}")
            bin_title  = f"Counts vs Bin  —  row {rec['row_idx']}  ({fname})"
            spec_fig, filt = _make_spectrum_fig(
                row_C, spec_title, apply_cr=apply_cr, apply_bg=apply_bg
            )
            bin_fig = _make_bin_fig(filt["net"], bin_title,
                                    source="tab", C_sum=sum_C.tolist(),
                                    n_channels=spec["n_channels"])
            status  = _filter_status_str(filt, apply_cr, apply_bg)
            return (spec_fig, bin_fig,
                    rec["filepath"], sum_C.tolist(), row_C.tolist(), "tab",
                    f"ENERGY SPECTRUM  —  {fname}  row {rec['row_idx']}",
                    f"COUNTS vs BIN  —  {fname}  row {rec['row_idx']}", status)

    _, idx   = TREE.query([x, y, z])
    row      = df.iloc[idx]
    filepath = str(DATA_DIR / row.filename)
    spec     = load_spectrum(filepath)
    if spec is None:
        return no_data
    C     = np.sum(spec["counts"], axis=0)
    fname = Path(filepath).name
    spec_fig, filt = _make_spectrum_fig(
        C, f"GRaND Spectrum  {lat:.2f}°, {lon:.2f}°  |  {fname}",
        apply_cr=apply_cr, apply_bg=apply_bg,
    )
    bin_fig = _make_bin_fig(filt["net"], f"Counts vs Bin  —  {fname}",
                            n_channels=spec["n_channels"])
    status  = _filter_status_str(filt, apply_cr, apply_bg)
    return (spec_fig, bin_fig, filepath, C.tolist(), C.tolist(), "dat",
            f"ENERGY SPECTRUM  —  {fname}", f"COUNTS vs BIN  —  {fname}", status)


# ────────────────────────────────────────────────────────────────────────────
# CALLBACK 5 — spectrum click → Counts vs Bin zoom
# ────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("bin-graph", "figure", allow_duplicate=True),
    Input("spectrum", "clickData"),
    State("current-counts",     "data"),
    State("current-row-counts", "data"),
    State("current-source",     "data"),
    State("filter-opts",        "value"),
    prevent_initial_call=True,
)
def update_bin_on_spectrum_click(click_data, counts_json, row_counts_json,
                                 source, filter_opts):
    if counts_json is None:
        return _empty_fig()

    apply_cr  = "cr" in (filter_opts or [])
    apply_bg  = "bg" in (filter_opts or [])
    C_sum_raw = np.array(counts_json)
    C_row_raw = np.array(row_counts_json) if row_counts_json else C_sum_raw
    n_ch      = len(C_sum_raw)

    filt_row = apply_spectral_filters(C_row_raw, apply_cr=apply_cr, apply_bg=apply_bg)
    filt_sum = apply_spectral_filters(C_sum_raw, apply_cr=apply_cr, apply_bg=apply_bg)
    C_row    = filt_row["net"]
    C_sum    = filt_sum["net"]

    if click_data and "points" in click_data and len(click_data["points"]) > 0:
        clicked_E_keV = click_data["points"][0]["x"]
        clicked_E_MeV = clicked_E_keV / 1000.0
        clicked_ch    = int(np.clip((clicked_E_MeV - 0.01) / 0.0025, 0, n_ch - 1))
        window = 40
        lo     = max(0, clicked_ch - window)
        hi     = min(n_ch - 1, clicked_ch + window)
        lo_kev = channel_to_energy(lo, n_channels=n_ch) * 1000
        hi_kev = channel_to_energy(hi, n_channels=n_ch) * 1000
        title_str = (f"Counts vs Bin  ch {lo}–{hi}  "
                     f"(~{lo_kev:.0f}–{hi_kev:.0f} keV)")
        C_plot   = C_row[lo: hi + 1]
        C_s_plot = C_sum[lo: hi + 1] if source == "tab" else None
        return _make_bin_fig(C_plot, title_str, source=source,
                             C_sum=C_s_plot.tolist() if C_s_plot is not None else None,
                             n_channels=n_ch)
    else:
        return _make_bin_fig(C_row, "Counts vs Bin  (full spectrum)", source=source,
                             C_sum=C_sum.tolist() if source == "tab" else None,
                             n_channels=n_ch)


# ────────────────────────────────────────────────────────────────────────────
# CALLBACK 6 — file-picker → load spectrum directly
# ────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("spectrum",           "figure",   allow_duplicate=True),
    Output("bin-graph",          "figure",   allow_duplicate=True),
    Output("current-file",       "data",     allow_duplicate=True),
    Output("current-counts",     "data",     allow_duplicate=True),
    Output("current-row-counts", "data",     allow_duplicate=True),
    Output("current-source",     "data",     allow_duplicate=True),
    Output("spectrum-label",     "children", allow_duplicate=True),
    Output("bin-label",          "children", allow_duplicate=True),
    Output("filter-status",      "children", allow_duplicate=True),
    Input("file-picker", "value"),
    State("filter-opts", "value"),
    prevent_initial_call=True,
)
def load_file_from_picker(filepath, filter_opts):
    no_data = (_empty_fig(), _empty_fig(), None, None, None, None,
               "Click the globe or select a file to load a spectrum",
               "Awaiting spectrum selection", "")
    if not filepath:
        return no_data

    spec = load_spectrum(filepath)
    if spec is None:
        return no_data

    apply_cr = "cr" in (filter_opts or [])
    apply_bg = "bg" in (filter_opts or [])
    fname    = Path(filepath).name
    ext      = Path(filepath).suffix.lower()
    C_sum    = np.sum(spec["counts"], axis=0)

    if ext == ".tab" and spec["counts"].shape[0] > 0:
        C_row      = spec["counts"][0]
        src        = "tab"
        utc0       = spec["utc"][0] if spec["utc"] else ""
        spec_title = f"GRaND  {fname}  —  row 0  {utc0[:19]}"
        bin_title  = f"Counts vs Bin  —  {fname}  row 0"
    else:
        C_row      = C_sum
        src        = "dat"
        spec_title = f"GRaND Spectrum  —  {fname}"
        bin_title  = f"Counts vs Bin  —  {fname}"

    spec_fig, filt = _make_spectrum_fig(
        C_row, spec_title, apply_cr=apply_cr, apply_bg=apply_bg
    )
    bin_fig = _make_bin_fig(filt["net"], bin_title, source=src,
                            C_sum=C_sum.tolist() if src == "tab" else None,
                            n_channels=spec["n_channels"])
    status  = _filter_status_str(filt, apply_cr, apply_bg)

    return (spec_fig, bin_fig,
            filepath, C_sum.tolist(), C_row.tolist(), src,
            f"ENERGY SPECTRUM  —  {fname}",
            f"COUNTS vs BIN  —  {fname}", status)


# ────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    PORT = 8050
    threading.Timer(1.5, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}")).start()
    app.run(debug=True, port=PORT)
