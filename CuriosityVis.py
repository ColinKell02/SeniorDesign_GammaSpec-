"""
Mars Curiosity DAN Interactive Path Viewer  (improved layout)

Three panels in a single browser window:
  Top-left  – CTN/CETN thermal-to-epithermal ratio map
               (compact dropdown contrast selector; GaleCrater.png basemap)
  Top-right – Count-rate time series vs. Mars Sol
               • CTN  (thermal)    – left y-axis
               • CETN (epithermal) – left y-axis
               • CTN − CETN        – right / secondary y-axis (purple, dotted)
  Bottom    – Hydrogen / Neutron-Absorber Index strip (full width)
               Sol-ordered colour strip: CTN/CETN normalised to 0–100 H-Index
               Sandy/dry → green → blue (water-rich) geological colourscale

Changes from the original:
  - Contrast buttons replaced with a compact dropdown (much less header clutter)
  - Top margin reduced 130 → 100 px
  - 2-row subplot layout; row 2 is the H-Index strip
  - H-Index colourbar with geological tick labels
"""

import io
import base64
import struct
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
from PIL import Image

pio.renderers.default = "browser"

GALE_CRATER_IMG = "/Users/lucasturner/Desktop/GaleCrater2.png"

# ─────────────────────────────────────────────────────────────────────────────
# Mars Sol helper
# ─────────────────────────────────────────────────────────────────────────────

_MSL_LANDING_UTC  = datetime(2012, 8, 6, 5, 17, 57, tzinfo=timezone.utc)
_MARS_SOL_SECONDS = 88775.244          # Earth seconds per Mars sol


def utc_to_sol(dt):
    """Convert a UTC datetime to a Curiosity mission Sol (float)."""
    if dt is None:
        return np.nan
    return (dt - _MSL_LANDING_UTC).total_seconds() / _MARS_SOL_SECONDS




# ─────────────────────────────────────────────────────────────────────────────
# PDS3 / binary parsing
# ─────────────────────────────────────────────────────────────────────────────

def parse_record_bytes(lbl_path):
    with lbl_path.open("r") as f:
        for line in f:
            if line.strip().upper().startswith("RECORD_BYTES"):
                return int(line.split("=", 1)[1].strip())
    raise RuntimeError(f"RECORD_BYTES not found in {lbl_path}")


def read_fmt_columns(fmt_path):
    columns, current = [], None
    with fmt_path.open("r") as f:
        for raw in f:
            line = raw.strip()
            if line.upper().startswith("OBJECT") and "COLUMN" in line.upper():
                current = {}
            elif line.upper().startswith("END_OBJECT") and "COLUMN" in line.upper():
                if current is not None:
                    columns.append(current)
                current = None
            elif current is not None and "=" in line:
                key, val = [s.strip() for s in line.split("=", 1)]
                current[key.upper()] = val.strip('"')

    for col in columns:
        for k in ("START_BYTE", "BYTES", "ITEMS", "ITEM_BYTES"):
            if k in col:
                try:
                    col[k] = int(col[k])
                except ValueError:
                    pass
        col.setdefault("ITEMS", 1)

    columns.sort(key=lambda c: c.get("START_BYTE", 0))
    return columns


def _col_struct(col):
    dtype      = col.get("DATA_TYPE", "").upper()
    total      = int(col.get("BYTES", 0))
    n          = int(col.get("ITEMS", 1))
    item_bytes = col.get("ITEM_BYTES") or total // max(n, 1)

    if "MSB_UNSIGNED_INTEGER" in dtype:
        code = {1: "B", 2: "H", 4: "I"}[item_bytes]
    elif "MSB_INTEGER" in dtype:
        code = {1: "b", 2: "h", 4: "i"}[item_bytes]
    elif "IEEE_REAL" in dtype:
        code = {4: "f", 8: "d"}[item_bytes]
    else:
        return f">{total}s", 1, total

    return f">{n}{code}", n, total


def decode_dan_table(dat_path, columns, record_bytes):
    blob      = dat_path.read_bytes()
    n_records = len(blob) // record_bytes

    for col in columns:
        col["_fmt"], col["_n"], col["_tb"] = _col_struct(col)

    out = {col["NAME"]: [] for col in columns}

    for i in range(n_records):
        rec = blob[i * record_bytes : (i + 1) * record_bytes]
        for col in columns:
            offset = col["START_BYTE"] - 1
            vals   = struct.unpack(col["_fmt"], rec[offset : offset + col["_tb"]])
            if "CHARACTER" in col.get("DATA_TYPE", "").upper():
                out[col["NAME"]].append(vals[0])
            elif col["_n"] == 1:
                out[col["NAME"]].append(vals[0])
            else:
                out[col["NAME"]].append(np.array(vals))

    for name, arr in out.items():
        out[name] = np.stack(arr) if isinstance(arr[0], np.ndarray) else np.array(arr)

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Timestamp extraction
# ─────────────────────────────────────────────────────────────────────────────

_TIME_COL_CANDIDATES = ["START_TIME", "BEGIN_UTC", "SPACECRAFT_CLOCK_START_COUNT", "UTC", "TIME"]


def _parse_pds_utc(val):
    if isinstance(val, (bytes, bytearray)):
        val = val.decode("ascii", errors="replace")
    val = val.strip()
    for fmt in (
        "%Y-%jT%H:%M:%S.%f", "%Y-%jT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(val, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def extract_timestamps(table):
    cols         = list(table.keys())
    search_order = list(_TIME_COL_CANDIDATES)
    for c in cols:
        cu = c.upper()
        if ("TIME" in cu or "UTC" in cu) and c not in search_order:
            search_order.append(c)

    for name in search_order:
        if name not in table:
            continue
        arr = table[name]
        if arr.dtype.kind not in ("S", "U", "O"):
            continue
        if _parse_pds_utc(arr[0]) is None:
            continue
        times = np.array([_parse_pds_utc(v) for v in arr])
        if np.any(times == None):   # noqa: E711
            continue
        print(f"    Using timestamp column: {name}")
        return times

    print("    Warning: no parseable UTC timestamp column found; date filtering disabled for this file")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Data extraction helpers
# ─────────────────────────────────────────────────────────────────────────────

def extract_lat_lon(table):
    cols = list(table.keys())
    if "BEGIN_LATITUDE" in cols and "BEGIN_LONGITUDE" in cols:
        lat = (table["BEGIN_LATITUDE"] + table.get("END_LATITUDE",  table["BEGIN_LATITUDE"]))  / 2
        lon = (table["BEGIN_LONGITUDE"] + table.get("END_LONGITUDE", table["BEGIN_LONGITUDE"])) / 2
        return lat, lon
    lat_col = next((c for c in cols if "LAT" in c.upper()), None)
    lon_col = next((c for c in cols if "LON" in c.upper()), None)
    if lat_col and lon_col:
        return table[lat_col], table[lon_col]
    raise ValueError("No latitude/longitude columns found")


def extract_ctn_cetn(table):
    cols     = list(table.keys())
    ctn_col  = next((c for c in cols if "CTN"  in c.upper() and "BKGD" not in c.upper() and "CETN" not in c.upper()), None)
    cetn_col = next((c for c in cols if "CETN" in c.upper() and "BKGD" not in c.upper()), None)
    if not ctn_col or not cetn_col:
        raise ValueError("CTN/CETN columns not found")

    def _sum(arr):
        return arr.sum(axis=1) if arr.ndim > 1 else arr

    return _sum(table[ctn_col]), _sum(table[cetn_col])


# ─────────────────────────────────────────────────────────────────────────────
# Multi-file loader
# ─────────────────────────────────────────────────────────────────────────────

def load_dan_files(dat_files):
    all_lat, all_lon, all_ctn, all_cetn, all_idx, all_times = [], [], [], [], [], []

    for file_idx, dat_path in enumerate(dat_files):
        dat_path = Path(dat_path)
        lbl_path = dat_path.with_suffix(".lbl")
        if not lbl_path.exists():
            lbl_path = dat_path.with_suffix(".LBL")

        fmt_path = dat_path.parent / "dan_rdr_derived_activ_mod.fmt"
        if not fmt_path.exists():
            fmt_path = dat_path.parent.parent / "dan_rdr_derived_activ_mod.fmt"

        if not lbl_path.exists() or not fmt_path.exists():
            print(f"  Skipping {dat_path.name}: missing .lbl or .fmt file")
            continue

        print(f"  Loading {dat_path.name}")
        try:
            record_bytes = parse_record_bytes(lbl_path)
            columns      = read_fmt_columns(fmt_path)
            table        = decode_dan_table(dat_path, columns, record_bytes)
            lat, lon     = extract_lat_lon(table)
            ctn, cetn    = extract_ctn_cetn(table)
            timestamps   = extract_timestamps(table)

            valid = (np.isfinite(lat) & np.isfinite(lon) &
                     np.isfinite(ctn)  & np.isfinite(cetn) &
                     (lat >= -90) & (lat <= 90) & (lon >= -180) & (lon <= 360))

            n = valid.sum()
            print(f"    {n} valid points")
            if n == 0:
                continue

            all_lat.append(lat[valid])
            all_lon.append(lon[valid])
            all_ctn.append(ctn[valid])
            all_cetn.append(cetn[valid])
            all_idx.append(np.full(n, file_idx))
            all_times.append(timestamps[valid] if timestamps is not None else np.full(n, None))

        except Exception as e:
            print(f"  Error loading {dat_path.name}: {e}")

    if not all_lat:
        raise ValueError("No valid data loaded")

    return {
        "lat":        np.concatenate(all_lat),
        "lon":        np.concatenate(all_lon),
        "ctn":        np.concatenate(all_ctn),
        "cetn":       np.concatenate(all_cetn),
        "file_idx":   np.concatenate(all_idx),
        "timestamps": np.concatenate(all_times),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Date-range filtering
# ─────────────────────────────────────────────────────────────────────────────

def _date_input(prompt, default=None):
    while True:
        raw = input(prompt).strip()
        if raw == "" and default is not None:
            return default
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        print("  Unrecognised format — use YYYY-MM-DD")


def apply_date_filter(data):
    ts       = data["timestamps"]
    has_time = np.array([t is not None for t in ts])

    if not has_time.any():
        print("\nNo timestamp data — date filtering skipped.")
        return data, None, None

    valid_ts = np.array([t for t in ts if t is not None])
    t_min, t_max = min(valid_ts), max(valid_ts)

    print(f"\nTimestamp range:  {t_min:%Y-%m-%d %H:%M:%S UTC}  →  {t_max:%Y-%m-%d %H:%M:%S UTC}")
    print("Enter a date range (press Enter for full range).")

    start_dt = _date_input(f"  Start date [default {t_min:%Y-%m-%d}]: ", default=t_min)
    end_dt   = _date_input(f"  End date   [default {t_max:%Y-%m-%d}]: ", default=t_max)
    end_dt   = end_dt.replace(hour=23, minute=59, second=59)

    mask   = np.array([(t is not None and start_dt <= t <= end_dt) for t in ts])
    n_kept = mask.sum()
    print(f"\n  {n_kept} / {len(ts)} points in {start_dt:%Y-%m-%d} → {end_dt:%Y-%m-%d}")

    if n_kept == 0:
        print("  WARNING: no points in range — returning unfiltered data.")
        return data, start_dt, end_dt

    return {k: v[mask] for k, v in data.items()}, start_dt, end_dt


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint display
# ─────────────────────────────────────────────────────────────────────────────

def print_path_endpoints(data):
    lat, lon, ts = data["lat"], data["lon"], data["timestamps"]
    _f = lambda t: t.strftime("%Y-%m-%d %H:%M:%S UTC") if t is not None else "n/a"
    print("\n" + "─" * 50)
    print("PATH ENDPOINTS")
    print("─" * 50)
    print(f"  FIRST →  lat {lat[0]:.6f}°  lon {lon[0]:.6f}°  {_f(ts[0])}")
    print(f"  LAST  →  lat {lat[-1]:.6f}°  lon {lon[-1]:.6f}°  {_f(ts[-1])}")
    print(f"  Lat range: {lat.min():.6f}° → {lat.max():.6f}°  (span {lat.max()-lat.min():.6f}°)")
    print(f"  Lon range: {lon.min():.6f}° → {lon.max():.6f}°  (span {lon.max()-lon.min():.6f}°)")
    print("─" * 50)


# ─────────────────────────────────────────────────────────────────────────────
# Hydrogen Index helper
# ─────────────────────────────────────────────────────────────────────────────

# Geological colourscale: dry regolith → hydrated minerals → water-rich
H_COLORSCALE = [
    [0.00, "#c4a26a"],   #   0 – Dry regolith / low-H rock
    [0.20, "#e8c840"],   #  20 – Slightly hydrated
    [0.40, "#70b860"],   #  40 – Hydrated silicates / clays
    [0.60, "#38a8c0"],   #  60 – Moderately water-bearing
    [0.80, "#1e60d0"],   #  80 – Highly hydrated / brines
    [1.00, "#0a1580"],   # 100 – Water-rich / near-surface ice
]

H_TICKVALS  = [0, 20, 40, 60, 80, 100]
H_TICKTEXT  = ["Dry rock", "Slightly hyd.", "Hyd. minerals", "Mod. water-bearing", "Highly hydrated", "Water-rich"]


def ratio_to_hindex(ratio, p_lo=2, p_hi=98):
    """Normalise CTN/CETN ratio to 0–100 Hydrogen Index using percentile clipping."""
    r_lo = float(np.percentile(ratio, p_lo))
    r_hi = float(np.percentile(ratio, p_hi))
    hi   = np.clip((ratio - r_lo) / (r_hi - r_lo + 1e-9), 0.0, 1.0) * 100.0
    return hi, r_lo, r_hi




# ─────────────────────────────────────────────────────────────────────────────
# Single combined figure
# ─────────────────────────────────────────────────────────────────────────────

def build_figure(data, dat_files, date_range=None):
    """
    Two-row Plotly figure:

    Row 1 (75 %):
      col 1 – CTN/CETN ratio map  (compact dropdown contrast selector; GaleCrater basemap)
      col 2 – Count-rate time series (CTN / CETN primary y; CTN−CETN secondary y)

    Row 2 (25 %):
      colspan 2 – Hydrogen / Neutron-Absorber Index strip
                  Sol-ordered squares coloured by normalised CTN/CETN ratio (0–100 H-Index)
                  Geological colourscale with labelled ticks.

    Trace order: [LogMap(0), ClipMap(1), DivMap(2), CTN(3), CETN(4), Diff(5), HIndex(6)]
    Visibility args for the dropdown toggle all 7 traces.
    """
    lat   = data["lat"]
    lon   = data["lon"]
    ctn   = data["ctn"]
    cetn  = data["cetn"]
    ts    = data["timestamps"]

    ratio = ctn / (cetn + 1.0)
    diff  = ctn - cetn
    sols  = np.array([utc_to_sol(t) for t in ts])

    # Sol-sorted indices for clean time-series / H-index strip
    sol_order = np.argsort(sols)
    sols_s    = sols[sol_order]
    ctn_s     = ctn[sol_order]
    cetn_s    = cetn[sol_order]
    diff_s    = diff[sol_order]
    ts_s      = ts[sol_order]
    ratio_s   = ratio[sol_order]

    # -------------
    # Bin by integer Sol
    sol_bins = np.floor(sols_s)

    unique_sols = np.unique(sol_bins)

    sol_binned = []
    ctn_binned = []
    cetn_binned = []
    diff_binned = []

    for s in unique_sols:
        mask = sol_bins == s
        sol_binned.append(s)
        ctn_binned.append(ctn_s[mask].mean())
        cetn_binned.append(cetn_s[mask].mean())
        diff_binned.append(diff_s[mask].mean())

    sols_s  = np.array(sol_binned)
    ctn_s   = np.array(ctn_binned)
    cetn_s  = np.array(cetn_binned)
    diff_s  = np.array(diff_binned)
    # --------------


    def _ft(t):
        return t.strftime("%Y-%m-%d %H:%M") if t is not None else "n/a"

    date_str = (f"  |  {date_range[0]:%Y-%m-%d} → {date_range[1]:%Y-%m-%d}"
                if date_range and date_range[0] and date_range[1] else "")

    # ── Subplot grid ──────────────────────────────────────────────────────────
    # Row 1: [ratio map | time series+secondary y]
    # Row 2: [H-index strip colspan=2           ]
    fig = make_subplots(
        rows=2, cols=2,
        row_heights=[0.75, 0.25],
        column_widths=[0.50, 0.50],
        horizontal_spacing=0.10,
        vertical_spacing=0.09,
        specs=[
            [{}, {"secondary_y": True}],
            [{"colspan": 2}, None],
        ],
        subplot_titles=(
            "CTN / CETN Ratio Map",
            "Count Rate vs. Mars Sol",
            "Hydrogen / Neutron-Absorber Index  —  CTN/CETN · Sol-ordered",
        ),
    )

    # ── Colorbar positioning ──────────────────────────────────────────────────
    # With row_heights [0.75, 0.25] + vertical_spacing 0.09:
    #   row 1 paper-y  ≈  0.34 → 1.00   (centre ≈ 0.67)
    #   row 2 paper-y  ≈  0.00 → 0.25   (centre ≈ 0.125)
    CB_X        = 0.455   # just right of col-1 right edge
    MAP_CB_Y    = 0.67
    MAP_CB_LEN  = 0.65

    def _cb(title):
        return dict(
            title=dict(text=title, font=dict(size=11, color="#cccccc")),
            thickness=14, len=MAP_CB_LEN,
            yanchor="middle", y=MAP_CB_Y,
            x=CB_X, xanchor="left",
            tickfont=dict(size=10, color="#cccccc"), outlinewidth=0,
        )

    # ── Ratio map – three contrast modes (traces 0, 1, 2) ────────────────────
    eps       = 1e-6
    ratio_log = np.log10(ratio + eps)

    hover_map = [
        f"Lat: {lat[i]:.4f}°  Lon: {lon[i]:.4f}°<br>"
        f"Sol: {sols[i]:.1f}  |  {_ft(ts[i])}<br>"
        f"CTN: {ctn[i]:.1f}   CETN: {cetn[i]:.1f}<br>"
        f"Ratio: {ratio[i]:.3f}"
        for i in range(len(lat))
    ]

    # Mode A – log scale (default)
    fig.add_trace(go.Scattergl(
        x=lon, y=lat, mode="markers",
        marker=dict(size=5, color=ratio_log, colorscale="RdYlBu_r", showscale=True,
                    cmin=float(np.percentile(ratio_log, 2)),
                    cmax=float(np.percentile(ratio_log, 98)),
                    colorbar=_cb("log₁₀(CTN/CETN)")),
        text=hover_map, hoverinfo="text", name="Ratio–Log", visible=True,
    ), row=1, col=1)

    # Mode B – percentile clip
    p05, p95   = float(np.percentile(ratio, 5)), float(np.percentile(ratio, 95))
    ratio_clip = np.clip(ratio, p05, p95)
    fig.add_trace(go.Scattergl(
        x=lon, y=lat, mode="markers",
        marker=dict(size=5, color=ratio_clip, colorscale="RdYlBu_r", showscale=True,
                    cmin=p05, cmax=p95,
                    colorbar=_cb("CTN/CETN (p5–p95)")),
        text=hover_map, hoverinfo="text", name="Ratio–Clip", visible=False,
    ), row=1, col=1)

    # Mode C – diverging / median-centred
    med        = float(np.median(ratio))
    half_range = float(max(abs(np.percentile(ratio, 98) - med),
                           abs(med - np.percentile(ratio, 2))))
    fig.add_trace(go.Scattergl(
        x=lon, y=lat, mode="markers",
        marker=dict(size=5, color=ratio, colorscale="RdBu_r", showscale=True,
                    cmin=med - half_range, cmax=med + half_range,
                    colorbar=_cb(f"CTN/CETN  med={med:.2f}")),
        text=hover_map, hoverinfo="text", name="Ratio–Div", visible=False,
    ), row=1, col=1)

    # ── Time-series traces (traces 3, 4, 5) ──────────────────────────────────
    hover_ctn  = [f"Sol: {sols_s[i]:.1f}<br>{_ft(ts_s[i])}<br>CTN: {ctn_s[i]:.1f}"      for i in range(len(sols_s))]
    hover_cetn = [f"Sol: {sols_s[i]:.1f}<br>{_ft(ts_s[i])}<br>CETN: {cetn_s[i]:.1f}"    for i in range(len(sols_s))]
    hover_diff = [f"Sol: {sols_s[i]:.1f}<br>{_ft(ts_s[i])}<br>CTN−CETN: {diff_s[i]:.1f}" for i in range(len(sols_s))]

    fig.add_trace(go.Scatter(
        x=sols_s, y=ctn_s,
        mode="lines+markers", line=dict(color="#e07b39", width=1.5),
        marker=dict(size=3, color="#e07b39"),
        name="CTN (thermal)", text=hover_ctn, hoverinfo="text",
    ), row=1, col=2, secondary_y=False)

    fig.add_trace(go.Scatter(
        x=sols_s, y=cetn_s,
        mode="lines+markers", line=dict(color="#4a90d9", width=1.5),
        marker=dict(size=3, color="#4a90d9"),
        name="CETN (epithermal)", text=hover_cetn, hoverinfo="text",
    ), row=1, col=2, secondary_y=False)

    fig.add_trace(go.Scatter(
        x=sols_s, y=diff_s,
        mode="lines", line=dict(color="#9b59b6", width=1.5, dash="dot"),
        name="CTN − CETN", text=hover_diff, hoverinfo="text",
    ), row=1, col=2, secondary_y=True)

    # ── Hydrogen / Absorber Index strip (trace 6, row 2 colspan) ─────────────
    # Normalise ratio → 0–100 H-Index using dataset percentile range
    hi_all, r_lo, r_hi = ratio_to_hindex(ratio)
    hi_s = ratio_to_hindex(ratio_s)[0]   # sol-ordered version (same bounds)

    hover_hi = [
        f"Sol: {sols_s[i]:.1f}  |  {_ft(ts_s[i])}<br>"
        f"H-Index: {hi_s[i]:.0f} / 100<br>"
        f"CTN/CETN: {ratio_s[i]:.3f}  (raw ratio)<br>"
        f"CTN: {ctn_s[i]:.1f}   CETN: {cetn_s[i]:.1f}"
        for i in range(len(sols_s))
    ]

    fig.add_trace(go.Scattergl(
        x=sols_s,
        y=np.zeros(len(sols_s)),   # constant y → strip appearance
        mode="markers",
        marker=dict(
            size=16,
            symbol="square",
            color=hi_s,
            colorscale=H_COLORSCALE,
            cmin=0, cmax=100,
            showscale=True,
            colorbar=dict(
                title=dict(text="H-Index", font=dict(size=11, color="#cccccc")),
                thickness=14, len=0.22,
                yanchor="middle", y=0.125,
                x=1.01, xanchor="left",
                tickvals=H_TICKVALS,
                ticktext=H_TICKTEXT,
                tickfont=dict(size=9, color="#cccccc"), outlinewidth=0,
            ),
        ),
        text=hover_hi, hoverinfo="text",
        name="H-Index",
        showlegend=False,
    ), row=2, col=1)

    # ── Visibility patterns (7 traces) ───────────────────────────────────────
    # [LogMap, ClipMap, DivMap, CTN, CETN, Diff, HIndex]
    vis_log  = [True,  False, False, True, True, True, True]
    vis_clip = [False, True,  False, True, True, True, True]
    vis_div  = [False, False, True,  True, True, True, True]

    # ── Axes: row 1 col 1 (ratio map) ────────────────────────────────────────
    fig.update_xaxes(title_text="Longitude (°)", title_font=dict(color="#aaaaaa"),
                     showgrid=False, zeroline=False,
                     showline=True, linecolor="#444444", mirror=True,
                     tickfont=dict(color="#aaaaaa"), row=1, col=1)
    fig.update_yaxes(title_text="Latitude (°)", title_font=dict(color="#aaaaaa"),
                     showgrid=False, zeroline=False,
                     showline=True, linecolor="#444444", mirror=True,
                     tickfont=dict(color="#aaaaaa"), row=1, col=1)

    # ── Axes: row 1 col 2 primary ─────────────────────────────────────────────
    fig.update_xaxes(title_text="Mars Sol", title_font=dict(color="#aaaaaa"),
                     showgrid=True, gridcolor="#2a2a2a",
                     zeroline=False, showline=True, linecolor="#444444",
                     tickfont=dict(color="#aaaaaa"), row=1, col=2)
    fig.update_yaxes(title_text="Count Rate", title_font=dict(color="#aaaaaa"),
                     showgrid=True, gridcolor="#2a2a2a",
                     zeroline=False, showline=True, linecolor="#444444",
                     tickfont=dict(color="#aaaaaa"),
                     secondary_y=False, row=1, col=2)

    # ── Axes: row 1 col 2 secondary ───────────────────────────────────────────
    fig.update_yaxes(title_text="CTN − CETN", showgrid=False,
                     zeroline=True, zerolinecolor="#5a3a7a",
                     showline=True, linecolor="#9b59b6",
                     tickfont=dict(color="#9b59b6"),
                     title_font=dict(color="#9b59b6"),
                     secondary_y=True, row=1, col=2)

    # ── Axes: row 2 H-index strip ─────────────────────────────────────────────
    fig.update_xaxes(title_text="Mars Sol", title_font=dict(color="#aaaaaa"),
                     showgrid=True, gridcolor="#2a2a2a",
                     zeroline=False, showline=True, linecolor="#444444",
                     tickfont=dict(color="#aaaaaa"), row=2, col=1)
    fig.update_yaxes(visible=False, row=2, col=1)

    # ── Subplot title font ────────────────────────────────────────────────────
    for ann in fig.layout.annotations:
        ann.update(font=dict(size=12, color="#cccccc"), yshift=4)

    # ── Overall layout ────────────────────────────────────────────────────────
    fig.update_layout(
        title=dict(
            text=(f"<b>Mars Curiosity DAN</b>"
                  f"<br><span style='font-size:12px;color:#888'>"
                  f"{len(dat_files)} file(s) · {len(lat):,} points{date_str}</span>"),
            x=0.01, xanchor="left", font=dict(size=16, color="#eeeeee"),
        ),
        height=860, width=1600,
        hovermode="closest",
        paper_bgcolor="#111111",
        plot_bgcolor="#111111",
        # Reduced top margin — compact dropdown takes far less vertical space than 3 buttons
        margin=dict(t=100, b=60, l=60, r=150),

        legend=dict(
            x=0.535, y=0.97,
            xanchor="left", yanchor="top",
            bgcolor="rgba(20,20,20,0.85)",
            bordercolor="#444444", borderwidth=1,
            font=dict(size=11, color="#cccccc"),
        ),

        # ── Contrast selector: compact dropdown (replaces three wide buttons) ─
        # Positioned just above the right edge of col 1 so it never overlaps
        # the main title, the subplot title, or col 2 content.
        updatemenus=[dict(
            type="dropdown",
            direction="down",
            xanchor="right", x=0.44,   # right edge of col 1
            yanchor="bottom", y=1.01,
            showactive=True,
            bgcolor="#1e1e1e", bordercolor="#555555", borderwidth=1,
            font=dict(size=11, color="#cccccc"),
            buttons=[
                dict(label="Log scale",
                     method="update", args=[{"visible": vis_log}]),
                dict(label="Percentile clip (p5–p95)",
                     method="update", args=[{"visible": vis_clip}]),
                dict(label="Diverging (median-centred)",
                     method="update", args=[{"visible": vis_div}]),
            ],
        )],

        annotations=list(fig.layout.annotations) + [
            # Small label for the dropdown
            dict(text="<b>Contrast:</b>",
                 x=0.28, xanchor="right", y=1.055, yanchor="bottom",
                 xref="paper", yref="paper", showarrow=False,
                 font=dict(size=11, color="#aaaaaa")),
            # Interpretive label beneath the H-index strip
            dict(
                text=(f"<i>Dry/low-H  ←  {r_lo:.3f}  ·  CTN/CETN ratio range  ·  "
                      f"{r_hi:.3f}  →  water-rich/high-H</i>"),
                x=0.50, xanchor="center",
                y=-0.04, yanchor="top",
                xref="paper", yref="paper", showarrow=False,
                font=dict(size=10, color="#888888"),
            ),
        ],
    )

    # ── GaleCrater basemap (col 1 only — xref="x" / yref="y") ───────────────
    pad   = 0.15
    lat_p = max((lat.max() - lat.min()) * pad, 0.05)
    lon_p = max((lon.max() - lon.min()) * pad, 0.05)
    lat_min, lat_max = lat.min() - lat_p, lat.max() + lat_p
    lon_min, lon_max = lon.min() - lon_p, lon.max() + lon_p

    buf = io.BytesIO()
    Image.open(GALE_CRATER_IMG).save(buf, format="PNG")
    img_b64 = "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()

    fig.add_layout_image(dict(
        source=img_b64,
        xref="x", yref="y",
        x=lon_min, y=lat_max,
        sizex=lon_max - lon_min,
        sizey=lat_max - lat_min,
        sizing="stretch", opacity=1.0, layer="below",
    ))

    return fig


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def run():
    print("\n" + "=" * 60)
    print("MARS CURIOSITY DAN INTERACTIVE PATH VIEWER")
    print("=" * 60)

    mars_dir = Path("NASA_Data/Mars") / "data"
    if not mars_dir.exists():
        print(f"ERROR: Data directory not found: {mars_dir}")
        return

    dat_files = sorted(f for f in mars_dir.iterdir() if f.suffix.upper() == ".DAT")
    if not dat_files:
        print(f"No .dat files found in {mars_dir}")
        return

    print(f"\nFound {len(dat_files)} .dat file(s):")
    for i, f in enumerate(dat_files):
        print(f"  {i + 1}: {f.name}")

    print("\nSelect files to load:")
    print("  all     – load everything")
    print("  1,3,5   – comma-separated list")
    print("  2-6     – inclusive range")
    print("  4       – single file")

    selection = input("\nYour selection: ").strip().lower()

    if selection == "all":
        selected = dat_files
    else:
        indices = set()
        for part in selection.split(","):
            part = part.strip()
            if "-" in part:
                a, b = part.split("-", 1)
                indices.update(range(int(a) - 1, int(b)))
            else:
                indices.add(int(part) - 1)
        selected = [dat_files[i] for i in sorted(indices) if 0 <= i < len(dat_files)]

    if not selected:
        print("No valid files selected.")
        return

    print(f"\nLoading {len(selected)} file(s)...")
    data = load_dan_files(selected)

    data, date_start, date_end = apply_date_filter(data)
    print_path_endpoints(data)

    print("\nBuilding figure...")
    fig = build_figure(data, selected, date_range=(date_start, date_end))
    fig.show()


if __name__ == "__main__":
    run()
