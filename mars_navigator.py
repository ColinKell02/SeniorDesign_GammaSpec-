import io
import base64
import struct
from pathlib import Path

import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
from PIL import Image

pio.renderers.default = "browser"

GALE_CRATER_IMG = "/Users/lucasturner/Desktop/GaleCrater.png"


# ─────────────────────────────────────────────
# PDS3 / binary parsing
# ─────────────────────────────────────────────

def parse_record_bytes(lbl_path):
    """Return RECORD_BYTES value from a PDS3 label file."""
    with lbl_path.open("r") as f:
        for line in f:
            if line.strip().upper().startswith("RECORD_BYTES"):
                return int(line.split("=", 1)[1].strip())
    raise RuntimeError(f"RECORD_BYTES not found in {lbl_path}")


def read_fmt_columns(fmt_path):
    """Parse column definitions from a PDS-style .FMT file."""
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
                val = val.strip('"')
                current[key.upper()] = val

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
    """Return (struct_fmt, n_items, total_bytes) for a column definition."""
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
    """Decode a DAN binary table and return a dict of numpy arrays."""
    blob = dat_path.read_bytes()
    n_records = len(blob) // record_bytes

    for col in columns:
        col["_fmt"], col["_n"], col["_tb"] = _col_struct(col)

    out = {col["NAME"]: [] for col in columns}

    for i in range(n_records):
        rec = blob[i * record_bytes : (i + 1) * record_bytes]
        for col in columns:
            offset = col["START_BYTE"] - 1
            vals = struct.unpack(col["_fmt"], rec[offset : offset + col["_tb"]])
            if "CHARACTER" in col.get("DATA_TYPE", "").upper():
                out[col["NAME"]].append(vals[0])
            elif col["_n"] == 1:
                out[col["NAME"]].append(vals[0])
            else:
                out[col["NAME"]].append(np.array(vals))

    for name, arr in out.items():
        out[name] = np.stack(arr) if isinstance(arr[0], np.ndarray) else np.array(arr)

    return out


# ─────────────────────────────────────────────
# Data extraction helpers
# ─────────────────────────────────────────────

def extract_lat_lon(table):
    """Return (lat, lon) arrays from a decoded DAN table."""
    cols = list(table.keys())
    if "BEGIN_LATITUDE" in cols and "BEGIN_LONGITUDE" in cols:
        lat = (table["BEGIN_LATITUDE"] + table.get("END_LATITUDE", table["BEGIN_LATITUDE"])) / 2
        lon = (table["BEGIN_LONGITUDE"] + table.get("END_LONGITUDE", table["BEGIN_LONGITUDE"])) / 2
        return lat, lon
    lat_col = next((c for c in cols if "LAT" in c.upper()), None)
    lon_col = next((c for c in cols if "LON" in c.upper()), None)
    if lat_col and lon_col:
        return table[lat_col], table[lon_col]
    raise ValueError("No latitude/longitude columns found")


def extract_ctn_cetn(table):
    """Return (ctn_counts, cetn_counts) summed arrays from a decoded DAN table."""
    cols = list(table.keys())
    ctn_col  = next((c for c in cols if "CTN"  in c.upper() and "BKGD" not in c.upper() and "CETN" not in c.upper()), None)
    cetn_col = next((c for c in cols if "CETN" in c.upper() and "BKGD" not in c.upper()), None)
    if not ctn_col or not cetn_col:
        raise ValueError("CTN/CETN columns not found")

    def _sum(arr):
        return arr.sum(axis=1) if arr.ndim > 1 else arr

    return _sum(table[ctn_col]), _sum(table[cetn_col])


# ___________
#  Load Data
# ___________

def load_dan_files(dat_files):
    """
    Load and combine data from a list of DAN .dat files.
    Returns a dict: lat, lon, ctn, cetn, file_idx (all numpy arrays).
    """
    all_lat, all_lon, all_ctn, all_cetn, all_idx = [], [], [], [], []

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

            valid = (np.isfinite(lat) & np.isfinite(lon) &
                     np.isfinite(ctn)  & np.isfinite(cetn) &
                     (lat >= -90) & (lat <= 90) &
                     (lon >= -180) & (lon <= 360))

            n = valid.sum()
            print(f"    {n} valid points")
            if n == 0:
                continue

            all_lat.append(lat[valid])
            all_lon.append(lon[valid])
            all_ctn.append(ctn[valid])
            all_cetn.append(cetn[valid])
            all_idx.append(np.full(n, file_idx))

        except Exception as e:
            print(f"  Error loading {dat_path.name}: {e}")

    if not all_lat:
        raise ValueError("No valid data loaded")

    return {
        "lat":      np.concatenate(all_lat),
        "lon":      np.concatenate(all_lon),
        "ctn":      np.concatenate(all_ctn),
        "cetn":     np.concatenate(all_cetn),
        "file_idx": np.concatenate(all_idx),
    }


# ________________
#    Plotting
# ________________


def build_figure(data, dat_files):
    """
    Build and return a three-panel Plotly figure:
      Panel 1 – Rover path (coloured by file)
      Panel 2 – CTN/CETN ratio
      Panel 3 – log10 total counts
    GaleCrater.png is placed as the background in all three panels.
    """
    lat      = data["lat"]
    lon      = data["lon"]
    ctn      = data["ctn"]
    cetn     = data["cetn"]
    file_idx = data["file_idx"]

    ratio  = ctn / (cetn + 1.0)
    total  = ctn + cetn
    names  = [Path(f).stem for f in dat_files]

    hover = [
        f"File: {names[int(file_idx[i])]}<br>"
        f"Lat: {lat[i]:.4f}°  Lon: {lon[i]:.4f}°<br>"
        f"CTN: {ctn[i]:.1f}  CETN: {cetn[i]:.1f}<br>"
        f"Ratio: {ratio[i]:.3f}  Total: {total[i]:.1f}"
        for i in range(len(lat))
    ]

    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=("Rover Path (by file)", "CTN/CETN Ratio", "Total Counts (log10)"),
        specs=[[{"type": "scattergl"}] * 3],
        column_widths=[0.4, 0.3, 0.3],
    )

    # Panel 1 – path coloured by file index
    fig.add_trace(go.Scattergl(
        x=lon, y=lat,
        mode="lines+markers",
        line=dict(color="rgba(100,100,255,0.3)", width=1),
        marker=dict(
            size=5, color=file_idx, colorscale="Viridis", showscale=False,
            colorbar=dict(title="File", x=0.35,
                          tickvals=list(range(len(dat_files))),
                          ticktext=[f"F{i+1}" for i in range(len(dat_files))]),
        ),
        text=hover, hoverinfo="text", name="Path",
    ), row=1, col=1)

    # Panel 2 – CTN/CETN ratio
    fig.add_trace(go.Scattergl(
        x=lon, y=lat,
        mode="markers",
        marker=dict(
            size=5, color=ratio, colorscale="RdYlBu_r", showscale=True,
            cmin=0, cmax=float(np.percentile(ratio, 95)),
            colorbar=dict(title="CTN/CETN", x=0.67),
        ),
        text=hover, hoverinfo="text", name="Ratio",
    ), row=1, col=2)

    # Panel 3 – log10 total counts
    fig.add_trace(go.Scattergl(
        x=lon, y=lat,
        mode="markers",
        marker=dict(
            size=5, color=np.log10(total + 1), colorscale="Viridis", showscale=True,
            colorbar=dict(title="log10(Total)", x=1.0),
        ),
        text=hover, hoverinfo="text", name="Total",
    ), row=1, col=3)

    for col in [1, 2, 3]:
        fig.update_xaxes(title_text="Longitude (°)", row=1, col=col)
        fig.update_yaxes(title_text="Latitude (°)",  row=1, col=col)

    fig.update_layout(
        title=f"Mars Curiosity DAN – {len(dat_files)} file(s), {len(lat)} points",
        height=620, width=1800, showlegend=False, hovermode="closest",
    )

    # Basemap: stretch GaleCrater.png behind all three panels
    pad   = 0.15
    lat_p = max((lat.max() - lat.min()) * pad, 0.05)
    lon_p = max((lon.max() - lon.min()) * pad, 0.05)
    lat_min, lat_max = lat.min() - lat_p, lat.max() + lat_p
    lon_min, lon_max = lon.min() - lon_p, lon.max() + lon_p

    buf = io.BytesIO()
    Image.open(GALE_CRATER_IMG).save(buf, format="PNG")
    img_b64 = "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()

    for col in [1, 2, 3]:
        fig.add_layout_image(dict(
            source=img_b64,
            xref=f"x{col if col > 1 else ''}",
            yref=f"y{col if col > 1 else ''}",
            x=lon_min, y=lat_max,
            sizex=lon_max - lon_min,
            sizey=lat_max - lat_min,
            sizing="stretch", opacity=1.0, layer="below",
        ), row=1, col=col)

    return fig


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def run():
    print("\n" + "=" * 60)
    print("MARS CURIOSITY DAN INTERACTIVE PATH VIEWER")
    print("=" * 60)

    mars_dir = Path("Mars") / "data"
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

    print("Building figure...")
    fig = build_figure(data, selected)
    fig.show()


if __name__ == "__main__":
    run()
