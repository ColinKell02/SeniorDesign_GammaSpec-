
from __future__ import annotations

import os
import json
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from dash import Dash, dcc, html, Input, Output, State, ctx, dash_table
import plotly.graph_objects as go
from plotly.subplots import make_subplots

PARQUET_PATH = r"C:\Users\davis\Downloads\2024-08-21\RUN-2024-08-21-00013\parquet-data\RUN-2024-08-21-00013-00001-pandas.parquet"

ENERGY_MIN = 0.0
ENERGY_MAX = 13.0

USE_SAMPLE_ROI = True
ROI = {
    "x_min": -50.0,
    "x_max": 50.0,
    "y_min": -50.0,
    "y_max": 50.0,
    "z_min": -2.0,
    "z_max": 0.0,
}

DEFAULT_PEAK_HALF_WIDTH = 0.03
DEFAULT_R = 255
DEFAULT_G = 0
DEFAULT_B = 0
DEFAULT_OPACITY = 0.65

PEAK_VRECT_ALPHA = 0.30
PEAK_LINE_ALPHA = 0.95
BACKGROUND_ALPHA_2D = 0.12
BACKGROUND_ALPHA_3D = 0.01

MAX_BACKGROUND_POINTS_2D = 120_000
MAX_BACKGROUND_POINTS_3D = 12_000
MAX_SELECTED_POINTS_PER_PEAK_2D = 40_000
MAX_SELECTED_POINTS_PER_PEAK_3D = 8_000

SPECTRUM_BINS = 1600

FORCE_COLUMNS = {
    "energy": None,
    "x": None,
    "y": None,
    "z": None,
}

SLIDER_STYLE = {
    "included": True,
}

def sanitize_rgb(r: float, g: float, b: float) -> Tuple[int, int, int]:
    return (
        int(np.clip(round(r), 0, 255)),
        int(np.clip(round(g), 0, 255)),
        int(np.clip(round(b), 0, 255)),
    )


def sanitize_opacity(alpha: float) -> float:
    return float(np.clip(alpha, 0.0, 1.0))


def rgba_tuple_to_css(rgb: Tuple[int, int, int], alpha: float = 0.45) -> str:
    r, g, b = rgb
    r = int(np.clip(r, 0, 255))
    g = int(np.clip(g, 0, 255))
    b = int(np.clip(b, 0, 255))
    alpha = float(np.clip(alpha, 0.0, 1.0))
    return f"rgba({r},{g},{b},{alpha})"


def rgb_tuple_to_css(rgb: Tuple[int, int, int]) -> str:
    r, g, b = rgb
    r = int(np.clip(r, 0, 255))
    g = int(np.clip(g, 0, 255))
    b = int(np.clip(b, 0, 255))
    return f"rgb({r},{g},{b})"


def deterministic_downsample(df: pd.DataFrame, nmax: int) -> pd.DataFrame:
    if len(df) <= nmax:
        return df
    idx = np.linspace(0, len(df) - 1, nmax, dtype=int)
    return df.iloc[idx]


def find_column(df: pd.DataFrame, forced: Optional[str], candidates: List[str], kind: str) -> str:
    if forced is not None:
        if forced not in df.columns:
            raise KeyError(f"Forced {kind} column '{forced}' not found.")
        return forced

    cols_lower = {c.lower(): c for c in df.columns}

    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]

    for c in df.columns:
        cl = c.lower()
        for cand in candidates:
            if cand.lower() in cl:
                return c

    raise KeyError(
        f"Could not auto-detect {kind} column.\n"
        f"Available columns: {list(df.columns)}\n"
        f"Set FORCE_COLUMNS['{kind}'] manually."
    )


def load_and_prepare_dataframe(parquet_path: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet not found:\n{parquet_path}")

    print(f"Loading parquet: {parquet_path}")
    df = pd.read_parquet(parquet_path)

    energy_col = find_column(
        df, FORCE_COLUMNS["energy"],
        ["energy", "e", "Energy", "Energy_MeV", "gamma_energy", "Eg"],
        "energy"
    )
    x_col = find_column(
        df, FORCE_COLUMNS["x"],
        ["x", "X", "posx", "x_pos", "position_x"],
        "x"
    )
    y_col = find_column(
        df, FORCE_COLUMNS["y"],
        ["y", "Y", "posy", "y_pos", "position_y"],
        "y"
    )
    z_col = find_column(
        df, FORCE_COLUMNS["z"],
        ["z", "Z", "posz", "z_pos", "position_z"],
        "z"
    )

    colmap = {"energy": energy_col, "x": x_col, "y": y_col, "z": z_col}

    out = pd.DataFrame({
        "energy": pd.to_numeric(df[energy_col], errors="coerce"),
        "x": pd.to_numeric(df[x_col], errors="coerce"),
        "y": pd.to_numeric(df[y_col], errors="coerce"),
        "z": pd.to_numeric(df[z_col], errors="coerce"),
    }).dropna()

    out = out[(out["energy"] >= ENERGY_MIN) & (out["energy"] <= ENERGY_MAX)]

    if USE_SAMPLE_ROI:
        out = out[
            (out["x"] >= ROI["x_min"]) & (out["x"] <= ROI["x_max"]) &
            (out["y"] >= ROI["y_min"]) & (out["y"] <= ROI["y_max"]) &
            (out["z"] >= ROI["z_min"]) & (out["z"] <= ROI["z_max"])
        ]

    out = out.reset_index(drop=True)

    print(f"Rows after cuts: {len(out)}")
    if len(out) > 0:
        print("Energy range:", out["energy"].min(), out["energy"].max())
        print("x range:", out["x"].min(), out["x"].max())
        print("y range:", out["y"].min(), out["y"].max())
        print("z range:", out["z"].min(), out["z"].max())

    return out, colmap


def build_spectrum_trace(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    counts, edges = np.histogram(
        df["energy"].values,
        bins=SPECTRUM_BINS,
        range=(ENERGY_MIN, ENERGY_MAX)
    )
    centers = 0.5 * (edges[:-1] + edges[1:])
    return centers, counts


def peak_mask(df: pd.DataFrame, center: float, half_width: float) -> pd.Series:
    return (df["energy"] >= center - half_width) & (df["energy"] <= center + half_width)


def input_style(width_px: int) -> Dict[str, str]:
    return {
        "width": f"{width_px}px",
        "height": "42px",
        "fontSize": "18px",
        "padding": "6px 10px",
        "borderRadius": "6px",
        "border": "1px solid #666",
        "backgroundColor": "#161616",
        "color": "white",
    }


def label_style() -> Dict[str, str]:
    return {
        "display": "block",
        "marginBottom": "6px",
        "fontSize": "15px",
        "fontWeight": "600",
    }


def slider_wrapper(width_px: int) -> Dict[str, str]:
    return {
        "width": f"{width_px}px",
        "padding": "0 8px 12px 8px",
    }


def make_4pane_figure(df: pd.DataFrame, selected_peaks: List[Dict], opacity: float) -> go.Figure:
    opacity = sanitize_opacity(opacity)

    spec_x, spec_y = build_spectrum_trace(df)
    ymax = float(spec_y.max()) if len(spec_y) else 1.0

    bg2d = deterministic_downsample(df, MAX_BACKGROUND_POINTS_2D)
    bg3d = deterministic_downsample(df, MAX_BACKGROUND_POINTS_3D)

    fig = make_subplots(
        rows=2, cols=2,
        specs=[
            [{"type": "xy"}, {"type": "xy"}],
            [{"type": "xy"}, {"type": "scene"}],
        ],
        subplot_titles=(
            "Energy Spectrum",
            "XY Projection",
            "XZ Projection",
            "3D Projection",
        ),
        horizontal_spacing=0.08,
        vertical_spacing=0.12,
    )

    # Spectrum
    fig.add_trace(
        go.Scattergl(
            x=spec_x,
            y=spec_y,
            mode="lines",
            name="Spectrum",
            line=dict(width=1.6),
            hovertemplate="E=%{x:.6f}<br>Counts=%{y}<extra></extra>",
        ),
        row=1, col=1
    )

    for pk in selected_peaks:
        center = float(pk["center"])
        half_width = float(pk["half_width"])
        rgb = (pk["r"], pk["g"], pk["b"])
        label = pk["label"]

        fig.add_vrect(
            x0=center - half_width,
            x1=center + half_width,
            fillcolor=rgba_tuple_to_css(rgb, PEAK_VRECT_ALPHA),
            line_width=0,
            row=1, col=1,
        )

        fig.add_vline(
            x=center,
            line_width=2,
            line_dash="dash",
            line_color=rgba_tuple_to_css(rgb, PEAK_LINE_ALPHA),
            row=1, col=1,
        )

        fig.add_annotation(
            x=center,
            y=ymax,
            text=label,
            showarrow=True,
            arrowhead=1,
            yshift=10,
            font=dict(size=10, color=rgb_tuple_to_css(rgb)),
            row=1, col=1,
        )

    # 2D background
    fig.add_trace(
        go.Scattergl(
            x=bg2d["x"],
            y=bg2d["y"],
            mode="markers",
            marker=dict(size=3, color=f"rgba(180,180,180,{BACKGROUND_ALPHA_2D})"),
            name="All sample events (XY)",
            hoverinfo="skip",
            showlegend=False,
        ),
        row=1, col=2
    )

    fig.add_trace(
        go.Scattergl(
            x=bg2d["x"],
            y=bg2d["z"],
            mode="markers",
            marker=dict(size=3, color=f"rgba(180,180,180,{BACKGROUND_ALPHA_2D})"),
            name="All sample events (XZ)",
            hoverinfo="skip",
            showlegend=False,
        ),
        row=2, col=1
    )

    fig.add_trace(
        go.Scatter3d(
            x=bg3d["x"],
            y=bg3d["y"],
            z=bg3d["z"],
            mode="markers",
            marker=dict(
                size=1.5,
                color="rgba(120,120,120,0.01)",
            ),
            name="All sample events (3D)",
            hoverinfo="skip",
            showlegend=False,
        ),
        row=2, col=2
    )

    for pk in selected_peaks:
        center = float(pk["center"])
        half_width = float(pk["half_width"])
        label = pk["label"]
        rgb = (pk["r"], pk["g"], pk["b"])
        rgba = rgba_tuple_to_css(rgb, opacity)

        sel = df.loc[peak_mask(df, center, half_width)]
        sel2d = deterministic_downsample(sel, MAX_SELECTED_POINTS_PER_PEAK_2D)
        sel3d = deterministic_downsample(sel, MAX_SELECTED_POINTS_PER_PEAK_3D)

        fig.add_trace(
            go.Scattergl(
                x=sel2d["x"],
                y=sel2d["y"],
                mode="markers",
                marker=dict(size=5, color=rgba),
                name=label,
                legendgroup=label,
                showlegend=True,
                hovertemplate=f"{label}<br>x=%{{x:.3f}}<br>y=%{{y:.3f}}<extra></extra>",
            ),
            row=1, col=2
        )

        fig.add_trace(
            go.Scattergl(
                x=sel2d["x"],
                y=sel2d["z"],
                mode="markers",
                marker=dict(size=5, color=rgba),
                name=f"{label} XZ",
                legendgroup=label,
                showlegend=False,
                hovertemplate=f"{label}<br>x=%{{x:.3f}}<br>z=%{{y:.3f}}<extra></extra>",
            ),
            row=2, col=1
        )

        fig.add_trace(
            go.Scatter3d(
                x=sel3d["x"],
                y=sel3d["y"],
                z=sel3d["z"],
                mode="markers",
                marker=dict(size=2, color=rgba),
                name=f"{label} 3D",
                legendgroup=label,
                showlegend=False,
                hovertemplate=f"{label}<br>x=%{{x:.3f}}<br>y=%{{y:.3f}}<br>z=%{{z:.3f}}<extra></extra>",
            ),
            row=2, col=2
        )

    fig.update_xaxes(title_text="Energy", row=1, col=1)
    fig.update_yaxes(title_text="Counts", row=1, col=1)

    fig.update_xaxes(title_text="x", row=1, col=2)
    fig.update_yaxes(title_text="y", row=1, col=2)

    fig.update_xaxes(title_text="x", row=2, col=1)
    fig.update_yaxes(title_text="z", row=2, col=1)

    fig.update_scenes(
        xaxis_title="x",
        yaxis_title="y",
        zaxis_title="z",
        aspectmode="data",
        bgcolor="rgb(10,10,10)",
        xaxis=dict(
            backgroundcolor="rgb(10,10,10)",
            gridcolor="rgba(255,255,255,0.08)",
            zerolinecolor="rgba(255,255,255,0.12)",
            color="white",
        ),
        yaxis=dict(
            backgroundcolor="rgb(10,10,10)",
            gridcolor="rgba(255,255,255,0.08)",
            zerolinecolor="rgba(255,255,255,0.12)",
            color="white",
        ),
        zaxis=dict(
            backgroundcolor="rgb(10,10,10)",
            gridcolor="rgba(255,255,255,0.08)",
            zerolinecolor="rgba(255,255,255,0.12)",
            color="white",
        ),
        row=2, col=2
    )

    fig.update_layout(
        height=980,
        template="plotly_dark",
        title="NASA-gamma Sample-Focused Peak Explorer",
        margin=dict(l=40, r=20, t=80, b=40),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            x=0.0,
            font=dict(size=12),
        ),
    )

    return fig


def selected_table_data(selected_peaks: List[Dict]) -> List[Dict]:
    rows = []
    for i, pk in enumerate(selected_peaks, start=1):
        rows.append({
            "idx": i,
            "label": pk["label"],
            "center": round(float(pk["center"]), 6),
            "half_width": round(float(pk["half_width"]), 6),
            "R": int(pk["r"]),
            "G": int(pk["g"]),
            "B": int(pk["b"]),
        })
    return rows

DF, USED_COLS = load_and_prepare_dataframe(PARQUET_PATH)

if len(DF) == 0:
    raise RuntimeError("No rows remain after cuts. Adjust ENERGY_MIN/MAX or ROI bounds.")

app = Dash(__name__)
server = app.server

app.layout = html.Div(
    style={
        "padding": "12px",
        "fontFamily": "Arial, sans-serif",
        "backgroundColor": "#0f0f0f",
        "color": "white",
        "minHeight": "100vh",
    },
    children=[
        html.H2("NASA-gamma 4-Pane Peak Dashboard"),

        html.Div(
            style={
                "padding": "12px",
                "border": "1px solid #444",
                "borderRadius": "8px",
                "marginBottom": "12px",
                "backgroundColor": "#141414",
            },
            children=[
                html.Div(f"Parquet: {PARQUET_PATH}", style={"marginBottom": "6px"}),
                html.Div(f"Detected columns: {json.dumps(USED_COLS)}", style={"marginBottom": "6px"}),
                html.Div(f"Rows after cuts: {len(DF):,}", style={"marginBottom": "6px"}),
                html.Div(
                    f"ROI enabled: {USE_SAMPLE_ROI} | "
                    f"x=[{ROI['x_min']}, {ROI['x_max']}], "
                    f"y=[{ROI['y_min']}, {ROI['y_max']}], "
                    f"z=[{ROI['z_min']}, {ROI['z_max']}]"
                ),
            ],
        ),

        html.Div(
            style={
                "padding": "12px",
                "border": "1px solid #444",
                "borderRadius": "8px",
                "marginBottom": "12px",
                "backgroundColor": "#141414",
            },
            children=[
                html.Div(
                    "Click the spectrum to capture a peak center, then set width, color, and opacity.",
                    style={"marginBottom": "12px", "fontSize": "16px"}
                ),

                html.Div(
                    style={
                        "display": "flex",
                        "flexWrap": "wrap",
                        "gap": "24px",
                        "alignItems": "flex-end",
                        "marginBottom": "18px",
                    },
                    children=[
                        html.Div([
                            html.Label("Peak center", style=label_style()),
                            dcc.Input(
                                id="peak-center",
                                type="number",
                                step=0.000001,
                                value=1.0,
                                style=input_style(200),
                            ),
                        ]),
                        html.Div([
                            html.Label("Half-width", style=label_style()),
                            html.Div(
                                style=slider_wrapper(340),
                                children=[
                                    dcc.Slider(
                                        id="peak-half-width",
                                        min=0.001,
                                        max=0.10,
                                        step=0.0005,
                                        value=DEFAULT_PEAK_HALF_WIDTH,
                                        tooltip={"placement": "bottom", "always_visible": True},
                                        marks={
                                            0.001: "0.001",
                                            0.02: "0.02",
                                            0.04: "0.04",
                                            0.06: "0.06",
                                            0.08: "0.08",
                                            0.10: "0.10",
                                        },
                                        **SLIDER_STYLE,
                                    )
                                ],
                            ),
                        ]),
                        html.Div([
                            html.Label("Label", style=label_style()),
                            dcc.Input(
                                id="peak-label",
                                type="text",
                                value="Peak 1",
                                style=input_style(230),
                            ),
                        ]),
                    ],
                ),

                html.Div(
                    style={
                        "display": "flex",
                        "flexWrap": "wrap",
                        "gap": "28px",
                        "alignItems": "flex-end",
                        "marginBottom": "18px",
                    },
                    children=[
                        html.Div([
                            html.Label("R", style=label_style()),
                            html.Div(
                                style=slider_wrapper(280),
                                children=[
                                    dcc.Slider(
                                        id="color-r",
                                        min=0,
                                        max=255,
                                        step=1,
                                        value=DEFAULT_R,
                                        tooltip={"placement": "bottom", "always_visible": True},
                                        marks={0: "0", 64: "64", 128: "128", 192: "192", 255: "255"},
                                        **SLIDER_STYLE,
                                    )
                                ],
                            ),
                        ]),
                        html.Div([
                            html.Label("G", style=label_style()),
                            html.Div(
                                style=slider_wrapper(280),
                                children=[
                                    dcc.Slider(
                                        id="color-g",
                                        min=0,
                                        max=255,
                                        step=1,
                                        value=DEFAULT_G,
                                        tooltip={"placement": "bottom", "always_visible": True},
                                        marks={0: "0", 64: "64", 128: "128", 192: "192", 255: "255"},
                                        **SLIDER_STYLE,
                                    )
                                ],
                            ),
                        ]),
                        html.Div([
                            html.Label("B", style=label_style()),
                            html.Div(
                                style=slider_wrapper(280),
                                children=[
                                    dcc.Slider(
                                        id="color-b",
                                        min=0,
                                        max=255,
                                        step=1,
                                        value=DEFAULT_B,
                                        tooltip={"placement": "bottom", "always_visible": True},
                                        marks={0: "0", 64: "64", 128: "128", 192: "192", 255: "255"},
                                        **SLIDER_STYLE,
                                    )
                                ],
                            ),
                        ]),
                    ],
                ),

                html.Div(
                    style={
                        "display": "flex",
                        "flexWrap": "wrap",
                        "gap": "28px",
                        "alignItems": "flex-end",
                        "marginBottom": "18px",
                    },
                    children=[
                        html.Div([
                            html.Label("Opacity", style=label_style()),
                            html.Div(
                                style=slider_wrapper(280),
                                children=[
                                    dcc.Slider(
                                        id="opacity-slider",
                                        min=0.05,
                                        max=1.0,
                                        step=0.01,
                                        value=DEFAULT_OPACITY,
                                        tooltip={"placement": "bottom", "always_visible": True},
                                        marks={
                                            0.1: "0.1",
                                            0.3: "0.3",
                                            0.5: "0.5",
                                            0.7: "0.7",
                                            1.0: "1.0",
                                        },
                                        **SLIDER_STYLE,
                                    )
                                ],
                            ),
                        ]),
                        html.Div([
                            html.Label("Color preview", style=label_style()),
                            html.Div(
                                id="color-preview",
                                style={
                                    "display": "inline-block",
                                    "width": "60px",
                                    "height": "42px",
                                    "border": "1px solid #999",
                                    "borderRadius": "6px",
                                    "backgroundColor": f"rgba(255,0,0,{DEFAULT_OPACITY})",
                                },
                            ),
                        ]),
                    ],
                ),

                html.Div(
                    style={"marginBottom": "14px"},
                    children=[
                        html.Button(
                            "Add Peak",
                            id="add-peak-btn",
                            n_clicks=0,
                            style={
                                "marginRight": "10px",
                                "height": "42px",
                                "fontSize": "16px",
                                "padding": "0 16px",
                                "borderRadius": "6px",
                                "cursor": "pointer",
                            },
                        ),
                        html.Button(
                            "Remove Last Peak",
                            id="remove-last-btn",
                            n_clicks=0,
                            style={
                                "marginRight": "10px",
                                "height": "42px",
                                "fontSize": "16px",
                                "padding": "0 16px",
                                "borderRadius": "6px",
                                "cursor": "pointer",
                            },
                        ),
                        html.Button(
                            "Clear Peaks",
                            id="clear-peaks-btn",
                            n_clicks=0,
                            style={
                                "height": "42px",
                                "fontSize": "16px",
                                "padding": "0 16px",
                                "borderRadius": "6px",
                                "cursor": "pointer",
                            },
                        ),
                    ],
                ),

                dash_table.DataTable(
                    id="selected-peaks-table",
                    columns=[
                        {"name": "idx", "id": "idx"},
                        {"name": "label", "id": "label"},
                        {"name": "center", "id": "center"},
                        {"name": "half_width", "id": "half_width"},
                        {"name": "R", "id": "R"},
                        {"name": "G", "id": "G"},
                        {"name": "B", "id": "B"},
                    ],
                    data=[],
                    style_table={"overflowX": "auto"},
                    style_cell={
                        "textAlign": "center",
                        "padding": "8px",
                        "backgroundColor": "#1f1f1f",
                        "color": "white",
                        "border": "1px solid #444",
                        "fontSize": "15px",
                    },
                    style_header={
                        "backgroundColor": "#111",
                        "fontWeight": "bold",
                        "border": "1px solid #555",
                    },
                ),
            ],
        ),

        dcc.Store(id="selected-peaks-store", data=[]),

        dcc.Graph(
            id="main-graph",
            figure=make_4pane_figure(DF, [], DEFAULT_OPACITY),
            clear_on_unhover=True,
            style={"height": "1000px"},
        ),
    ]
)

@app.callback(
    Output("color-preview", "style"),
    Input("color-r", "value"),
    Input("color-g", "value"),
    Input("color-b", "value"),
    Input("opacity-slider", "value"),
)
def update_color_preview(r, g, b, opacity):
    r, g, b = sanitize_rgb(r or 0, g or 0, b or 0)
    opacity = sanitize_opacity(opacity if opacity is not None else DEFAULT_OPACITY)
    return {
        "display": "inline-block",
        "width": "60px",
        "height": "42px",
        "border": "1px solid #999",
        "borderRadius": "6px",
        "backgroundColor": rgba_tuple_to_css((r, g, b), opacity),
    }


@app.callback(
    Output("peak-center", "value"),
    Input("main-graph", "clickData"),
    State("peak-center", "value"),
    prevent_initial_call=True,
)
def capture_peak_center_from_spectrum(click_data, current_center):
    if not click_data or "points" not in click_data or len(click_data["points"]) == 0:
        return current_center

    pt = click_data["points"][0]
    x = pt.get("x", None)
    if x is None:
        return current_center

    try:
        x = float(x)
    except Exception:
        return current_center

    if ENERGY_MIN <= x <= ENERGY_MAX:
        return x

    return current_center


@app.callback(
    Output("selected-peaks-store", "data"),
    Output("peak-label", "value"),
    Input("add-peak-btn", "n_clicks"),
    Input("remove-last-btn", "n_clicks"),
    Input("clear-peaks-btn", "n_clicks"),
    State("selected-peaks-store", "data"),
    State("peak-center", "value"),
    State("peak-half-width", "value"),
    State("peak-label", "value"),
    State("color-r", "value"),
    State("color-g", "value"),
    State("color-b", "value"),
    prevent_initial_call=True,
)
def manage_peaks(add_clicks, remove_clicks, clear_clicks,
                 selected_peaks, center, half_width, label, r, g, b):
    selected_peaks = list(selected_peaks or [])
    trigger = ctx.triggered_id

    if trigger == "clear-peaks-btn":
        return [], "Peak 1"

    if trigger == "remove-last-btn":
        if selected_peaks:
            selected_peaks.pop()
        return selected_peaks, f"Peak {len(selected_peaks) + 1}"

    if trigger == "add-peak-btn":
        if center is None or half_width is None:
            return selected_peaks, label

        r, g, b = sanitize_rgb(r or 0, g or 0, b or 0)

        if label is None or str(label).strip() == "":
            label = f"Peak {len(selected_peaks) + 1}"

        selected_peaks.append({
            "center": float(center),
            "half_width": float(abs(half_width)),
            "label": str(label).strip(),
            "r": r,
            "g": g,
            "b": b,
        })
        return selected_peaks, f"Peak {len(selected_peaks) + 1}"

    return selected_peaks, label


@app.callback(
    Output("selected-peaks-table", "data"),
    Output("main-graph", "figure"),
    Input("selected-peaks-store", "data"),
    Input("opacity-slider", "value"),
)
def refresh_table_and_graph(selected_peaks, opacity):
    selected_peaks = list(selected_peaks or [])
    opacity = sanitize_opacity(opacity if opacity is not None else DEFAULT_OPACITY)
    fig = make_4pane_figure(DF, selected_peaks, opacity)
    table = selected_table_data(selected_peaks)
    return table, fig


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.2", port=8050)