import os
import sys
import socket
import subprocess
import threading
import time
import webbrowser
from pathlib import Path

import dash
from dash import Dash, dcc, html, Input, Output, State, ctx, no_update


# ────────────────────────────────────────────────────────────────────────────
# Configuration: where each tool's script lives + how to launch it
# ────────────────────────────────────────────────────────────────────────────

HERE = Path(__file__).parent.resolve()


def _script(name: str) -> str:
    """Resolve a tool's script path. Falls back to the launcher's directory."""
    p = HERE / name
    return str(p)


# Each tool has:
#   key         — internal id (used in HTML ids and the _JOBS dict)
#   title       — what the user sees
#   description — one-liner under the title
#   script      — path to the Python script
#   port        — TCP port the tool binds, or None if it doesn't bind one
#   url         — what URL to open in the browser (None for no-port tools)
#   extra_args  — list of additional CLI args to pass to the subprocess
#   env_extra   — environment variable overrides for the subprocess
#   accent      — color used on the launch button
TOOLS = [
    {
        "key":         "lunar",
        "title":       "Lunar Prospector GRS Viewer",
        "description": ("3D moon globe + spectrum browser. "
                        "Click a point to load gamma-ray spectra; "
                        "draw a region to average."),
        "script":      _script("LunarProspector_WebApp.py"),
        "port":        8050,
        "url":         "http://127.0.0.1:8050",
        "extra_args":  [],
        "env_extra":   {},
        "accent":      "#7ecfff",
    },
    {
        "key":         "ceres",
        "title":       "Ceres DAWN GRaND Viewer",
        "description": ("3D Ceres globe with SPICE-driven orbit tracks, "
                        "GRaND elemental hotspots, "
                        "and per-row spectrum exports."),
        "script":      _script("Ceres_Plotter.py"),
        "port":        8052,                     # moved off 8050 to coexist with Lunar
        "url":         "http://127.0.0.1:8052",
        "extra_args":  [],
        "env_extra":   {"PORT": "8052"},          # picked up by patched ceres script
        "accent":      "#c97bff",
    },
    {
        "key":         "mars",
        "title":       "Mars Curiosity DAN Viewer",
        "description": ("CTN/CETN ratio map of the rover path, "
                        "count-rate time series, and a "
                        "Hydrogen-Index strip — opens in a browser tab."),
        "script":      _script("CuriosityVis.py"),
        "port":        None,                       # uses fig.show() — no server
        "url":         None,
        "extra_args":  ["--non-interactive"],      # avoids stdin prompts
        "env_extra":   {},
        "accent":      "#e07b39",
    },
    {
        "key":         "fetcher",
        "title":       "NASA PDS4 Data Fetcher",
        "description": ("Browser GUI for downloading "
                        "Lunar / Mars / Ceres data + SPICE kernels "
                        "from NASA archives."),
        "script":      _script("data_fetcher.py"),
        "port":        8051,
        "url":         "http://127.0.0.1:8051",
        "extra_args":  [],
        "env_extra":   {},
        "accent":      "#ffd166",
    },
]

TOOLS_BY_KEY = {t["key"]: t for t in TOOLS}


# ────────────────────────────────────────────────────────────────────────────
# Subprocess tracking
# ────────────────────────────────────────────────────────────────────────────

# Maps tool key → dict with "proc" (Popen), "started_at" (float), "url_opened"
_JOBS: dict[str, dict] = {}
_JOBS_LOCK = threading.Lock()


def _is_port_open(port: int, host: str = "127.0.0.1", timeout: float = 0.4) -> bool:
    """Quick TCP probe.  True when something is bound to the port."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _job_status(tool_key: str) -> str:
    """Return one of: 'idle', 'starting', 'running', 'stopped', 'failed'."""
    with _JOBS_LOCK:
        job = _JOBS.get(tool_key)
    if job is None:
        return "idle"
    proc = job["proc"]
    rc = proc.poll()
    if rc is None:
        # Still running.  If it has a port, check whether it's bound yet.
        tool = TOOLS_BY_KEY[tool_key]
        if tool["port"] is None:
            return "running"
        return "running" if _is_port_open(tool["port"]) else "starting"
    # Process exited.  rc == 0 → stopped cleanly; rc != 0 → failed.
    return "stopped" if rc == 0 else "failed"


def _launch(tool_key: str) -> str:
    """Spawn the subprocess for a tool.  Returns a status string for display."""
    tool = TOOLS_BY_KEY.get(tool_key)
    if tool is None:
        return f"error: unknown tool {tool_key!r}"

    with _JOBS_LOCK:
        existing = _JOBS.get(tool_key)
        if existing and existing["proc"].poll() is None:
            return "already running"

    script_path = tool["script"]
    if not Path(script_path).exists():
        return f"error: script not found at {script_path}"

    env = os.environ.copy()
    env.update(tool["env_extra"])

    cmd = [sys.executable, script_path] + list(tool["extra_args"])

    # We deliberately let the subprocess inherit stdout/stderr so its output
    # appears in the launcher's terminal.  This means the user sees the
    # tool's startup messages and any tracebacks without us needing to run a
    # log-collection thread.
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,    # no interactive prompts can hang us
            stdout=None,
            stderr=None,
            env=env,
            cwd=str(HERE),
        )
    except Exception as e:
        return f"error: failed to spawn ({e})"

    with _JOBS_LOCK:
        _JOBS[tool_key] = {
            "proc":       proc,
            "started_at": time.time(),
            "url_opened": False,
        }

    # If this tool exposes a URL, kick off a background thread that waits
    # for the port to come up (or a few seconds), then opens the browser.
    if tool["url"]:
        threading.Thread(
            target=_open_when_ready, args=(tool_key,), daemon=True
        ).start()

    return "launched"


def _open_when_ready(tool_key: str, timeout: float = 30.0):
    tool = TOOLS_BY_KEY[tool_key]
    deadline = time.time() + timeout
    while time.time() < deadline:
        with _JOBS_LOCK:
            job = _JOBS.get(tool_key)
        if not job:
            return
        if job["proc"].poll() is not None:
            return  # exited before we got a chance
        if tool["port"] and _is_port_open(tool["port"]):
            with _JOBS_LOCK:
                if not job["url_opened"]:
                    job["url_opened"] = True
                    webbrowser.open(tool["url"])
            return
        time.sleep(0.4)


def _stop(tool_key: str) -> str:
    """Politely terminate the subprocess.  Returns a status string."""
    with _JOBS_LOCK:
        job = _JOBS.get(tool_key)
    if not job:
        return "not running"
    proc = job["proc"]
    if proc.poll() is not None:
        return "already exited"

    proc.terminate()
    try:
        proc.wait(timeout=3.0)
        return "stopped"
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2.0)
        return "killed (didn't respond to terminate)"


# ────────────────────────────────────────────────────────────────────────────
# Dash UI
# ────────────────────────────────────────────────────────────────────────────

_PAL = dict(
    BG_DEEP="#0a0e1a", BG_PANEL="#0d1424", BG_INPUT="#1a2438",
    FG_TEXT="#e0e8ff", FG_DIM="#7a8aa8",
    FG_CYAN="#7ecfff", FG_YELLOW="#ffd166", FG_GREEN="#88ff88",
    FG_PURPLE="#c97bff", FG_RED="#ff7777",
    BORDER="#1e2a3a",
)

STATUS_COLORS = {
    "idle":     _PAL["FG_DIM"],
    "starting": _PAL["FG_YELLOW"],
    "running":  _PAL["FG_GREEN"],
    "stopped":  _PAL["FG_DIM"],
    "failed":   _PAL["FG_RED"],
}


def _tile(tool: dict):
    return html.Div(
        id=f"card-{tool['key']}",
        style={
            "background":   _PAL["BG_PANEL"],
            "border":       f"1px solid {_PAL['BORDER']}",
            "borderRadius": "5px",
            "padding":      "18px 20px",
            "display":      "flex",
            "flexDirection": "column",
            "gap":          "10px",
            "minHeight":    "200px",
        },
        children=[
            html.Div(
                style={"display": "flex", "alignItems": "baseline",
                       "justifyContent": "space-between"},
                children=[
                    html.H3(tool["title"],
                            style={"margin": 0, "color": tool["accent"],
                                   "fontSize": "1.05rem",
                                   "letterSpacing": "0.06em"}),
                    html.Span(
                        id=f"status-{tool['key']}",
                        children="idle",
                        style={"fontSize": "0.7rem", "letterSpacing": "0.15em",
                               "color": STATUS_COLORS["idle"]},
                    ),
                ],
            ),
            html.Div(
                tool["description"],
                style={"color": _PAL["FG_DIM"], "fontSize": "0.82rem",
                       "lineHeight": "1.4", "flex": 1},
            ),
            html.Div(
                style={"fontSize": "0.7rem", "color": _PAL["FG_DIM"],
                       "letterSpacing": "0.1em"},
                children=(
                    f"Port {tool['port']}  ·  {tool['url']}" if tool["port"]
                    else "No server — opens a browser tab directly"
                ),
            ),
            html.Div(
                style={"display": "flex", "gap": "8px", "marginTop": "4px"},
                children=[
                    html.Button(
                        "▶  Launch", id=f"launch-{tool['key']}", n_clicks=0,
                        style={
                            "background": "#1e3a5f", "color": tool["accent"],
                            "border": f"1px solid {tool['accent']}",
                            "padding": "6px 14px", "cursor": "pointer",
                            "fontFamily": "'Courier New', monospace",
                            "fontSize": "0.82rem", "fontWeight": "bold",
                            "letterSpacing": "0.08em", "borderRadius": "3px",
                            "flex": 1,
                        },
                    ),
                    html.Button(
                        "■  Stop", id=f"stop-{tool['key']}", n_clicks=0,
                        style={
                            "background": "#3a1e1e", "color": "#ff9999",
                            "border": "1px solid #ff9999",
                            "padding": "6px 14px", "cursor": "pointer",
                            "fontFamily": "'Courier New', monospace",
                            "fontSize": "0.82rem",
                            "letterSpacing": "0.08em", "borderRadius": "3px",
                        },
                    ),
                ],
            ),
        ],
    )


app = Dash(__name__, title="NASA Plotting Launcher")

app.layout = html.Div(
    style={"background": _PAL["BG_DEEP"], "color": _PAL["FG_TEXT"],
           "minHeight": "100vh", "padding": "20px",
           "fontFamily": "'Courier New', monospace"},
    children=[
        html.Div(
            style={"display": "flex", "alignItems": "center", "gap": "20px",
                   "marginBottom": "8px"},
            children=[
                html.H1("NASA TOOLCHAIN LAUNCHER",
                        style={"margin": 0, "letterSpacing": "0.12em",
                               "fontSize": "1.4rem", "color": _PAL["FG_CYAN"]}),
                html.Span("MISSION CONTROL · senior design",
                          style={"opacity": 0.45, "fontSize": "0.75rem",
                                 "letterSpacing": "0.2em",
                                 "color": _PAL["FG_DIM"]}),
            ]
        ),
        html.Div(
            style={"color": _PAL["FG_DIM"], "fontSize": "0.78rem",
                   "marginBottom": "20px", "letterSpacing": "0.05em",
                   "lineHeight": "1.5"},
            children=[
                "Click Launch on any tool to start it.  Each one runs in a ",
                "separate process on its own port, so you can have several ",
                "running at once.  Closing this launcher does NOT shut the ",
                "tools down — use Stop, or close them in their browser tab.",
            ],
        ),

        # 2×2 tile grid
        html.Div(
            style={"display": "grid", "gap": "16px",
                   "gridTemplateColumns": "1fr 1fr"},
            children=[_tile(t) for t in TOOLS],
        ),

        # Status poll — keeps the per-tile status indicators current
        dcc.Interval(id="status-poll", interval=1500, n_intervals=0),
    ]
)


# ────────────────────────────────────────────────────────────────────────────
# Callbacks
# ────────────────────────────────────────────────────────────────────────────

def _make_launch_callback(tool_key):
    @app.callback(
        Output(f"status-{tool_key}", "children", allow_duplicate=True),
        Output(f"status-{tool_key}", "style",    allow_duplicate=True),
        Input(f"launch-{tool_key}", "n_clicks"),
        prevent_initial_call=True,
    )
    def _on_launch(n_clicks):
        if not n_clicks:
            return no_update, no_update
        msg = _launch(tool_key)
        if msg.startswith("error"):
            color = STATUS_COLORS["failed"]
            return msg, {"fontSize": "0.7rem",
                         "letterSpacing": "0.15em", "color": color}
        if msg == "already running":
            return "already running", {"fontSize": "0.7rem",
                                       "letterSpacing": "0.15em",
                                       "color": STATUS_COLORS["running"]}
        return "starting", {"fontSize": "0.7rem",
                            "letterSpacing": "0.15em",
                            "color": STATUS_COLORS["starting"]}
    return _on_launch


def _make_stop_callback(tool_key):
    @app.callback(
        Output(f"status-{tool_key}", "children", allow_duplicate=True),
        Output(f"status-{tool_key}", "style",    allow_duplicate=True),
        Input(f"stop-{tool_key}", "n_clicks"),
        prevent_initial_call=True,
    )
    def _on_stop(n_clicks):
        if not n_clicks:
            return no_update, no_update
        _stop(tool_key)
        return "stopped", {"fontSize": "0.7rem",
                           "letterSpacing": "0.15em",
                           "color": STATUS_COLORS["stopped"]}
    return _on_stop


# Register one launch + stop callback per tool.
for _t in TOOLS:
    _make_launch_callback(_t["key"])
    _make_stop_callback(_t["key"])


# Status poll → updates all four status badges at once
@app.callback(
    [Output(f"status-{t['key']}", "children") for t in TOOLS] +
    [Output(f"status-{t['key']}", "style")    for t in TOOLS],
    Input("status-poll", "n_intervals"),
)
def _poll_statuses(_n):
    children = []
    styles   = []
    for t in TOOLS:
        st = _job_status(t["key"])
        children.append(st)
        styles.append({
            "fontSize": "0.7rem",
            "letterSpacing": "0.15em",
            "color": STATUS_COLORS.get(st, _PAL["FG_DIM"]),
        })
    return children + styles


# ────────────────────────────────────────────────────────────────────────────
# Entry point
# ────────────────────────────────────────────────────────────────────────────

def main():
    PORT = 8049
    print("=" * 60)
    print("  NASA Toolchain Launcher")
    print("=" * 60)
    print(f"  Listening on http://127.0.0.1:{PORT}")
    print(f"  Looking for tool scripts in: {HERE}")
    for t in TOOLS:
        present = "✓" if Path(t["script"]).exists() else "✗ MISSING"
        print(f"    {present}  {t['title']:<32}  →  {Path(t['script']).name}")
    print("=" * 60)
    print("  Ctrl+C in this terminal to shut the launcher down.")
    print("  Running tools will keep going until you stop them.")
    print("=" * 60)

    threading.Timer(
        1.5, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}")
    ).start()
    app.run(debug=False, port=PORT, host="127.0.0.1")


if __name__ == "__main__":
    main()