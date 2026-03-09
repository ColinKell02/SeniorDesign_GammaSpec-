import pandas as pd
import numpy as np
import plotly.graph_objects as go
import dash
from dash import Dash, dcc, html, Input, Output, State, ctx
import dash_vtk
from scipy.spatial import KDTree
import pds4_tools as pds
from pathlib import Path
from PIL import Image
import matplotlib.pyplot as plt

# Configuration options
INDEX_FILE = "spatial_library_full.csv"
ABUNDANCE_FILE = "lunar_abundances.csv"
DATA_DIR = Path("Moon/data")
TEXTURE_FILE = "moon_surface.tif" 
R_MOON = 1737.4 

Image.MAX_IMAGE_PIXELS = None 

# --- Data Loading ---
print("Loading spatial library...")
df_index = pd.read_csv(INDEX_FILE)
df_moon_full = df_index[df_index['mission'] == 'Moon'].copy()

if 'altitude' in df_moon_full.columns:
    df_moon_full['altitude_phase'] = np.where(df_moon_full['altitude'] > 50, 'High', 'Low')
else:
    df_moon_full['altitude_phase'] = 'High'

# Normalize CSV longitudes to -180 to 180 safely for bounding box checks
df_moon_full['lon_norm'] = (df_moon_full['lon'] + 180) % 360 - 180

# Load Elemental Abundances
print("Loading abundance library...")
try:
    df_abund = pd.read_csv(ABUNDANCE_FILE)
    df_abund = df_abund[df_abund['SOURCE_FILE'].str.contains('2deg')].copy()
    df_abund['CENTER_LAT'] = (df_abund['SOUTHERNMOST_LATITUDE'] + df_abund['NORTHERNMOST_LATITUDE']) / 2.0
    df_abund['CENTER_LON'] = (df_abund['WESTERNMOST_LONGITUDE'] + df_abund['EASTERNMOST_LONGITUDE']) / 2.0
    
    abund_lat_rad = np.radians(df_abund['CENTER_LAT'].values)
    abund_lon_rad = np.radians(df_abund['CENTER_LON'].values)
    abund_x = R_MOON * np.cos(abund_lat_rad) * np.cos(abund_lon_rad)
    abund_y = R_MOON * np.cos(abund_lat_rad) * np.sin(abund_lon_rad)
    abund_z = R_MOON * np.sin(abund_lat_rad)
    ABUND_TREE = KDTree(np.column_stack((abund_x, abund_y, abund_z)))

# Element Dictionary:
    ELEMENTS = {
        'W_MGO': 'Magnesium (MgO)',
        'W_AL2O3': 'Aluminum (Al2O3)',
        'W_SIO2': 'Silicon (SiO2)',
        'W_CAO': 'Calcium (CaO)',
        'W_FEO': 'Iron (FeO)',
        'W_K': 'Potassium (K)',
        'W_TH': 'Thorium (Th)',
        'W_U': 'Uranium (U)',
        'W_TIO2': 'Titanium (TiO2)'
    }
    AVAILABLE_ELEMENTS = {k: v for k, v in ELEMENTS.items() if k in df_abund.columns}
except FileNotFoundError:
    print(f"Warning: {ABUNDANCE_FILE} not found. Heatmaps disabled.")
    df_abund = pd.DataFrame()
    AVAILABLE_ELEMENTS = {}

# --- Math & Logic Helpers ---
def load_lp_spectrum(xml_file, record_index):
    try:
        struct = pds.read(str(xml_file), lazy_load=False, quiet=True)
        iden = struct[0].id
        data = struct[iden].data
        spec = data["GROUP_0, Accepted Spectrum"]
        return spec[record_index] if spec.ndim > 1 else spec
    except Exception as e:
        return []

def get_spherical_coords(lat_deg, lon_deg, radius=R_MOON):
    lat_rad, lon_rad = np.radians(lat_deg), np.radians(lon_deg)
    x = radius * np.cos(lat_rad) * np.cos(lon_rad)
    y = radius * np.cos(lat_rad) * np.sin(lon_rad)
    z = radius * np.sin(lat_rad)
    return x, y, z

def get_lat_lon(x, y, z, radius=R_MOON):
    lat = np.degrees(np.arcsin(z / radius))
    lon = np.degrees(np.arctan2(y, x))
    return lat, lon

def create_crosshair(lat, lon, size=3):
    """Creates a 3D crosshair for single point selection"""
    lats = [lat - size, lat + size, lat, lat]
    lons = [lon, lon, lon - size, lon + size]
    x, y, z = get_spherical_coords(np.array(lats), np.array(lons), R_MOON * 1.02)
    pts = np.column_stack((x, y, z)).flatten().tolist()
    lines = [2, 0, 1, 2, 2, 3] # Two intersecting lines
    return pts, lines

def create_highlight_geometry(min_lat, max_lat, min_lon, max_lon):
    """Generates the VTK line coordinates for visual feedback"""
    if pd.isna(min_lat) or pd.isna(min_lon):
        return [], []
        
    if pd.isna(max_lat) or pd.isna(max_lon): # Single point
        return create_crosshair(min_lat, min_lon)
    
    # Region Bounding Box
    lons_bottom = np.linspace(min_lon, max_lon, 20)
    lats_bottom = np.full(20, min_lat)
    lons_right = np.full(20, max_lon)
    lats_right = np.linspace(min_lat, max_lat, 20)
    lons_top = np.linspace(max_lon, min_lon, 20)
    lats_top = np.full(20, max_lat)
    lons_left = np.full(20, min_lon)
    lats_left = np.linspace(max_lat, min_lat, 20)
    
    all_lats = np.concatenate([lats_bottom, lats_right, lats_top, lats_left])
    all_lons = np.concatenate([lons_bottom, lons_right, lons_top, lons_left])
    
    x, y, z = get_spherical_coords(all_lats, all_lons, R_MOON * 1.01)
    pts = np.column_stack((x, y, z)).flatten().tolist()
    n_pts = len(all_lats)
    lines = [n_pts] + list(range(n_pts))
    return pts, lines

# --- VTK Base Data ---
lon_res, lat_res = 1536, 768
print(f"Pre-computing VTK Point Cloud Moon at {lon_res}x{lat_res}...")

phi = np.linspace(-np.pi, np.pi, lon_res)
theta = np.linspace(-np.pi/2, np.pi/2, lat_res)
phi, theta = np.meshgrid(phi, theta)

X_BASE = (R_MOON * np.cos(theta) * np.cos(phi)).flatten()
Y_BASE = (R_MOON * np.cos(theta) * np.sin(phi)).flatten()
Z_BASE = (R_MOON * np.sin(theta)).flatten()
MOON_PTS_FLAT = np.column_stack((X_BASE, Y_BASE, Z_BASE)).flatten().tolist()
MOON_VERTS_FLAT = [len(X_BASE)] + list(range(len(X_BASE)))

try:
    img = Image.open(TEXTURE_FILE).convert('RGB')
    img = img.resize((lon_res, lat_res), Image.Resampling.LANCZOS) 
    img_data = np.flipud(np.array(img))
    BASE_COLORS = img_data.reshape(-1, 3)
except Exception:
    BASE_COLORS = np.ones((lat_res * lon_res, 3), dtype=np.uint8) * 150
    
BASE_COLORS_FLAT = BASE_COLORS.flatten().tolist()

def generate_heatmap_colors(element_col):
    if df_abund.empty or element_col not in df_abund.columns:
        return BASE_COLORS_FLAT
    
    vals = df_abund[element_col].values
    vmin, vmax = np.percentile(vals, 2), np.percentile(vals, 98)
    norm_vals = np.clip((vals - vmin) / (vmax - vmin), 0, 1)
    cmap = plt.get_cmap('plasma')
    rgb_map = (cmap(norm_vals)[:, :3] * 255).astype(np.uint8)
    
    coords = np.column_stack((X_BASE, Y_BASE, Z_BASE))
    dist, idx = ABUND_TREE.query(coords)
    heatmap_colors = rgb_map[idx]
    
    blended = (heatmap_colors * 0.5 + BASE_COLORS * 0.5).astype(np.uint8)
    return blended.flatten().tolist()

def create_reference_lines():
    pts = []
    phi = np.linspace(-np.pi, np.pi, 200)
    for p in phi:
        pts.extend([R_MOON * 1.01 * np.cos(p), R_MOON * 1.01 * np.sin(p), 0.0])
    pts.extend([0.0, 0.0, R_MOON * 1.15]) 
    pts.extend([0.0, 0.0, -R_MOON * 1.15]) 
    lines = [200] + list(range(200)) + [2, 200, 201]
    return pts, lines

ref_pts, ref_lines = create_reference_lines()

# --- Initialize App ---
app = Dash(__name__)

view_options = [{'label': 'Visual (Grayscale)', 'value': 'VISUAL'}]
view_options.extend([{'label': v, 'value': k} for k, v in AVAILABLE_ELEMENTS.items()])

app.layout = html.Div(
    style={
        'fontFamily': 'sans-serif', 'color': 'white', 'backgroundColor': '#111111', 
        'padding': '20px', 'height': '95vh', 'display': 'flex', 
        'flexDirection': 'column', 'boxSizing': 'border-box'
    }, 
    children=[
    
    html.H1("Lunar Prospector Spatial Spectrometer", style={'flex': '0 1 auto', 'margin': '0 0 20px 0'}),
    
    dcc.Store(id='click-state', data={'step': 0, 'lat1': None, 'lon1': None}),
    dcc.Store(id='plot-trigger', data=0), # Invisible trigger to fire the plotting callback
    
    # --- TOP HALF ---
    html.Div(style={'display': 'flex', 'flexDirection': 'row', 'flex': '1 1 50%', 'marginBottom': '20px', 'minHeight': 0}, children=[
        
# LEFT COLUMN
        html.Div(style={
            'width': '33%', 'backgroundColor': '#222222', 'padding': '20px', 
            'borderRadius': '5px', 'marginRight': '20px', 'display': 'flex', 
            'flexDirection': 'column', 'gap': '15px', 'overflowY': 'auto', 'boxSizing': 'border-box'
        }, children=[
            
            html.Div([
                html.Label("Map Layer & Altitude:", style={'fontWeight': 'bold', 'display': 'block', 'marginBottom': '8px'}),
                dcc.Dropdown(id='map-select', options=view_options, value='VISUAL', clearable=False, style={'color': 'black', 'marginBottom': '10px'}),
                dcc.Dropdown(id='altitude-select', options=[{'label': 'High Altitude (~100km)', 'value': 'High'}, {'label': 'Low Altitude (~30km)', 'value': 'Low'}], value='High', clearable=False, style={'color': 'black'})
            ]),
            
            # NEW: Camera Reset Button
            html.Button('🎥 Reset Camera View', id='reset-camera-btn', n_clicks=0, style={'width': '100%', 'padding': '8px', 'cursor': 'pointer', 'backgroundColor': '#555', 'color': 'white', 'border': 'none', 'borderRadius': '3px'}),
            
            # Missing Data Warning Banner
            html.Div(id='missing-data-warning', style={'backgroundColor': '#856404', 'color': '#ffeeba', 'padding': '10px', 'borderRadius': '3px', 'fontSize': '12px', 'display': 'none'}),
            
            # Region Selection Tools
            html.Div([
                html.Label("Interaction Mode:", style={'fontWeight': 'bold', 'display': 'block', 'marginBottom': '8px'}),
                dcc.RadioItems(
                    id='interaction-mode',
                    options=[
                        {'label': ' Single Point (Nearest)', 'value': 'single'},
                        {'label': ' Select Region (Average)', 'value': 'region'}
                    ],
                    value='single',
                    style={'marginBottom': '10px'},
                    # THIS FIXES THE TEXT COLOR:
                    labelStyle={'color': 'white', 'display': 'block', 'marginBottom': '5px'} 
                ),
                
                html.Div(id='status-text', children="Click the moon to plot a single point.", style={'fontSize': '12px', 'color': '#AAAAAA', 'marginBottom': '15px', 'fontStyle': 'italic'}),
                
                html.Div(style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr', 'gap': '10px', 'marginBottom': '10px'}, children=[
                    dcc.Input(id='min-lat-input', type='number', placeholder='Min Lat', style={'padding': '5px', 'color': 'black'}),
                    dcc.Input(id='max-lat-input', type='number', placeholder='Max Lat', style={'padding': '5px', 'color': 'black'}),
                    dcc.Input(id='min-lon-input', type='number', placeholder='Min Lon', style={'padding': '5px', 'color': 'black'}),
                    dcc.Input(id='max-lon-input', type='number', placeholder='Max Lon', style={'padding': '5px', 'color': 'black'}),
                ]),
                
                html.Button('Manual Plot / Refresh', id='search-btn', n_clicks=0, style={'width': '100%', 'padding': '10px', 'cursor': 'pointer', 'fontWeight': 'bold', 'backgroundColor': '#28A745', 'color': 'white', 'border': 'none', 'borderRadius': '3px'})
            ])
        ]),

# RIGHT COLUMN: VTK 3D View
        html.Div(style={'width': '67%', 'border': '1px solid #444', 'position': 'relative'}, children=[
            dash_vtk.View(
                id='vtk-view', 
                background=[0.05, 0.05, 0.05], 
                # FIXED: Camera looking at equator from the X-axis, Z-axis points Up
                cameraPosition=[R_MOON * 3.5, 0, 0], 
                cameraViewUp=[0, 0, 1],
                pickingModes=['click'],
                children=[
                    dash_vtk.GeometryRepresentation(
                        property={'pointSize': 6, 'lighting': False}, 
                        children=[dash_vtk.PolyData(points=MOON_PTS_FLAT, verts=MOON_VERTS_FLAT, children=[dash_vtk.PointData([dash_vtk.DataArray(id='moon-colors-array', registration='setScalars', name='colors', values=BASE_COLORS_FLAT, type='Uint8Array', numberOfComponents=3)])])]
                    ),
                    dash_vtk.GeometryRepresentation(property={'color': [0.5, 0.5, 0.5], 'lighting': False, 'lineWidth': 1}, children=[dash_vtk.PolyData(points=ref_pts, lines=ref_lines)]),
                    dash_vtk.GeometryRepresentation(id='vtk-highlight-rep', property={'color': [0.0, 1.0, 0.0], 'lineWidth': 4, 'lighting': False}, children=[dash_vtk.PolyData(id='vtk-highlight-poly', points=[], lines=[])])
                ]
            )
        ])
    ]),
    
    # --- BOTTOM HALF (With Loading Spinner) ---
    html.Div(style={'flex': '1 1 50%', 'border': '1px solid #444', 'minHeight': 0, 'position': 'relative'}, children=[
        dcc.Loading(
            id="loading-spectra",
            type="dot",
            color="#00FF00",
            parent_style={'height': '100%'},
            style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center', 'height': '100%'},
            children=[dcc.Graph(id='spectrum-plot', style={'height': '100%'})]
        )
    ])
])

# Callbacks

@app.callback(Output('moon-colors-array', 'values'), Input('map-select', 'value'))
def update_moon_texture(selected_map):
    return BASE_COLORS_FLAT if selected_map == 'VISUAL' else generate_heatmap_colors(selected_map)

# Reset Camera Callback
@app.callback(
    [Output('vtk-view', 'cameraPosition'), 
     Output('vtk-view', 'cameraViewUp')],
    [Input('reset-camera-btn', 'n_clicks')]
)
def reset_camera(n_clicks):
    # Add a microscopic jitter to trick Dash into registering a state change
    eps = n_clicks * 1e-5
    return [R_MOON * 3.5, eps, eps], [0, 0, 1]

# 1. State Machine: Handles interaction and populates inputs
@app.callback(
    [Output('min-lat-input', 'value'), Output('max-lat-input', 'value'),
     Output('min-lon-input', 'value'), Output('max-lon-input', 'value'),
     Output('click-state', 'data'), Output('status-text', 'children'),
     Output('status-text', 'style'), Output('plot-trigger', 'data')],
    [Input('vtk-view', 'clickInfo'), Input('search-btn', 'n_clicks'), Input('interaction-mode', 'value')],
    [State('click-state', 'data'), State('min-lat-input', 'value'), State('max-lat-input', 'value'),
     State('min-lon-input', 'value'), State('max-lon-input', 'value'), State('plot-trigger', 'data')]
)
def handle_interactions(click_info, search_clicks, mode, state, in_min_lat, in_max_lat, in_min_lon, in_max_lon, plot_trigger):
    trigger = ctx.triggered_id
    
    # If user just changed the radio button, update the helper text
    if trigger == 'interaction-mode':
        if mode == 'single':
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, {'step': 0, 'lat1': None, 'lon1': None}, "Click the moon to plot a single point.", {'color': '#AAAAAA'}, dash.no_update
        else:
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, {'step': 1, 'lat1': None, 'lon1': None}, "🟢 Click your first corner on the map...", {'color': '#00FF00', 'fontWeight': 'bold'}, dash.no_update

    # Manual plot button triggers the plot
    if trigger == 'search-btn':
        return in_min_lat, in_max_lat, in_min_lon, in_max_lon, state, dash.no_update, dash.no_update, plot_trigger + 1

    # 3D Map Clicks
    if trigger == 'vtk-view' and click_info and 'worldPosition' in click_info:
        pos = click_info['worldPosition']
        lat, lon = get_lat_lon(pos[0], pos[1], pos[2])
        
        if mode == 'single':
            # Instantly populate just the min fields and trigger the plot
            return round(lat, 2), None, round(lon, 2), None, state, "✔️ Single point updated.", {'color': '#00AA00'}, plot_trigger + 1
            
        elif mode == 'region':
            if state['step'] == 1:
                return dash.no_update, dash.no_update, dash.no_update, dash.no_update, {'step': 2, 'lat1': lat, 'lon1': lon}, "🟡 Now click the opposite corner...", {'color': '#FFFF00', 'fontWeight': 'bold'}, dash.no_update
            elif state['step'] == 2:
                min_lat, max_lat = round(min(state['lat1'], lat), 2), round(max(state['lat1'], lat), 2)
                min_lon, max_lon = round(min(state['lon1'], lon), 2), round(max(state['lon1'], lon), 2)
                # Populate all 4 fields, reset step, and trigger plot
                return min_lat, max_lat, min_lon, max_lon, {'step': 1, 'lat1': None, 'lon1': None}, "✔️ Region bounds populated & plotted.", {'color': '#00AA00'}, plot_trigger + 1

    return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update


# 2. Main Plotting: Triggered invisibly by the plot-trigger data store
@app.callback(
    [Output('spectrum-plot', 'figure'),
     Output('vtk-highlight-poly', 'points'),
     Output('vtk-highlight-poly', 'lines'),
     Output('missing-data-warning', 'children'),
     Output('missing-data-warning', 'style'),
     Output('vtk-highlight-rep', 'property')], # NEW: Controls the 3D highlight color
    [Input('plot-trigger', 'data')],
    [State('min-lat-input', 'value'), State('max-lat-input', 'value'),
     State('min-lon-input', 'value'), State('max-lon-input', 'value'),
     State('altitude-select', 'value')]
)
def update_spectrum_and_highlight(trigger_val, min_lat, max_lat, min_lon, max_lon, selected_altitude):
    blank_fig = go.Figure().update_layout(template="plotly_dark", title="Click the moon or enter bounds to plot.", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
    hide_warning = {'display': 'none'}
    
    # Define our consistent color scheme based on Altitude
    if selected_altitude == 'High':
        plot_color = 'cyan'
        vtk_color = [0.0, 1.0, 1.0] # RGB for Cyan
    else:
        plot_color = 'magenta'
        vtk_color = [1.0, 0.0, 1.0] # RGB for Magenta
        
    highlight_prop = {'color': vtk_color, 'lineWidth': 4, 'lighting': False}
    
    if trigger_val == 0 or min_lat is None or min_lon is None:
        return blank_fig, [], [], "", hide_warning, dash.no_update

    df = df_moon_full[df_moon_full['altitude_phase'] == selected_altitude].reset_index(drop=True)
    if df.empty:
         return blank_fig.update_layout(title=f"No data available for {selected_altitude} altitude."), [], [], "", hide_warning, dash.no_update

    # Generate visual feedback boundaries
    highlight_pts, highlight_lines = create_highlight_geometry(min_lat, max_lat, min_lon, max_lon)

    # 1. Single Point Logic
    if max_lat is None or max_lon is None:
        target_pos = get_spherical_coords(min_lat, min_lon)
        x, y, z = get_spherical_coords(df['lat'].values, df['lon_norm'].values)
        tree = KDTree(np.column_stack((x, y, z)))
        dist, idx = tree.query(target_pos)
        
        if dist > 150: 
            return blank_fig.update_layout(title="No data found near that coordinate."), highlight_pts, highlight_lines, "", hide_warning, highlight_prop

        row = df.iloc[idx]
        if not (DATA_DIR / row['filename']).exists():
            warn = f"⚠️ The nearest file ({row['filename']}) is missing from {DATA_DIR}."
            return blank_fig.update_layout(title="Nearest data file missing."), highlight_pts, highlight_lines, warn, {'display': 'block', 'backgroundColor': '#856404', 'color': '#ffeeba', 'padding': '10px'}, highlight_prop

        spec = load_lp_spectrum(DATA_DIR / row['filename'], int(row['record_index']))
        if len(spec) == 0: return blank_fig.update_layout(title="Error reading spectrum array."), highlight_pts, highlight_lines, "", hide_warning, highlight_prop
        
        # Plot single spectrum with coordinated color
        fig = go.Figure(data=go.Scatter(y=spec, mode='lines', line=dict(color=plot_color)))
        fig.update_layout(title=f"Single Source (Nearest): Lat {row['lat']:.2f}°, Lon {row['lon']:.2f}°", yaxis_type="log", template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        return fig, highlight_pts, highlight_lines, "", hide_warning, highlight_prop

    # 2. Region Bounding Box Logic (Average Spectra)
    df_region = df[(df['lat'] >= min_lat) & (df['lat'] <= max_lat) & 
                   (df['lon_norm'] >= min_lon) & (df['lon_norm'] <= max_lon)]

    if df_region.empty:
        return blank_fig.update_layout(title="No data footprints found within that bounding box."), highlight_pts, highlight_lines, "", hide_warning, highlight_prop

    summed_spec = None
    successful_reads = 0
    missing_files_count = 0

    grouped_files = df_region.groupby('filename')

    for filename, group in grouped_files:
        filepath = DATA_DIR / filename
        if not filepath.exists():
            missing_files_count += 1
            continue
            
        try:
            struct = pds.read(str(filepath), lazy_load=False, quiet=True)
            iden = struct[0].id
            full_data = struct[iden].data["GROUP_0, Accepted Spectrum"]
            
            indices = group['record_index'].astype(int).values
            
            for idx in indices:
                spec = full_data[idx] if full_data.ndim > 1 else full_data
                if len(spec) > 0:
                    if summed_spec is None:
                        summed_spec = np.array(spec, dtype=float)
                    else:
                        summed_spec += np.array(spec, dtype=float)
                    successful_reads += 1
        except Exception as e:
            continue

    warning_text, warning_style = "", hide_warning
    if missing_files_count > 0:
        warning_text = f"{missing_files_count} required data file(s) for this region were not found in {DATA_DIR} and were excluded from the average."
        warning_style = {'display': 'block', 'backgroundColor': '#856404', 'color': '#ffeeba', 'padding': '10px'}

    if successful_reads == 0:
        return blank_fig.update_layout(title="No valid spectra could be loaded from this region."), highlight_pts, highlight_lines, warning_text, warning_style, highlight_prop

    avg_spec = summed_spec / successful_reads
    
    # Plot averaged spectrum with coordinated color
    fig = go.Figure(data=go.Scatter(y=avg_spec, mode='lines', line=dict(color=plot_color)))
    fig.update_layout(
        title=f"Averaged Spectrum ({selected_altitude} Altitude): {successful_reads} Datasets combined", 
        yaxis_type="log", template="plotly_dark",
        xaxis_title="Channel", yaxis_title="Average Counts (log)",
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig, highlight_pts, highlight_lines, warning_text, warning_style, highlight_prop

if __name__ == '__main__':
    app.run(debug=True)