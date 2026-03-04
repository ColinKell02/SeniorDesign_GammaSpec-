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

# Data Loading
print("Loading spatial library...")
df_index = pd.read_csv(INDEX_FILE)
df_moon_full = df_index[df_index['mission'] == 'Moon'].copy()

if 'altitude' in df_moon_full.columns:
    df_moon_full['altitude_phase'] = np.where(df_moon_full['altitude'] > 50, 'High', 'Low')
else:
    df_moon_full['altitude_phase'] = 'High'

# Load Elemental Abundances
print("Loading abundance library...")
try:
    df_abund = pd.read_csv(ABUNDANCE_FILE)
    # Filter to only use the highest resolution 2-degree map
    df_abund = df_abund[df_abund['SOURCE_FILE'].str.contains('2deg')].copy()
    
    # Calculate Center Lat/Lon based on the column names
    df_abund['CENTER_LAT'] = (df_abund['SOUTHERNMOST_LATITUDE'] + df_abund['NORTHERNMOST_LATITUDE']) / 2.0
    df_abund['CENTER_LON'] = (df_abund['WESTERNMOST_LONGITUDE'] + df_abund['EASTERNMOST_LONGITUDE']) / 2.0
    
    # Build a KDTree for the abundance map 
    abund_lat_rad = np.radians(df_abund['CENTER_LAT'].values)
    abund_lon_rad = np.radians(df_abund['CENTER_LON'].values)
    abund_x = R_MOON * np.cos(abund_lat_rad) * np.cos(abund_lon_rad)
    abund_y = R_MOON * np.cos(abund_lat_rad) * np.sin(abund_lon_rad)
    abund_z = R_MOON * np.sin(abund_lat_rad)
    ABUND_TREE = KDTree(np.column_stack((abund_x, abund_y, abund_z)))

    ELEMENTS = {
        'W_FEO': 'Iron (FeO)',
        'W_TH': 'Thorium (Th)',
        'W_TIO2': 'Titanium (TiO2)',
        'W_K': 'Potassium (K)',
        'W_AL2O3': 'Aluminum (Al2O3)'
    }
    # Filter ELEMENTS dict to only what exists in the CSV
    AVAILABLE_ELEMENTS = {k: v for k, v in ELEMENTS.items() if k in df_abund.columns}
except FileNotFoundError:
    print(f"Warning: {ABUNDANCE_FILE} not found. Heatmaps disabled.")
    df_abund = pd.DataFrame()
    AVAILABLE_ELEMENTS = {}


# --- Math Logic ---
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

# VTK Base Data
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

# Heatmap Generator
def generate_heatmap_colors(element_col):
    if df_abund.empty or element_col not in df_abund.columns:
        return BASE_COLORS_FLAT

    print(f"Generating heatmap for {element_col}...")
    
    # 1. Get the values and set up the colormap (e.g., 'plasma' or 'inferno')
    vals = df_abund[element_col].values
    vmin, vmax = np.percentile(vals, 2), np.percentile(vals, 98) # Clip outliers
    norm_vals = np.clip((vals - vmin) / (vmax - vmin), 0, 1)
    
    cmap = plt.get_cmap('plasma')
    rgb_map = (cmap(norm_vals)[:, :3] * 255).astype(np.uint8)
    
    # 2. Map every single VTK point (X_BASE, Y_BASE, Z_BASE) to the closest 2-degree abundance pixel
    coords = np.column_stack((X_BASE, Y_BASE, Z_BASE))
    dist, idx = ABUND_TREE.query(coords)
    
    # Extract the colors for each VTK point based on the KDTree lookup
    heatmap_colors = rgb_map[idx]
    
    # 3. Blend with the original grayscale image to keep the terrain visible! (50% blend)
    blended = (heatmap_colors * 0.5 + BASE_COLORS * 0.5).astype(np.uint8)
    
    return blended.flatten().tolist()

# Build Equator and Axis Lines
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

# Initialize App
app = Dash(__name__)

# Dropdown options include Visual + all available elements
view_options = [{'label': 'Visual (Grayscale)', 'value': 'VISUAL'}]
view_options.extend([{'label': v, 'value': k} for k, v in AVAILABLE_ELEMENTS.items()])


app.layout = html.Div(style={'fontFamily': 'sans-serif', 'color': 'white', 'backgroundColor': '#111111', 'padding': '20px'}, children=[
    html.H1("Lunar Prospector Spatial Spectrometer"),
    
    # Dashboard Controls
    html.Div([
        
        # NEW Map View Dropdown
        html.Label("Map Layer:", style={'fontWeight': 'bold', 'marginRight': '10px'}),
        dcc.Dropdown(
            id='map-select',
            options=view_options,
            value='VISUAL',
            clearable=False,
            style={'width': '220px', 'color': 'black', 'display': 'inline-block', 'verticalAlign': 'middle'}
        ),
        
        html.Label("Altitude Phase:", style={'fontWeight': 'bold', 'marginRight': '10px', 'marginLeft': '20px'}),
        dcc.Dropdown(
            id='altitude-select',
            options=[
                {'label': 'High Altitude (~100km)', 'value': 'High'},
                {'label': 'Low Altitude (~30km)', 'value': 'Low'}
            ],
            value='High',
            clearable=False,
            style={'width': '220px', 'color': 'black', 'display': 'inline-block', 'verticalAlign': 'middle'}
        ),
        
        html.Label("Search Coordinate:", style={'fontWeight': 'bold', 'marginRight': '10px', 'marginLeft': '20px'}),
        dcc.Input(id='lat-input', type='text', placeholder='Lat (-90 to 90)', style={'width': '120px', 'marginRight': '10px', 'padding': '5px', 'color': 'black'}),
        dcc.Input(id='lon-input', type='text', placeholder='Lon (-180 to 180)', style={'width': '130px', 'marginRight': '10px', 'padding': '5px', 'color': 'black'}),
        html.Button('Search Data', id='search-btn', n_clicks=0, style={'padding': '6px 15px', 'cursor': 'pointer', 'fontWeight': 'bold', 'backgroundColor': '#444', 'color': 'white', 'border': 'none', 'borderRadius': '3px'})
    
    ], style={'backgroundColor': '#222222', 'padding': '15px', 'borderRadius': '5px', 'marginBottom': '20px'}),
    
    html.Div([
        # LEFT SIDE: VTK 3D VIEW
        html.Div([
            dash_vtk.View(
                id='vtk-view',
                background=[0.05, 0.05, 0.05], 
                cameraPosition=[0, 0, R_MOON * 3.5],
                pickingModes=['click'],
                children=[
                    # 1. Solid Moon Surface (Colors controlled by callback now!)
                    dash_vtk.GeometryRepresentation(
                        property={'pointSize': 6, 'lighting': False}, 
                        children=[
                            dash_vtk.PolyData(
                                points=MOON_PTS_FLAT,
                                verts=MOON_VERTS_FLAT, 
                                children=[
                                    dash_vtk.PointData([
                                        dash_vtk.DataArray(
                                            id='moon-colors-array',
                                            registration='setScalars',
                                            name='colors',
                                            values=BASE_COLORS_FLAT,
                                            type='Uint8Array',
                                            numberOfComponents=3
                                        )
                                    ])
                                ]
                            )
                        ]
                    ),
                    # 2. Equator & Polar Axis Lines
                    dash_vtk.GeometryRepresentation(
                        property={'color': [0.8, 0.8, 0.8], 'lighting': False, 'lineWidth': 3},
                        children=[
                            dash_vtk.PolyData(
                                points=ref_pts,
                                lines=ref_lines
                            )
                        ]
                    )
                ]
            )
        ], style={'width': '58%', 'height': '75vh', 'display': 'inline-block', 'border': '1px solid #444'}),
        
        # RIGHT SIDE: PLOTLY SPECTRUM
        html.Div([
            dcc.Graph(id='spectrum-plot', style={'height': '75vh'})
        ], style={'width': '40%', 'display': 'inline-block', 'verticalAlign': 'top'})
    ])
])

# Callbacks

@app.callback(
    Output('moon-colors-array', 'values'),
    Input('map-select', 'value')
)
def update_moon_texture(selected_map):
    if selected_map == 'VISUAL':
        return BASE_COLORS_FLAT
    else:
        # Generate the heatmap on the fly
        return generate_heatmap_colors(selected_map)

@app.callback(
    Output('spectrum-plot', 'figure'), 
    [Input('vtk-view', 'clickInfo'),
     Input('search-btn', 'n_clicks')],
    [State('lat-input', 'value'),
     State('lon-input', 'value'),
     State('altitude-select', 'value')]
)
def update_spectrum(clickInfo, n_clicks, lat_val, lon_val, selected_altitude):
    trigger_id = ctx.triggered_id
    
    blank_fig = go.Figure().update_layout(
        template="plotly_dark", 
        title="Click the moon or search a coordinate to view spectra",
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
    )

    target_pos = None

    if trigger_id == 'vtk-view' and clickInfo and 'worldPosition' in clickInfo:
        target_pos = clickInfo['worldPosition']
        
    elif trigger_id == 'search-btn' and lat_val and lon_val:
        try:
            lat_f = float(lat_val)
            lon_f = float(lon_val)
            target_pos = get_spherical_coords(lat_f, lon_f, radius=R_MOON)
        except ValueError:
            return blank_fig.update_layout(title="Invalid coordinates. Please enter numbers.")
        
    if target_pos is None:
        return blank_fig

    df = df_moon_full[df_moon_full['altitude_phase'] == selected_altitude].reset_index(drop=True)
    
    if df.empty:
         return blank_fig.update_layout(title=f"No data available for {selected_altitude} altitude.")

    x, y, z = get_spherical_coords(df['lat'].values, df['lon'].values, radius=R_MOON)
    tree = KDTree(np.column_stack((x, y, z)))
    
    dist, idx = tree.query(target_pos)
    
    if dist > 150: 
        return blank_fig.update_layout(title="No data found near that coordinate.")

    row = df.iloc[idx]
    spec = load_lp_spectrum(DATA_DIR / row['filename'], int(row['record_index']))
    
    if len(spec) == 0:
        return blank_fig.update_layout(title="Error loading spectrum")
    
    plot_color = 'cyan' if selected_altitude == 'High' else 'magenta'
    
    fig = go.Figure(data=go.Scatter(y=spec, mode='markers', marker=dict(color=plot_color)))
    fig.update_layout(
        title=f"Matched Data Source: Lat {row['lat']:.2f}°, Lon {row['lon']:.2f}°", 
        yaxis_type="log", template="plotly_dark",
        xaxis_title="Channel", yaxis_title="Counts (log)",
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

if __name__ == '__main__':
    app.run(debug=True)