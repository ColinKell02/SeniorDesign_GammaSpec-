import pandas as pd
import numpy as np
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output
import pds4_tools as pds
from pathlib import Path
from PIL import Image

# --- Configuration ---
INDEX_FILE = "spatial_library_full.csv"
DATA_DIR = Path("Moon/data")
TEXTURE_FILE = "moon_surface.jpg" 
R_MOON = 1737.4 

# --- Region Definitions ---
# You can add as many as you want here!
REGIONS = {
    "Global Overview": {
        "lat": [-90, 90], "lon": [-180, 360], 
        "downsample": 100, "res": 800,
        "desc": "The entire lunar surface."
    },
    "Lunar South Pole": {
        "lat": [-90, -65], "lon": [-180, 360], 
        "downsample": 10, "res": 1200,
        "desc": "Artemis target area. Investigating hydrogen signatures for water ice."
    },
    "Procellarum KREEP Terrane": {
        "lat": [0, 50], "lon": [-75, -10], 
        "downsample": 10, "res": 1200,
        "desc": "High radioactivity region enriched in Potassium, Rare Earth Elements, and Phosphorus."
    },
    "Mare Tranquillitatis": {
        "lat": [0, 25], "lon": [20, 45], 
        "downsample": 10, "res": 1200,
        "desc": "Apollo 11 landing site."
    }
}

# --- Data Loading ---
print("Loading spatial library...")
df_index = pd.read_csv(INDEX_FILE)
df_moon_full = df_index[df_index['mission'] == 'Moon'].copy()

# --- Parsing Logic ---
def load_lp_spectrum(xml_file, record_index):
    try:
        struct = pds.read(str(xml_file), lazy_load=False)
        iden = struct[0].id
        data = struct[iden].data
        spec = data["GROUP_0, Accepted Spectrum"]
        if spec.ndim > 1:
            return spec[record_index]
        return spec
    except Exception as e:
        print(f"Error loading spectrum from {xml_file.name}: {e}")
        return []

def get_spherical_coords(lat_deg, lon_deg, radius=R_MOON):
    lat_rad, lon_rad = np.radians(lat_deg), np.radians(lon_deg)
    x = radius * np.cos(lat_rad) * np.cos(lon_rad)
    y = radius * np.cos(lat_rad) * np.sin(lon_rad)
    z = radius * np.sin(lat_rad)
    return x, y, z

# --- Build Textured Sphere Geometries ---
def create_textured_moon(res):
    print(f"Pre-computing 3D Moon mesh at {res}x{res} resolution...")
    phi = np.linspace(0, 2*np.pi, res)
    theta = np.linspace(-np.pi/2, np.pi/2, res)
    phi, theta = np.meshgrid(phi, theta)
    xs = R_MOON * np.cos(theta) * np.cos(phi)
    ys = R_MOON * np.cos(theta) * np.sin(phi)
    zs = R_MOON * np.sin(theta)

    try:
        img = Image.open(TEXTURE_FILE).convert('L')
        img = img.resize((res, res)) 
        texture_data = np.flipud(np.array(img))
    except FileNotFoundError:
        texture_data = np.ones((res, res)) * 128

    return xs, ys, zs, texture_data

# Pre-calculate meshes so the dropdown is snappy
MESHES = {
    800: create_textured_moon(800),
    1200: create_textured_moon(1200)
}

# --- Initialize App ---
app = Dash(__name__)

app.layout = html.Div(style={'fontFamily': 'sans-serif', 'color': 'white', 'backgroundColor': '#111111', 'padding': '20px'}, children=[
    html.H1("Lunar Prospector Spatial Spectrometer"),
    
    # Dashboard Controls
    html.Div([
        html.Label("Select Lunar Region:", style={'fontWeight': 'bold', 'marginRight': '10px'}),
        dcc.Dropdown(
            id='region-select',
            options=[{'label': k, 'value': k} for k in REGIONS.keys()],
            value='Global Overview',
            clearable=False,
            style={'width': '300px', 'color': 'black', 'display': 'inline-block', 'verticalAlign': 'middle'}
        ),
        html.Div(id='region-desc', style={'marginTop': '10px', 'fontStyle': 'italic', 'color': '#cccccc'})
    ], style={'backgroundColor': '#222222', 'padding': '15px', 'borderRadius': '5px', 'marginBottom': '20px'}),
    
    # Graphs
    html.Div([
        html.Div([dcc.Graph(id='globe-map', style={'height': '75vh'})], style={'width': '58%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='spectrum-plot', style={'height': '75vh'})], style={'width': '40%', 'display': 'inline-block'})
    ])
])

@app.callback(
    [Output('globe-map', 'figure'), Output('region-desc', 'children')],
    Input('region-select', 'value')
)
def update_map(selected_region):
    config = REGIONS[selected_region]
    lat_min, lat_max = config['lat']
    lon_min, lon_max = config['lon']
    downsample = config['downsample']
    res = config['res']
    
    # Filter the dataframe for the region
    # The complex longitude check handles datasets using both 0-360 and -180 to 180 standard formats
    lat_mask = (df_moon_full['lat'] >= lat_min) & (df_moon_full['lat'] <= lat_max)
    lon_mask = ((df_moon_full['lon'] >= lon_min) & (df_moon_full['lon'] <= lon_max)) | \
               ((df_moon_full['lon'] >= lon_min + 360) & (df_moon_full['lon'] <= lon_max + 360))
    
    df_filtered = df_moon_full[lat_mask & lon_mask].copy()
    
    # Apply dynamic downsampling
    df_filtered = df_filtered.iloc[::downsample].copy()

    # Get track coordinates (with a 5km altitude offset to prevent clipping)
    all_x, all_y, all_z = get_spherical_coords(df_filtered['lat'].values, df_filtered['lon'].values, radius=R_MOON + 1.0)
    custom_data = df_filtered[['filename', 'record_index', 'lat', 'lon']].values

    # Fetch the correct pre-calculated mesh resolution
    xs, ys, zs, moon_tex = MESHES[res]

    fig_map = go.Figure()

    # Add Opaque Textured Surface
    fig_map.add_trace(go.Surface(
        x=xs, y=ys, z=zs,
        surfacecolor=moon_tex,
        colorscale='Greys',
        showscale=False,
        opacity=1.0,  
        hoverinfo='skip',
        lighting=dict(ambient=0.3, diffuse=0.8, roughness=0.9, specular=0.1, fresnel=0.1),
        lightposition=dict(x=2000, y=2000, z=1000), 
    ))

    # Add Orbital Tracks
    fig_map.add_trace(go.Scatter3d(
        x=all_x, y=all_y, z=all_z,
        mode='markers',
        marker=dict(size=2.5, color='cyan', opacity=1.0, line=dict(width=0)), # Optimized borders and opacity
        customdata=custom_data,  
        hovertemplate=(
            "<b>File:</b> %{customdata[0]}<br>"
            "<b>Index:</b> %{customdata[1]}<br>"
            "<b>Lat:</b> %{customdata[2]:.2f}°<br>"
            "<b>Lon:</b> %{customdata[3]:.2f}°<extra></extra>"
        )
    ))

    fig_map.update_layout(
        scene=dict(
            xaxis=dict(visible=False), yaxis=dict(visible=False), zaxis=dict(visible=False),
            aspectmode='data', bgcolor="black",
            camera=dict(eye=dict(x=1.5, y=1.5, z=1.5)) 
        ),
        margin=dict(l=0, r=0, b=0, t=0), template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
    )

    return fig_map, config['desc']

@app.callback(
    Output('spectrum-plot', 'figure'), 
    Input('globe-map', 'clickData')
)
def update_spectrum(clickData):
    if not clickData: 
        return go.Figure().update_layout(
            template="plotly_dark", 
            title="Click a track point on the Moon to view spectrum",
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
        )
    
    clicked_point_data = clickData['points'][0].get('customdata', None)
    if clicked_point_data is None:
        # User clicked the surface, not a track point
        return go.Figure().update_layout(template="plotly_dark", title="Please click a cyan track point, not the surface.")

    fname = clicked_point_data[0]
    record_index = int(clicked_point_data[1])
    lat = clicked_point_data[2]
    lon = clicked_point_data[3]
    
    spec = load_lp_spectrum(DATA_DIR / fname, record_index)
    
    if len(spec) == 0:
        return go.Figure().update_layout(template="plotly_dark", title="Error loading spectrum")

    fig = go.Figure(data=go.Scatter(y=spec, mode='markers', marker=dict(color='cyan')))
    fig.update_layout(
        title=f"File: {fname} | Index: {record_index}<br>Lat: {lat:.2f}°, Lon: {lon:.2f}°", 
        yaxis_type="log", 
        template="plotly_dark",
        xaxis_title="Channel",
        yaxis_title="Counts (log)",
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
    )
    return fig

if __name__ == '__main__':
    app.run(debug=True)