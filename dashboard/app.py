"""
NileFlow — Milestone 4
Live Congestion Intelligence Dashboard

Streamlit dashboard showing:
  - Folium map with color-coded corridor congestion + live vehicle markers
  - Plotly time-series charts (congestion index + speed)
  - Live alert feed from Elasticsearch
  - Weather sidebar with Cairo & Alexandria conditions

Auto-refreshes every 10 seconds.

Run:
    streamlit run dashboard/app.py
"""

import json
import os
import sys
import time
import base64
from datetime import datetime, timezone, timedelta
from pathlib import Path

import folium
import plotly.graph_objects as go
import redis as redis_lib
import requests
import streamlit as st
from cassandra.cluster import Cluster
from streamlit_folium import st_folium

# ---------------------------------------------------------------------------
# Page config — MUST be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="NileFlow — Congestion Intelligence",
    page_icon="\U0001F30A",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "nileflow")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

CORRIDORS = [
    {"id": "ring_road", "name": "Ring Road", "city": "Cairo",
     "start": {"lat": 30.0561, "lon": 31.3467}, "end": {"lat": 30.0131, "lon": 31.2089}},
    {"id": "corniche_cairo", "name": "Corniche El Nil", "city": "Cairo",
     "start": {"lat": 30.0459, "lon": 31.2243}, "end": {"lat": 30.0029, "lon": 31.2297}},
    {"id": "october_bridge", "name": "6th of October Bridge", "city": "Cairo",
     "start": {"lat": 30.0554, "lon": 31.2235}, "end": {"lat": 30.0434, "lon": 31.2015}},
    {"id": "salah_salem", "name": "Salah Salem Road", "city": "Cairo",
     "start": {"lat": 30.0724, "lon": 31.2834}, "end": {"lat": 30.0281, "lon": 31.2611}},
    {"id": "july26", "name": "26th of July Corridor", "city": "Cairo",
     "start": {"lat": 30.0609, "lon": 31.2003}, "end": {"lat": 30.0764, "lon": 31.1177}},
    {"id": "alex_corniche", "name": "Alexandria Corniche", "city": "Alexandria",
     "start": {"lat": 31.2135, "lon": 29.8854}, "end": {"lat": 31.2017, "lon": 29.9533}},
]

CORRIDOR_MAP = {c["id"]: c for c in CORRIDORS}

WEATHER_CODES = {
    0: ("Clear sky", "☀️"),
    1: ("Mainly clear", "\U0001F324️"),
    2: ("Partly cloudy", "⛅"),
    3: ("Overcast", "☁️"),
    45: ("Foggy", "\U0001F32B️"),
    48: ("Rime fog", "\U0001F32B️"),
    51: ("Light drizzle", "\U0001F326️"),
    53: ("Moderate drizzle", "\U0001F326️"),
    55: ("Dense drizzle", "\U0001F327️"),
    61: ("Slight rain", "\U0001F327️"),
    63: ("Moderate rain", "\U0001F327️"),
    65: ("Heavy rain", "⛈️"),
    80: ("Slight showers", "\U0001F326️"),
    81: ("Moderate showers", "\U0001F327️"),
    82: ("Violent showers", "⛈️"),
    95: ("Thunderstorm", "⚡"),
}

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
LOGO_PATH = Path(__file__).resolve().parent.parent / "assets" / "nileflow-logo.png"


def load_logo_base64() -> str:
    if LOGO_PATH.exists():
        return base64.b64encode(LOGO_PATH.read_bytes()).decode()
    return ""


LOGO_B64 = load_logo_base64()

st.markdown("""
<style>
    /* ── Global ── */
    .stApp {
        background: linear-gradient(180deg, #0D1117 0%, #161B22 100%);
    }

    /* ── Sidebar ── */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0D1117 0%, #0F1923 100%);
        border-right: 1px solid #1A6DFF33;
    }
    section[data-testid="stSidebar"] .stMarkdown p,
    section[data-testid="stSidebar"] .stMarkdown li,
    section[data-testid="stSidebar"] .stMarkdown h1,
    section[data-testid="stSidebar"] .stMarkdown h2,
    section[data-testid="stSidebar"] .stMarkdown h3 {
        color: #C9D1D9 !important;
    }

    /* ── Metric cards ── */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #161B22 0%, #1C2333 100%);
        border: 1px solid #30363D;
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 4px 16px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.03);
        transition: transform 0.2s, box-shadow 0.2s;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(26,109,255,0.15), inset 0 1px 0 rgba(255,255,255,0.03);
    }
    div[data-testid="stMetric"] label {
        color: #8B949E !important;
        font-size: 0.85rem !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #F0F6FC !important;
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }

    /* ── Headers ── */
    .dashboard-header {
        display: flex;
        align-items: center;
        gap: 16px;
        padding: 8px 0 16px 0;
    }
    .dashboard-header img { height: 50px; }
    .dashboard-header h1 {
        margin: 0;
        font-size: 1.8rem;
        background: linear-gradient(90deg, #1A6DFF, #00D4AA);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
    }
    .section-title {
        color: #F0F6FC;
        font-size: 1.2rem;
        font-weight: 700;
        margin: 24px 0 12px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid #1A6DFF44;
    }
    .section-icon { margin-right: 8px; }

    /* ── Pulsing live dot ── */
    @keyframes pulse-glow {
        0%   { box-shadow: 0 0 4px #00D4AA, 0 0 8px #00D4AA44; }
        50%  { box-shadow: 0 0 12px #00D4AA, 0 0 24px #00D4AA66; }
        100% { box-shadow: 0 0 4px #00D4AA, 0 0 8px #00D4AA44; }
    }
    .status-pill {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .status-live {
        background: #00D4AA22;
        color: #00D4AA;
        border: 1px solid #00D4AA44;
        animation: pulse-glow 2s ease-in-out infinite;
    }

    /* ── Vehicle count badge ── */
    .vehicle-badge {
        background: linear-gradient(135deg, #1A6DFF22 0%, #00D4AA22 100%);
        border: 1px solid #1A6DFF44;
        border-radius: 12px;
        padding: 12px 16px;
        text-align: center;
        margin-top: 8px;
    }
    .vehicle-badge .vb-count {
        font-size: 1.8rem;
        font-weight: 800;
        color: #58A6FF;
    }
    .vehicle-badge .vb-label {
        font-size: 0.75rem;
        color: #8B949E;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    /* ── Alert cards ── */
    .alert-card {
        background: linear-gradient(135deg, #1C1F2E 0%, #21162B 100%);
        border: 1px solid #FF444433;
        border-left: 4px solid #FF4444;
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 8px;
    }
    .alert-card .alert-corridor {
        color: #F0F6FC;
        font-weight: 700;
        font-size: 0.95rem;
    }
    .alert-card .alert-detail {
        color: #8B949E;
        font-size: 0.8rem;
        margin-top: 4px;
    }

    /* ── Weather cards ── */
    .weather-card {
        background: linear-gradient(135deg, #161B22 0%, #1A2332 100%);
        border: 1px solid #30363D;
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 12px;
        text-align: center;
    }
    .weather-city { color: #58A6FF; font-size: 1rem; font-weight: 700; margin-bottom: 4px; }
    .weather-temp { color: #F0F6FC; font-size: 2rem; font-weight: 800; }
    .weather-emoji { font-size: 2.5rem; }
    .weather-detail { color: #8B949E; font-size: 0.8rem; margin-top: 6px; }

    /* ── Corridor table ── */
    .corridor-table {
        width: 100%;
        border-collapse: separate;
        border-spacing: 0;
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid #30363D;
    }
    .corridor-table th {
        background: linear-gradient(135deg, #1A6DFF 0%, #1555CC 100%);
        color: #FFFFFF;
        padding: 10px 14px;
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        text-align: left;
    }
    .corridor-table td {
        padding: 10px 14px;
        color: #C9D1D9;
        font-size: 0.85rem;
        border-bottom: 1px solid #21262D;
    }
    .corridor-table tr:nth-child(even) td { background: #161B22; }
    .corridor-table tr:nth-child(odd) td { background: #0D1117; }
    .corridor-table tr:hover td {
        background: #1A6DFF11 !important;
    }

    /* ── Hide Streamlit defaults ── */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    div[data-testid="stToolbar"] {display: none;}

    /* ── Plotly chart backgrounds ── */
    .stPlotlyChart {
        border: 1px solid #30363D;
        border-radius: 12px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Data access helpers
# ---------------------------------------------------------------------------
@st.cache_resource
def get_cassandra_session():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    return session


def cassandra_execute(query: str, params=None):
    """Execute with one retry: the cached session dies if Cassandra restarts,
    so rebuild it once before giving up. Returns [] when Cassandra is down
    instead of crashing the whole page."""
    try:
        return get_cassandra_session().execute(query, params)
    except Exception:
        get_cassandra_session.clear()
        try:
            return get_cassandra_session().execute(query, params)
        except Exception:
            return []


@st.cache_resource
def get_redis_client():
    try:
        client = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        client.ping()
        return client
    except Exception:
        return None


def query_congestion_metrics(hours: int = 2) -> list[dict]:
    results = []
    for corridor in CORRIDORS:
        rows = cassandra_execute(
            "SELECT corridor_id, event_time, congestion_index, speed_kmh, is_anomaly "
            "FROM congestion_metrics WHERE corridor_id = %s "
            "AND event_time > %s ORDER BY event_time DESC",
            (corridor["id"], datetime.now(timezone.utc) - timedelta(hours=hours)),
        )
        for row in rows:
            results.append({
                "corridor_id": row.corridor_id,
                "event_time": row.event_time,
                "congestion_index": row.congestion_index or 0,
                "speed_kmh": row.speed_kmh or 0,
                "is_anomaly": row.is_anomaly or False,
            })
    return results


def query_latest_per_corridor() -> dict:
    latest = {}
    for corridor in CORRIDORS:
        rows = cassandra_execute(
            "SELECT corridor_id, event_time, congestion_index, speed_kmh, is_anomaly "
            "FROM congestion_metrics WHERE corridor_id = %s LIMIT 1",
            (corridor["id"],),
        )
        for row in rows:
            latest[row.corridor_id] = {
                "congestion_index": row.congestion_index or 0,
                "speed_kmh": row.speed_kmh or 0,
                "is_anomaly": row.is_anomaly or False,
                "event_time": row.event_time,
            }
    return latest


def query_weather() -> list[dict]:
    results = []
    for loc in ["Cairo", "Alexandria"]:
        rows = cassandra_execute(
            "SELECT location, event_time, temperature_c, humidity_pct, "
            "precipitation_mm, wind_speed_kmh, weather_code "
            "FROM weather_metrics WHERE location = %s LIMIT 1",
            (loc,),
        )
        for row in rows:
            results.append({
                "location": row.location,
                "event_time": row.event_time,
                "temperature_c": row.temperature_c,
                "humidity_pct": row.humidity_pct,
                "precipitation_mm": row.precipitation_mm,
                "wind_speed_kmh": row.wind_speed_kmh,
                "weather_code": row.weather_code,
            })
    return results


def query_alerts(limit: int = 20) -> list[dict]:
    try:
        resp = requests.post(
            f"{ELASTICSEARCH_URL}/congestion_alerts/_search",
            json={
                "size": limit,
                "sort": [{"created_at": {"order": "desc"}}],
                "query": {"match_all": {}},
            },
            timeout=5,
        )
        if resp.status_code == 200:
            hits = resp.json().get("hits", {}).get("hits", [])
            return [h["_source"] for h in hits]
    except Exception:
        pass
    return []


def query_vehicle_positions() -> list[dict]:
    client = get_redis_client()
    if not client:
        return []
    try:
        raw = client.get("nileflow:vehicle_positions")
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return []


# ---------------------------------------------------------------------------
# Color helpers
# ---------------------------------------------------------------------------
def congestion_color(idx: float) -> str:
    if idx > 0.7:
        return "#FF4444"
    if idx > 0.4:
        return "#FFA500"
    if idx > 0.2:
        return "#FFD700"
    return "#00D4AA"


def congestion_label(idx: float) -> str:
    if idx > 0.7:
        return "Severe"
    if idx > 0.4:
        return "Moderate"
    if idx > 0.2:
        return "Light"
    return "Free Flow"


# ---------------------------------------------------------------------------
# Sidebar — weather + logo + system info
# ---------------------------------------------------------------------------
with st.sidebar:
    if LOGO_B64:
        st.markdown(
            f'<div style="text-align:center; padding: 8px 0 16px 0;">'
            f'<img src="data:image/png;base64,{LOGO_B64}" style="width: 85%;" />'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown(
        '<div style="text-align:center;">'
        '<span class="status-pill status-live">LIVE</span>'
        '</div>',
        unsafe_allow_html=True,
    )

    st.markdown("---")

    st.markdown("### ☁️ Weather Conditions")

    weather_data = query_weather()
    if weather_data:
        for w in weather_data:
            code = w.get("weather_code", 0)
            desc, emoji = WEATHER_CODES.get(code, ("Unknown", "❓"))
            temp = w.get("temperature_c", 0)
            humidity = w.get("humidity_pct", 0)
            wind = w.get("wind_speed_kmh", 0)
            precip = w.get("precipitation_mm", 0)

            st.markdown(f"""
            <div class="weather-card">
                <div class="weather-city">{w['location']}</div>
                <div class="weather-emoji">{emoji}</div>
                <div class="weather-temp">{temp:.1f}°C</div>
                <div class="weather-detail">{desc}</div>
                <div class="weather-detail">
                    \U0001F4A7 {humidity:.0f}% &nbsp;&nbsp;
                    \U0001F32C️ {wind:.1f} km/h &nbsp;&nbsp;
                    \U0001F327️ {precip:.1f} mm
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No weather data yet — start the weather producer.")

    st.markdown("---")

    # Vehicle count in sidebar
    vehicles = query_vehicle_positions()
    st.markdown(
        f'<div class="vehicle-badge">'
        f'<div class="vb-count">{len(vehicles)}</div>'
        f'<div class="vb-label">\U0001F697 Vehicles Tracked</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    st.markdown("---")

    st.markdown("### ⚙️ System")
    st.caption(f"Cassandra: `{CASSANDRA_HOST}`")
    st.caption(f"Elasticsearch: `{ELASTICSEARCH_URL}`")
    st.caption(f"Redis: `{REDIS_HOST}:{REDIS_PORT}`")
    st.caption(f"Last refresh: `{datetime.now().strftime('%H:%M:%S')}`")
    st.caption("Auto-refresh: 10s")

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
if LOGO_B64:
    st.markdown(
        f'<div class="dashboard-header">'
        f'<img src="data:image/png;base64,{LOGO_B64}" />'
        f'<h1>Congestion Intelligence Dashboard</h1>'
        f'</div>',
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        '<div class="dashboard-header">'
        '<h1>\U0001F30A NileFlow — Congestion Intelligence Dashboard</h1>'
        '</div>',
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------
latest = query_latest_per_corridor()

total_corridors = len(CORRIDORS)
monitored = len(latest)
anomaly_count = sum(1 for v in latest.values() if v.get("is_anomaly"))
avg_congestion = (
    sum(v["congestion_index"] for v in latest.values()) / monitored
    if monitored > 0 else 0
)
avg_speed = (
    sum(v["speed_kmh"] for v in latest.values()) / monitored
    if monitored > 0 else 0
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Corridors Monitored", f"{monitored}/{total_corridors}")
k2.metric("Avg Congestion", f"{avg_congestion:.2f}")
k3.metric("Avg Speed", f"{avg_speed:.1f} km/h")
k4.metric("Active Anomalies", f"{anomaly_count}", delta=f"{anomaly_count} corridors" if anomaly_count else None, delta_color="inverse")
k5.metric("Vehicles Tracked", f"{len(vehicles)}")

# ---------------------------------------------------------------------------
# Map — Cairo + Alexandria with vehicle markers
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title"><span class="section-icon">\U0001F5FA️</span>Live Congestion Map</div>', unsafe_allow_html=True)

map_col, table_col = st.columns([3, 2])

with map_col:
    all_lats = []
    all_lons = []
    for c in CORRIDORS:
        all_lats.extend([c["start"]["lat"], c["end"]["lat"]])
        all_lons.extend([c["start"]["lon"], c["end"]["lon"]])

    center_lat = sum(all_lats) / len(all_lats)
    center_lon = sum(all_lons) / len(all_lons)

    m = folium.Map(
        location=[center_lat, center_lon],
        tiles="CartoDB dark_matter",
        zoom_start=8,
    )

    m.fit_bounds([
        [min(all_lats) - 0.15, min(all_lons) - 0.15],
        [max(all_lats) + 0.15, max(all_lons) + 0.15],
    ])

    for corridor in CORRIDORS:
        cid = corridor["id"]
        data = latest.get(cid, {})
        idx = data.get("congestion_index", 0)
        spd = data.get("speed_kmh", 0)
        color = congestion_color(idx)
        label = congestion_label(idx)

        coords = [
            [corridor["start"]["lat"], corridor["start"]["lon"]],
            [corridor["end"]["lat"], corridor["end"]["lon"]],
        ]

        popup_html = (
            f"<div style='font-family: sans-serif; min-width: 180px;'>"
            f"<b style='font-size: 14px;'>{corridor['name']}</b><br>"
            f"<span style='color: {color}; font-weight: bold;'>{label}</span><br>"
            f"<hr style='margin: 4px 0;'>"
            f"Congestion: <b>{idx:.2f}</b><br>"
            f"Speed: <b>{spd:.1f}</b> km/h<br>"
            f"City: {corridor['city']}"
            f"</div>"
        )

        folium.PolyLine(
            coords, color=color, weight=6, opacity=0.9,
            popup=folium.Popup(popup_html, max_width=250),
            tooltip=f"{corridor['name']} — {label}",
        ).add_to(m)

        # Glow effect — wider transparent line behind
        folium.PolyLine(
            coords, color=color, weight=14, opacity=0.15,
        ).add_to(m)

        for coord in coords:
            folium.CircleMarker(
                coord, radius=6, color=color,
                fill=True, fill_opacity=0.9,
                weight=2,
            ).add_to(m)

    # Vehicle markers from Redis
    if vehicles:
        for v in vehicles:
            lat = v.get("latitude")
            lon = v.get("longitude")
            spd = v.get("speed_kmh", 0)
            vid = v.get("vehicle_id", "")
            cid = v.get("corridor_id", "")

            if lat is None or lon is None:
                continue

            v_color = "#FF4444" if spd < 15 else "#FFD700" if spd < 30 else "#00D4AA"

            folium.CircleMarker(
                [lat, lon],
                radius=4,
                color=v_color,
                fill=True,
                fill_color=v_color,
                fill_opacity=0.85,
                weight=1,
                tooltip=f"\U0001F697 {vid} | {spd:.0f} km/h",
            ).add_to(m)

    st_folium(m, use_container_width=True, height=520)

with table_col:
    st.markdown('<div class="section-title"><span class="section-icon">\U0001F6E3️</span>Corridor Status</div>', unsafe_allow_html=True)

    table_html = '<table class="corridor-table"><thead><tr><th>Corridor</th><th>Congestion</th><th>Speed</th><th>Status</th></tr></thead><tbody>'
    for corridor in CORRIDORS:
        cid = corridor["id"]
        data = latest.get(cid, {})
        idx = data.get("congestion_index", 0)
        spd = data.get("speed_kmh", 0)
        color = congestion_color(idx)
        label = congestion_label(idx)
        anomaly = data.get("is_anomaly", False)

        status_dot = "\U0001F534" if anomaly else "\U0001F7E2"

        table_html += (
            f"<tr>"
            f"<td><b>{corridor['name']}</b><br>"
            f"<span style='color:#8B949E;font-size:0.75rem;'>{corridor['city']}</span></td>"
            f"<td><span style='color:{color};font-weight:bold;'>{idx:.2f}</span></td>"
            f"<td>{spd:.1f} km/h</td>"
            f"<td>{status_dot} {label}</td>"
            f"</tr>"
        )
    table_html += "</tbody></table>"
    st.markdown(table_html, unsafe_allow_html=True)

    # Vehicle summary per corridor
    if vehicles:
        st.markdown('<div class="section-title" style="margin-top:20px;"><span class="section-icon">\U0001F697</span>Vehicle Distribution</div>', unsafe_allow_html=True)
        corridor_vehicle_counts = {}
        for v in vehicles:
            cid = v.get("corridor_id", "")
            corridor_vehicle_counts[cid] = corridor_vehicle_counts.get(cid, 0) + 1

        dist_html = '<table class="corridor-table"><thead><tr><th>Corridor</th><th>Vehicles</th><th>Avg Speed</th></tr></thead><tbody>'
        for corridor in CORRIDORS:
            cid = corridor["id"]
            count = corridor_vehicle_counts.get(cid, 0)
            corridor_vehicles = [v for v in vehicles if v.get("corridor_id") == cid]
            avg_v_speed = sum(v.get("speed_kmh", 0) for v in corridor_vehicles) / max(len(corridor_vehicles), 1)
            dist_html += (
                f"<tr><td><b>{corridor['name']}</b></td>"
                f"<td>{count}</td>"
                f"<td>{avg_v_speed:.1f} km/h</td></tr>"
            )
        dist_html += "</tbody></table>"
        st.markdown(dist_html, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Time-series charts
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title"><span class="section-icon">\U0001F4C8</span>Real-Time Trends (Last 2 Hours)</div>', unsafe_allow_html=True)

metrics = query_congestion_metrics(hours=2)

chart_left, chart_right = st.columns(2)

CHART_COLORS = ["#1A6DFF", "#00D4AA", "#FF6B6B", "#FFD93D", "#A855F7", "#F97316"]
CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#0D1117",
    font=dict(color="#C9D1D9", family="Inter, sans-serif"),
    margin=dict(l=50, r=20, t=40, b=40),
    legend=dict(
        bgcolor="rgba(22,27,34,0.8)",
        bordercolor="#30363D",
        borderwidth=1,
        font=dict(size=11),
    ),
    xaxis=dict(gridcolor="#21262D", linecolor="#30363D"),
    yaxis=dict(gridcolor="#21262D", linecolor="#30363D"),
    hovermode="x unified",
)

with chart_left:
    fig_congestion = go.Figure()

    for i, corridor in enumerate(CORRIDORS):
        cid = corridor["id"]
        corridor_data = [m for m in metrics if m["corridor_id"] == cid]
        corridor_data.sort(key=lambda x: x["event_time"])

        if corridor_data:
            fig_congestion.add_trace(go.Scatter(
                x=[d["event_time"] for d in corridor_data],
                y=[d["congestion_index"] for d in corridor_data],
                mode="lines+markers",
                name=corridor["name"],
                line=dict(color=CHART_COLORS[i % len(CHART_COLORS)], width=2),
                marker=dict(size=4),
            ))

    fig_congestion.add_hline(
        y=0.7, line_dash="dash", line_color="#FF4444",
        annotation_text="Anomaly Threshold",
        annotation_font_color="#FF4444",
    )

    fig_congestion.update_layout(
        title=dict(text="Congestion Index", font=dict(size=16, color="#F0F6FC")),
        yaxis_title="Index",
        **CHART_LAYOUT,
    )
    st.plotly_chart(fig_congestion, use_container_width=True)

with chart_right:
    fig_speed = go.Figure()

    for i, corridor in enumerate(CORRIDORS):
        cid = corridor["id"]
        corridor_data = [m for m in metrics if m["corridor_id"] == cid]
        corridor_data.sort(key=lambda x: x["event_time"])

        if corridor_data:
            fig_speed.add_trace(go.Scatter(
                x=[d["event_time"] for d in corridor_data],
                y=[d["speed_kmh"] for d in corridor_data],
                mode="lines+markers",
                name=corridor["name"],
                line=dict(color=CHART_COLORS[i % len(CHART_COLORS)], width=2),
                marker=dict(size=4),
            ))

    fig_speed.add_hline(
        y=15, line_dash="dash", line_color="#FF4444",
        annotation_text="Slow Threshold",
        annotation_font_color="#FF4444",
    )

    fig_speed.update_layout(
        title=dict(text="Average Speed", font=dict(size=16, color="#F0F6FC")),
        yaxis_title="km/h",
        **CHART_LAYOUT,
    )
    st.plotly_chart(fig_speed, use_container_width=True)

# ---------------------------------------------------------------------------
# Alert feed
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title"><span class="section-icon">\U0001F6A8</span>Recent Alerts</div>', unsafe_allow_html=True)

alerts = query_alerts(limit=20)

if alerts:
    alert_cols = st.columns(2)
    for i, alert in enumerate(alerts):
        corridor_id = alert.get("corridor_id", "unknown")
        corridor_name = CORRIDOR_MAP.get(corridor_id, {}).get("name", corridor_id)
        idx = alert.get("congestion_index", 0)
        spd = alert.get("speed_kmh", 0)
        event_time = alert.get("event_time", "N/A")

        color = congestion_color(idx)

        with alert_cols[i % 2]:
            st.markdown(f"""
            <div class="alert-card">
                <div class="alert-corridor">
                    \U0001F6A8 {corridor_name}
                    <span style="float:right; color:{color}; font-size:0.85rem;">{congestion_label(idx)}</span>
                </div>
                <div class="alert-detail">
                    Congestion: <b style="color:{color};">{idx:.2f}</b> &nbsp;|&nbsp;
                    Speed: <b>{spd:.1f}</b> km/h &nbsp;|&nbsp;
                    {event_time}
                </div>
            </div>
            """, unsafe_allow_html=True)
else:
    st.markdown(
        '<div style="text-align:center; padding:24px; color:#8B949E;">'
        '✅ No congestion alerts — all corridors flowing smoothly.'
        '</div>',
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown(
    '<div style="text-align:center; color:#484F58; font-size:0.75rem; padding: 8px 0;">'
    'NileFlow — Real-Time Traffic & Transit Congestion Intelligence Platform '
    '&nbsp;|&nbsp; Greater Cairo & Alexandria '
    '&nbsp;|&nbsp; DEPI Data Engineering Capstone'
    '</div>',
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Auto-refresh every 10 seconds
# ---------------------------------------------------------------------------
time.sleep(10)
st.rerun()
