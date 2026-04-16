"""
Geospatial Analysis
====================
Cell tower mapping, suspect movement trajectories, co-location detection,
and interactive Folium map generation.
"""

import pandas as pd
import folium
from folium.plugins import HeatMap, AntPath
from datetime import timedelta


def get_tower_locations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract unique tower locations with GPS coordinates from the CDR data.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned CDR DataFrame.

    Returns
    -------
    pd.DataFrame
        One row per unique tower with ID, location name, lat, lon, call_count.
    """
    required = {"Tower_ID", "Tower_Latitude", "Tower_Longitude"}
    if not required.issubset(df.columns):
        return pd.DataFrame()

    tower_df = (
        df.dropna(subset=["Tower_Latitude", "Tower_Longitude"])
        .groupby(["Tower_ID", "Tower_Location", "Tower_Latitude", "Tower_Longitude"])
        .size()
        .reset_index(name="call_count")
        .sort_values("call_count", ascending=False)
    )
    return tower_df


def get_movement_trajectory(df: pd.DataFrame, phone_number: str) -> pd.DataFrame:
    """
    Reconstruct the movement trajectory of a phone number over time.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned CDR DataFrame.
    phone_number : str
        The phone number to track.

    Returns
    -------
    pd.DataFrame
        Chronological sequence of tower locations for this number.
    """
    required = {"Tower_Latitude", "Tower_Longitude", "Tower_ID", "Tower_Location"}
    if not required.issubset(df.columns):
        return pd.DataFrame()

    # Only use records where this number is the CALLER — the tower field
    # represents the caller's physical location, not the callee's.
    mask = df["Calling_Number"] == phone_number
    subset = df[mask].dropna(subset=["Tower_Latitude", "Tower_Longitude"]).copy()

    subset.sort_values("Datetime", inplace=True)
    subset.reset_index(drop=True, inplace=True)

    return subset[["Datetime", "Tower_ID", "Tower_Location", "Tower_Latitude", "Tower_Longitude", "Call_Type", "Duration_sec"]].copy()


def detect_colocation(df: pd.DataFrame, window_minutes: int = 30) -> pd.DataFrame:
    """
    Detect when two different phone numbers were at the same cell tower
    within a given time window — indicating a potential physical meeting.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned CDR DataFrame.
    window_minutes : int
        Time window (in minutes) within which co-presence counts as co-location.

    Returns
    -------
    pd.DataFrame
        Each row is a co-location event: number_a, number_b, tower, time_a, time_b, gap_minutes.
    """
    required = {"Tower_ID", "Tower_Latitude", "Tower_Longitude"}
    if not required.issubset(df.columns):
        return pd.DataFrame()

    window = timedelta(minutes=window_minutes)

    # Build per-tower event list
    tower_events = {}
    for _, row in df.dropna(subset=["Tower_ID"]).iterrows():
        tower = row["Tower_ID"]
        if tower not in tower_events:
            tower_events[tower] = []
        tower_events[tower].append({
            "phone": row["Calling_Number"],
            "datetime": row["Datetime"],
            "lat": row.get("Tower_Latitude"),
            "lon": row.get("Tower_Longitude"),
            "location": row.get("Tower_Location", tower),
        })

    results = []
    for tower, events in tower_events.items():
        events.sort(key=lambda e: e["datetime"])
        for i in range(len(events)):
            for j in range(i + 1, len(events)):
                e1, e2 = events[i], events[j]
                if e2["datetime"] - e1["datetime"] > window:
                    break
                if e1["phone"] == e2["phone"]:
                    continue
                gap = abs((e2["datetime"] - e1["datetime"]).total_seconds() / 60)
                results.append({
                    "phone_a": e1["phone"],
                    "phone_b": e2["phone"],
                    "tower_id": tower,
                    "tower_location": e1["location"],
                    "tower_lat": e1["lat"],
                    "tower_lon": e1["lon"],
                    "time_a": e1["datetime"],
                    "time_b": e2["datetime"],
                    "gap_minutes": round(gap, 1),
                })

    if not results:
        return pd.DataFrame()

    coloc_df = pd.DataFrame(results)
    # Deduplicate — keep shortest gap for each (a, b, tower) pair
    coloc_df = (
        coloc_df.sort_values("gap_minutes")
        .drop_duplicates(subset=["phone_a", "phone_b", "tower_id"])
        .reset_index(drop=True)
    )
    return coloc_df


def build_map(
    df: pd.DataFrame,
    colocation_df: pd.DataFrame = None,
    track_number: str = None,
    trajectory_df: pd.DataFrame = None,
) -> folium.Map:
    """
    Build an interactive Folium map with:
    - Tower markers (sized by call volume)
    - Heatmap of communication activity
    - Optional: movement trajectory for a tracked number
    - Optional: co-location event markers

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned CDR DataFrame.
    colocation_df : pd.DataFrame, optional
        Output of detect_colocation().
    track_number : str, optional
        Phone number to draw trajectory for.
    trajectory_df : pd.DataFrame, optional
        Output of get_movement_trajectory() for track_number.

    Returns
    -------
    folium.Map
        Interactive map object (call .save() or display in Streamlit).
    """
    tower_df = get_tower_locations(df)
    if tower_df.empty:
        return folium.Map(location=[20.5937, 78.9629], zoom_start=5)

    # If tracking a specific number, centre the map on that number's towers.
    # Otherwise centre on the full dataset's tower spread.
    if trajectory_df is not None and not trajectory_df.empty:
        focus_lats = trajectory_df["Tower_Latitude"].dropna()
        focus_lons = trajectory_df["Tower_Longitude"].dropna()
    else:
        focus_lats = tower_df["Tower_Latitude"]
        focus_lons = tower_df["Tower_Longitude"]

    center_lat = float(focus_lats.mean())
    center_lon = float(focus_lons.mean())

    # Compute zoom from the coordinate spread so the map fits the data
    lat_spread = float(focus_lats.max() - focus_lats.min())
    lon_spread = float(focus_lons.max() - focus_lons.min())
    spread = max(lat_spread, lon_spread)
    if spread < 0.5:
        zoom = 12
    elif spread < 2:
        zoom = 10
    elif spread < 5:
        zoom = 8
    elif spread < 10:
        zoom = 7
    else:
        zoom = 6

    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom, tiles="CartoDB positron")

    # --- Heatmap layer ---
    heat_data = [
        [float(row["Tower_Latitude"]), float(row["Tower_Longitude"]), int(row["call_count"])]
        for _, row in tower_df.iterrows()
    ]
    HeatMap(heat_data, radius=25, blur=15, name="Activity Heatmap").add_to(m)

    # --- Tower markers ---
    tower_layer = folium.FeatureGroup(name="Cell Towers")
    max_count = int(tower_df["call_count"].max())
    for _, row in tower_df.iterrows():
        call_count = int(row["call_count"])
        radius = 6 + 14 * (call_count / max_count)
        folium.CircleMarker(
            location=[float(row["Tower_Latitude"]), float(row["Tower_Longitude"])],
            radius=radius,
            color="#2563eb",
            fill=True,
            fill_color="#3b82f6",
            fill_opacity=0.7,
            popup=folium.Popup(
                f"<b>{row['Tower_ID']}</b><br>{row['Tower_Location']}<br>Calls: {call_count}",
                max_width=200,
            ),
            tooltip=row["Tower_Location"],
        ).add_to(tower_layer)
    tower_layer.add_to(m)

    # --- Movement trajectory ---
    if trajectory_df is not None and not trajectory_df.empty:
        traj_layer = folium.FeatureGroup(name=f"Trajectory: {track_number}")

        # Deduplicate: build movement path from unique towers in order of FIRST VISIT
        # This prevents the full-mesh problem where every call record = a new line
        seen_towers = []
        seen_set = set()
        for _, row in trajectory_df.iterrows():
            tid = row["Tower_ID"]
            if tid not in seen_set:
                seen_set.add(tid)
                seen_towers.append(row)

        # Draw path only through unique towers in chronological order
        unique_coords = [(r["Tower_Latitude"], r["Tower_Longitude"]) for r in seen_towers]
        if len(unique_coords) >= 2:
            AntPath(
                locations=unique_coords,
                color="#ef4444",
                weight=3,
                delay=800,
                tooltip=f"Movement path: {track_number}",
            ).add_to(traj_layer)

        # Markers at each unique tower visited, sized by visit count
        tower_visit_counts = trajectory_df["Tower_ID"].value_counts()
        for row in seen_towers:
            # Cast to int to avoid numpy int64 JSON serialization error
            visit_count = int(tower_visit_counts.get(row["Tower_ID"], 1))
            radius = 6 + min(visit_count, 10)
            lat = float(row["Tower_Latitude"])
            lon = float(row["Tower_Longitude"])
            folium.CircleMarker(
                location=[lat, lon],
                radius=radius,
                color="#ef4444",
                fill=True,
                fill_color="#fca5a5",
                fill_opacity=0.9,
                popup=folium.Popup(
                    f"<b>{track_number}</b><br>"
                    f"First seen: {row['Datetime']}<br>"
                    f"{row['Tower_Location']}<br>"
                    f"Total visits: {visit_count}",
                    max_width=240,
                ),
                tooltip=f"{row['Tower_Location']} ({visit_count} visits)",
            ).add_to(traj_layer)
        traj_layer.add_to(m)

    # --- Co-location markers ---
    if colocation_df is not None and not colocation_df.empty:
        coloc_layer = folium.FeatureGroup(name="Co-location Events")
        for _, row in colocation_df.dropna(subset=["tower_lat", "tower_lon"]).iterrows():
            folium.Marker(
                location=[float(row["tower_lat"]), float(row["tower_lon"])],
                icon=folium.Icon(color="red", icon="exclamation-sign", prefix="glyphicon"),
                popup=folium.Popup(
                    f"<b>Meeting Detected</b><br>"
                    f"{row['phone_a']} & {row['phone_b']}<br>"
                    f"{row['tower_location']}<br>"
                    f"Gap: {float(row['gap_minutes'])} min",
                    max_width=230,
                ),
                tooltip="Co-location Event",
            ).add_to(coloc_layer)
        coloc_layer.add_to(m)

    folium.LayerControl().add_to(m)
    return m
