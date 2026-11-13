
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_PATH = PROJECT_ROOT / "data" / "maritime_analytics.db"

# Approximate port-centre coordinates and geofence radii.
# These are analytical zones, not official port boundaries.
PORTS = {
    "Hamburg": {
        "latitude": 53.54,
        "longitude": 9.97,
        "radius_km": 8,
    },
    "Rotterdam": {
        "latitude": 51.95,
        "longitude": 4.14,
        "radius_km": 8,
    },
    "Antwerp": {
        "latitude": 51.26,
        "longitude": 4.40,
        "radius_km": 7,
    },
    "Bremerhaven": {
        "latitude": 53.54,
        "longitude": 8.58,
        "radius_km": 6,
    },
    "Wilhelmshaven": {
        "latitude": 53.52,
        "longitude": 8.14,
        "radius_km": 6,
    },
    "Felixstowe": {
        "latitude": 51.96,
        "longitude": 1.35,
        "radius_km": 5,
    },
}

MAX_OBSERVATION_GAP_HOURS = 24
MAX_ROUTE_GAP_DAYS = 7


def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance between coordinates in kilometres."""

    earth_radius_km = 6371.0

    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        np.sin(dlat / 2) ** 2
        + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    )

    return 2 * earth_radius_km * np.arcsin(np.sqrt(a))


def load_all_vessel_positions():
    """Load the complete historical AIS observation dataset."""

    with sqlite3.connect(DATABASE_PATH) as connection:
        df = pd.read_sql_query(
            """
            SELECT
                event_id,
                mmsi,
                vessel_name,
                latitude,
                longitude,
                event_timestamp
            FROM processed_vessel_events
            WHERE event_timestamp IS NOT NULL
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
            """,
            connection,
        )

    if df.empty:
        return df

    df["event_timestamp"] = pd.to_datetime(
        df["event_timestamp"],
        utc=True,
        errors="coerce",
    )

    df = df.dropna(
        subset=["mmsi", "latitude", "longitude", "event_timestamp"]
    )

    df = df.drop_duplicates(subset=["event_id"])

    return df.sort_values(["mmsi", "event_timestamp"])


def assign_nearest_port(df):
    """Assign an observation to a port when it falls inside its geofence."""

    if df.empty:
        return df.assign(port=None)

    result = df.copy()

    nearest_ports = []
    nearest_distances = []

    for row in result.itertuples():
        best_port = None
        best_distance = float("inf")

        for port_name, port in PORTS.items():
            distance = haversine_km(
                row.latitude,
                row.longitude,
                port["latitude"],
                port["longitude"],
            )

            if (
                distance <= port["radius_km"]
                and distance < best_distance
            ):
                best_port = port_name
                best_distance = distance

        nearest_ports.append(best_port)
        nearest_distances.append(
            best_distance if best_port else None
        )

    result["port"] = nearest_ports
    result["port_distance_km"] = nearest_distances

    return result




def detect_port_arrivals(df):
    """
    Detect transitions from outside a port geofence into a named port.
    Consecutive observations inside the same port count only once.
    """

    columns = [
        "mmsi",
        "vessel_name",
        "port",
        "arrival_time",
    ]

    if df.empty:
        return pd.DataFrame(columns=columns)

    df = df.copy()
    df["event_timestamp"] = pd.to_datetime(
        df["event_timestamp"],
        utc=True,
        errors="coerce",
    )

    df = df.dropna(
        subset=["mmsi", "event_timestamp"]
    )

    df = df.sort_values(["mmsi", "event_timestamp"])

    # A port entry is an observation inside a named port
    # whose preceding observation was outside that port.
    df["previous_port"] = (
        df.groupby("mmsi")["port"].shift(1)
    )

    df["previous_time"] = (
        df.groupby("mmsi")["event_timestamp"].shift(1)
    )

    df["gap_hours"] = (
        df["event_timestamp"] - df["previous_time"]
    ).dt.total_seconds() / 3600

    arrivals = df[
        df["port"].notna()
        & (
            df["previous_port"].isna()
            | (df["port"] != df["previous_port"])
        )
        & (
            df["previous_time"].isna()
            | (df["gap_hours"] <= MAX_OBSERVATION_GAP_HOURS)
        )
    ].copy()

    arrivals = arrivals.rename(
        columns={
            "event_timestamp": "arrival_time",
        }
    )

    return arrivals[columns].reset_index(drop=True)


def derive_observed_routes(arrivals):
    """Derive routes from consecutive detected port calls."""

    columns = [
        "origin",
        "destination",
        "route",
        "vessel_count",
        "journey_count",
    ]

    if arrivals.empty:
        return pd.DataFrame(columns=columns)

    journeys = []

    for mmsi, vessel in arrivals.groupby("mmsi"):
        vessel = vessel.sort_values("arrival_time")

        calls = vessel.to_dict("records")

        for first, second in zip(calls, calls[1:]):
            gap_days = (
                second["arrival_time"] - first["arrival_time"]
            ).total_seconds() / 86400

            if (
                first["port"] != second["port"]
                and 0 < gap_days <= MAX_ROUTE_GAP_DAYS
            ):
                journeys.append({
                    "mmsi": mmsi,
                    "origin": first["port"],
                    "destination": second["port"],
                    "route": (
                        f'{first["port"]} → {second["port"]}'
                    ),
                })

    if not journeys:
        return pd.DataFrame(columns=columns)

    journey_df = pd.DataFrame(journeys)

    routes = (
        journey_df.groupby(
            ["origin", "destination", "route"],
        )
        .agg(
            vessel_count=("mmsi", "nunique"),
            journey_count=("mmsi", "size"),
        )
        .reset_index()
        .sort_values("journey_count", ascending=False)
    )

    return routes


def get_port_analytics():
    """Return detected arrivals and observed route statistics."""

    positions = load_all_vessel_positions()

    if positions.empty:
        empty = pd.DataFrame()
        return empty, empty

    positions = assign_nearest_port(positions)

    arrivals = detect_port_arrivals(positions)

    if not arrivals.empty:
        arrivals = arrivals.sort_values("arrival_time")

    routes = derive_observed_routes(arrivals)

    return arrivals, routes
