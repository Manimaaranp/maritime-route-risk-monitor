
import sqlite3
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_PATH = PROJECT_ROOT / "data" / "maritime_analytics.db"


# Screening thresholds for weather exposure.
# These are provisional project thresholds, not official vessel safety limits.
THRESHOLDS = {
    "wave_height_m": {
        "moderate": 2.0,
        "high": 3.0,
    },
    "wind_speed_kmh": {
        "moderate": 30.0,
        "high": 45.0,
    },
    "wind_gusts_kmh": {
        "moderate": 40.0,
        "high": 60.0,
    },
    "current_velocity_ms": {
        "moderate": 0.75,
        "high": 1.25,
    },
}


def calculate_factor_score(value, thresholds):
    """Return a factor score from 0 to 100."""

    if value is None or pd.isna(value):
        return None

    if value >= thresholds["high"]:
        return 100

    if value >= thresholds["moderate"]:
        return 60

    return 20


def calculate_weather_risk(row):
    """
    Calculate an explainable weather-exposure score.

    Available weather factors are averaged.
    Missing weather factors are excluded rather than treated as zero.
    """

    factors = {
        "wave_height_m": row.get("wave_height_m"),
        "wind_speed_kmh": row.get("wind_speed_kmh"),
        "wind_gusts_kmh": row.get("wind_gusts_kmh"),
        "current_velocity_ms": row.get("current_velocity_ms"),
    }

    scores = {}
    for factor, value in factors.items():
        scores[factor] = calculate_factor_score(
            value,
            THRESHOLDS[factor],
        )

    available_scores = [
        score for score in scores.values()
        if score is not None
    ]

    if not available_scores:
        return {
            "risk_score": None,
            "risk_level": "Insufficient weather data",
            "factor_scores": scores,
        }

    risk_score = round(sum(available_scores) / len(available_scores))

    if risk_score >= 80:
        risk_level = "High"
    elif risk_score >= 50:
        risk_level = "Moderate"
    else:
        risk_level = "Low"

    return {
        "risk_score": risk_score,
        "risk_level": risk_level,
        "factor_scores": scores,
    }


def get_latest_vessel_events(limit=5000):
    """Retrieve the latest vessel observations from SQLite."""

    with sqlite3.connect(DATABASE_PATH) as connection:
        query = """
            SELECT
                event_id,
                mmsi,
                vessel_name,
                latitude,
                longitude,
                speed_over_ground,
                course_over_ground,
                event_timestamp,
                wave_height_m,
                wave_period_s,
                wind_speed_kmh,
                wind_gusts_kmh,
                current_velocity_ms,
                weather_cell_id,
                forecast_timestamp,
                weather_retrieved_at,
                processed_at
            FROM processed_vessel_events
            ORDER BY processed_at DESC
            LIMIT ?
        """

        return pd.read_sql_query(
            query,
            connection,
            params=(limit,),
        )


def get_vessel_risk_data(limit=5000):
    """Return vessel observations with calculated risk scores."""

    df = get_latest_vessel_events(limit)

    if df.empty:
        return df

    risk_results = df.apply(
        calculate_weather_risk,
        axis=1,
    )

    df["risk_score"] = risk_results.apply(
        lambda result: result["risk_score"]
    )

    df["risk_level"] = risk_results.apply(
        lambda result: result["risk_level"]
    )

    return df
