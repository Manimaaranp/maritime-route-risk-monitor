
import json
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import urlopen


# Fixed North Sea reference location for the first version
LATITUDE = 56.0
LONGITUDE = 3.0

MARINE_API_URL = "https://marine-api.open-meteo.com/v1/marine"
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_json(url, params):
    """Request JSON data from an API."""
    request_url = f"{url}?{urlencode(params)}"

    with urlopen(request_url, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_utc(timestamp):
    """Convert an API timestamp into a timezone-aware UTC datetime."""
    parsed = datetime.fromisoformat(timestamp)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def fetch_weather():
    """Fetch marine and atmospheric forecasts for the North Sea reference point."""

    marine_params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": (
            "wave_height,"
            "wave_period,"
            "ocean_current_velocity,"
            "ocean_current_direction"
        ),
        "forecast_days": 2,
        "timezone": "GMT",
        "cell_selection": "sea",
        "wind_speed_unit": "ms",
    }

    weather_params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": "wind_speed_10m,wind_gusts_10m",
        "forecast_days": 2,
        "timezone": "GMT",
        "wind_speed_unit": "kmh",
    }

    marine_data = fetch_json(MARINE_API_URL, marine_params)
    weather_data = fetch_json(WEATHER_API_URL, weather_params)

    marine_hourly = marine_data["hourly"]
    weather_hourly = weather_data["hourly"]

    # Match both API responses using their forecast timestamps
    marine_times = marine_hourly["time"]
    weather_times = weather_hourly["time"]

    weather_index = {
        timestamp: index
        for index, timestamp in enumerate(weather_times)
    }

    now = datetime.now(timezone.utc)

    matching_times = [
        timestamp
        for timestamp in marine_times
        if timestamp in weather_index
        and parse_utc(timestamp) >= now.replace(
            minute=0, second=0, microsecond=0
        )
    ]

    if not matching_times:
        raise ValueError("No matching future forecast timestamp found.")

    forecast_timestamp = matching_times[0]
    marine_index = marine_times.index(forecast_timestamp)
    weather_index_value = weather_index[forecast_timestamp]

    def get_value(data, variable, index):
        values = data.get(variable)

        if values is None or index >= len(values):
            return None

        return values[index]

    result = {
        "reference_location": {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
        },
        "forecast_timestamp": parse_utc(
            forecast_timestamp
        ).isoformat(),
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "marine": {
            "wave_height_m": get_value(
                marine_hourly, "wave_height", marine_index
            ),
            "wave_period_s": get_value(
                marine_hourly, "wave_period", marine_index
            ),
            "current_velocity_ms": get_value(
                marine_hourly, "ocean_current_velocity", marine_index
            ),
            "current_direction_deg": get_value(
                marine_hourly, "ocean_current_direction", marine_index
            ),
        },
        "weather": {
            "wind_speed_kmh": get_value(
                weather_hourly, "wind_speed_10m", weather_index_value
            ),
            "wind_gusts_kmh": get_value(
                weather_hourly, "wind_gusts_10m", weather_index_value
            ),
        },
        "units": {
            "wave_height": marine_data.get(
                "hourly_units", {}
            ).get("wave_height"),
            "wave_period": marine_data.get(
                "hourly_units", {}
            ).get("wave_period"),
            "current_velocity": marine_data.get(
                "hourly_units", {}
            ).get("ocean_current_velocity"),
            "wind_speed": weather_data.get(
                "hourly_units", {}
            ).get("wind_speed_10m"),
            "wind_gusts": weather_data.get(
                "hourly_units", {}
            ).get("wind_gusts_10m"),
        },
    }

    return result


if __name__ == "__main__":
    try:
        forecast = fetch_weather()

        print("\nOpen-Meteo forecast retrieved successfully\n")
        print(json.dumps(forecast, indent=4))

    except Exception as error:
        print(
            f"Weather retrieval failed: "
            f"{type(error).__name__}: {error}"
        )
