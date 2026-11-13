
import streamlit as st
import pandas as pd
import plotly.express as px

from risk_engine import get_vessel_risk_data
from port_analytics import get_port_analytics


# ==========================================================
# PAGE CONFIGURATION
# ==========================================================

st.set_page_config(
    page_title="Maritime Route Risk Monitor",
    page_icon="🚢",
    layout="wide",
)

st.title("🚢 Maritime Route Risk Monitor")
st.caption(
    "Real-time AIS vessel monitoring and weather-exposure risk assessment"
)


# ==========================================================
# DATA LOADING AND CACHING
# ==========================================================

@st.cache_data(ttl=10)
def load_data():
    return get_vessel_risk_data(limit=5000)


@st.cache_data(ttl=60)
def load_port_data():
    return get_port_analytics()


# ==========================================================
# SESSION STATE INITIALIZATION
# ==========================================================

if "applied_risks" not in st.session_state:
    st.session_state.applied_risks = [
        "Low",
        "Moderate",
        "High",
        "Insufficient weather data",
    ]

if "applied_vessels" not in st.session_state:
    st.session_state.applied_vessels = []

if "map_data" not in st.session_state:
    st.session_state.map_data = load_data()


# ==========================================================
# SIDEBAR FILTERS - MANUAL APPLICATION
# ==========================================================

st.sidebar.header("Dashboard Filters")

risk_options = [
    "Low",
    "Moderate",
    "High",
    "Insufficient weather data",
]

map_snapshot = st.session_state.map_data

available_vessels = sorted(
    map_snapshot["vessel_name"].dropna().unique().tolist()
)

with st.sidebar.form("dashboard_filter_form"):

    selected_risks = st.multiselect(
        "Risk classification",
        risk_options,
        default=st.session_state.applied_risks,
    )

    selected_vessels = st.multiselect(
        "Vessels",
        available_vessels,
        default=[
            vessel
            for vessel in st.session_state.applied_vessels
            if vessel in available_vessels
        ],
    )

    apply_filters = st.form_submit_button("Apply Filters")

if apply_filters:
    st.session_state.applied_risks = selected_risks
    st.session_state.applied_vessels = selected_vessels
    st.rerun()


# ==========================================================
# MANUAL MAP REFRESH
# ==========================================================



# ==========================================================
# FILTER HELPER
# ==========================================================

def apply_dashboard_filters(data):

    result = data[
        data["risk_level"].isin(
            st.session_state.applied_risks
        )
    ].copy()

    if st.session_state.applied_vessels:
        result = result[
            result["vessel_name"].isin(
                st.session_state.applied_vessels
            )
        ]

    return result


# ==========================================================
# LIVE VESSEL METRICS - AUTO REFRESH EVERY 15 SECONDS
# ==========================================================

@st.fragment(run_every="15s")
def live_vessel_metrics():

    df = load_data()

    if df.empty:
        st.warning("No vessel observations available.")
        return

    filtered_df = apply_dashboard_filters(df)

    st.subheader("Live Vessel Monitoring")

    total_observations = len(filtered_df)
    unique_vessels = filtered_df["mmsi"].nunique()

    high_risk = (
        filtered_df["risk_level"] == "High"
    ).sum()

    moderate_risk = (
        filtered_df["risk_level"] == "Moderate"
    ).sum()

    low_risk = (
        filtered_df["risk_level"] == "Low"
    ).sum()

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Observations", f"{total_observations:,}")
    col2.metric("Unique Vessels", f"{unique_vessels:,}")
    col3.metric("High Risk", f"{high_risk:,}")
    col4.metric("Moderate Risk", f"{moderate_risk:,}")
    col5.metric("Low Risk", f"{low_risk:,}")

    st.caption(
        f"Last data refresh: {pd.Timestamp.now(tz='UTC').strftime('%H:%M:%S UTC')}"
    )

    st.divider()

    # Risk distribution and weather measurements

    left, right = st.columns(2)

    with left:

        st.subheader("Risk Classification Distribution")

        risk_counts = (
            filtered_df["risk_level"]
            .value_counts()
            .rename_axis("Risk Level")
            .reset_index(name="Observations")
        )

        if not risk_counts.empty:

            fig = px.pie(
                risk_counts,
                names="Risk Level",
                values="Observations",
                hole=0.45,
                color="Risk Level",
                color_discrete_map={
                    "Low": "green",
                    "Moderate": "orange",
                    "High": "red",
                    "Insufficient weather data": "gray",
                },
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
                key="live_risk_distribution",
            )
            
        else:
            st.info("No observations match the applied filters.")

    with right:

        st.subheader("Weather Exposure Measurements")

        weather_columns = [
            "wave_height_m",
            "wind_speed_kmh",
            "wind_gusts_kmh",
            "current_velocity_ms",
        ]

        weather_summary = (
            filtered_df[weather_columns]
            .mean()
            .reset_index()
        )

        weather_summary.columns = [
            "Weather Factor",
            "Average Value",
        ]

        fig = px.bar(
            weather_summary,
            x="Weather Factor",
            y="Average Value",
            title="Average Weather Measurements",
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            key="live_weather_chart",
        )

live_vessel_metrics()
st.divider()
if st.button("Refresh Map", key="refresh_map"):

                load_data.clear()
                st.session_state.map_data = load_data()

                st.success("Vessel map data refreshed.")



# ==========================================================
# VESSEL MAP - MANUAL REFRESH ONLY
# ==========================================================



st.subheader("Vessel Positions and Weather Risk")

st.caption(
    "Map data remains unchanged until Refresh Map is clicked."
)

map_filtered_df = apply_dashboard_filters(
    st.session_state.map_data
)

map_df = map_filtered_df.dropna(
    subset=["latitude", "longitude"]
).copy()

# North Sea display boundaries

map_df = map_df[
    map_df["latitude"].between(50, 62)
    & map_df["longitude"].between(-5, 10)
].copy()

if not map_df.empty:

    fig = px.scatter_map(
        map_df,
        center={"lat": 56.0, "lon": 3.0},
        lat="latitude",
        lon="longitude",
        color="risk_level",
        color_discrete_map={
            "Low": "green",
            "Moderate": "orange",
            "High": "red",
            "Insufficient weather data": "gray",
        },
        hover_name="vessel_name",
        hover_data={
            "mmsi": True,
            "risk_score": True,
            "wave_height_m": True,
            "wind_speed_kmh": True,
            "wind_gusts_kmh": True,
            "current_velocity_ms": True,
            "latitude": False,
            "longitude": False,
        },
        zoom=4,
        height=550,
    )

    fig.update_layout(
        map_style="open-street-map",
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
        key="manual_vessel_map",
    )

else:
    st.info("No vessel positions available for the selected filters.")


# ==========================================================
# VESSEL DETAILS - MANUAL SNAPSHOT
# ==========================================================

st.divider()

st.subheader("Vessel Risk Details")

display_columns = [
    "mmsi",
    "vessel_name",
    "latitude",
    "longitude",
    "speed_over_ground",
    "wave_height_m",
    "wind_speed_kmh",
    "wind_gusts_kmh",
    "current_velocity_ms",
    "risk_score",
    "risk_level",
    "processed_at",
]

st.dataframe(
    map_filtered_df[display_columns],
    use_container_width=True,
    hide_index=True,
)


# ==========================================================
# PORT TRAFFIC ANALYTICS - AUTO REFRESH EVERY 60 SECONDS
# ==========================================================

@st.fragment(run_every="60s")
def port_traffic_dashboard():

    st.divider()

    st.header("⚓ Port Traffic Analytics")

    st.caption(
        "Observed port entries and vessel movements derived "
        "from AIS positions using approximate port geofences."
    )

    arrivals, routes = load_port_data()

    if arrivals.empty:
        st.warning("No qualifying port entries detected.")
        return

    arrivals = arrivals.copy()

    arrivals["arrival_time"] = pd.to_datetime(
        arrivals["arrival_time"],
        utc=True,
    )

    # Port KPIs

    port1, port2, port3 = st.columns(3)

    port1.metric(
        "Total Observed Port Entries",
        f"{len(arrivals):,}",
    )

    port2.metric(
        "Unique Vessels",
        f"{arrivals['mmsi'].nunique():,}",
    )

    port3.metric(
        "Ports Monitored",
        f"{arrivals['port'].nunique():,}",
    )

    # Daily / Weekly / Monthly arrivals

    st.subheader("Port Arrivals Over Time")

    period = st.selectbox(
        "Arrival reporting period",
        ["Daily", "Weekly", "Monthly"],
        key="port_reporting_period",
    )

    time_data = arrivals.copy()

    if period == "Daily":

        time_data["period"] = (
            time_data["arrival_time"].dt.strftime("%Y-%m-%d")
        )

    elif period == "Weekly":

        time_data["period"] = (
            time_data["arrival_time"]
            .dt.to_period("W")
            .astype(str)
        )

    else:

        time_data["period"] = (
            time_data["arrival_time"].dt.strftime("%Y-%m")
        )

    trend = (
        time_data.groupby("period")
        .size()
        .reset_index(name="Arrivals")
    )

    fig = px.bar(
        trend,
        x="period",
        y="Arrivals",
        title=f"{period} Observed Port Entries",
        labels={
            "period": period,
            "Arrivals": "Vessel entries",
        },
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
        key="port_arrival_trend",
    )

    # Most visited ports and routes

    left, right = st.columns(2)

    with left:

        st.subheader("Most Visited Ports")

        port_counts = (
            arrivals.groupby("port")
            .agg(
                Arrivals=("mmsi", "size"),
                Unique_Vessels=("mmsi", "nunique"),
            )
            .reset_index()
            .sort_values("Arrivals", ascending=False)
        )

        fig = px.bar(
            port_counts,
            x="port",
            y="Arrivals",
            color="Arrivals",
            hover_data=["Unique_Vessels"],
            title="Observed Entries by Port",
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            key="most_visited_ports",
        )

    with right:

        st.subheader("Most Used Observed Routes")

        if not routes.empty:

            fig = px.bar(
                routes.head(10),
                x="route",
                y="journey_count",
                hover_data=["vessel_count"],
                title="Observed Vessel Journeys",
                labels={
                    "route": "Route",
                    "journey_count": "Journeys",
                },
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
                key="observed_routes",
            )

            st.dataframe(
                routes,
                use_container_width=True,
                hide_index=True,
            )

        else:

            st.info(
                "Insufficient consecutive port-call observations "
                "to identify routes."
            )

    # Detailed arrivals

    st.subheader("Detected Port Entry Records")

    st.dataframe(
        arrivals.sort_values(
            "arrival_time",
            ascending=False,
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "Port statistics represent AIS-derived observed entries, "
        "not official port-call statistics."
    )


port_traffic_dashboard()


# ==========================================================
# METHODOLOGY
# ==========================================================

st.divider()

st.caption(
    "Risk methodology: provisional threshold-based weather exposure score. "
    "Missing weather factors are excluded from the average. "
    "This is a screening indicator, not a certified navigational safety assessment."
)
