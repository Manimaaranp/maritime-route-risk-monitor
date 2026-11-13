# Maritime Route Risk Monitor

A real-time maritime monitoring and weather-risk screening prototype for
vessels operating in the North Sea.

## Project idea

The Maritime Route Risk Monitor collects live Automatic Identification
System (AIS) vessel-position messages, enriches observations with
marine-weather information, processes events through a streaming
pipeline, and presents the results in an interactive dashboard.


This is an academic analytics prototype, not a navigation,
collision-avoidance, or certified vessel-safety system.

## Objectives

-   Ingest live AIS messages from AISStream.
-   Validate and normalize vessel events.
-   Persist received events locally before publishing them.
-   Stream events through Apache Kafka.
-   Process the stream using Spark Structured Streaming.
-   Store processed observations in SQLite.
-   Enrich AIS observations using Open-Meteo marine and forecast data.
-   Visualize vessel positions, weather exposure, risk classifications,
    and AIS-derived port analytics.
-   Demonstrate recovery behaviour after Kafka or Spark interruptions.

##  Architecture

``` text
AISStream WebSocket
        |
        v
AIS ingestion service <----- Open-Meteo Marine / Forecast APIs
        |
        v
SQLite durable event spool
        |
        v
Kafka publisher
        |
        v
Apache Kafka: vessel-events
        |
        v
Spark Structured Streaming
        |
        v
SQLite analytics database
        |
        v
Streamlit dashboard
   |          |           |
Vessel map  Risk view  Port analytics
```

### Event flow

1.  The ingestion service receives live AIS messages over a WebSocket.
2.  Events are normalized and assigned a deterministic event ID and
    schema version.
3.  Each received event is persisted to the SQLite spool.
4.  The publisher sends pending events to the Kafka `vessel-events`
    topic and marks them published after Kafka acknowledgement.
5.  Spark Structured Streaming consumes Kafka events and writes them to
    the analytics database using event-ID-based idempotent inserts.
6.  The Streamlit dashboard reads the analytics database and presents
    the results.


## Technologies


  Python 3.12                         Application implementation

  AISStream WebSocket API             Live AIS message source

  Open-Meteo Marine and Forecast APIs Weather enrichment

  SQLite                              Durable spool and analytics storage

  Apache Kafka                        Event streaming and buffering

  Docker / Docker Compose             Local Kafka deployment

  Apache Spark Structured Streaming   Stream consumption and processing

  PySpark                             Python interface to Spark

  Streamlit                           Interactive dashboard

  Pandas                              Data processing and port analytics

  Plotly                              Interactive maps and charts

  kafka-python                        Kafka producer connectivity




##  Project structure

``` text
maritime-route-risk-monitor/
├── app/
│   ├── dashboard.py          # Streamlit dashboard
│   ├── ingestion.py          # AISStream connection and ingestion
│   ├── kafka_client.py       # Kafka client configuration
│   ├── publisher.py          # Publishes pending spool events
│   ├── spool.py              # Durable SQLite event spool
│   ├── spark_processor.py    # Spark Structured Streaming processor
│   ├── storage.py             # Analytics database operations
│   ├── weather.py             # Weather retrieval and enrichment
│   ├── risk_engine.py         # Weather-exposure scoring
│   └── port_analytics.py      # AIS-derived port analytics
├── data/                      # Local runtime databases
├── .env.example               # Example environment configuration
├── .gitignore                 # Excludes secrets and runtime files
├── docker-compose.yml         # Kafka service configuration
├── requirements.txt           # Python dependencies
└── README.md
```

Runtime databases, Kafka storage, and Spark checkpoints are generated
during execution and are not required to be present in a fresh clone.



## Prerequisites

Install the following:

  Requirement                                   
  Python 3.12                                   
  Java 17                                       
  Docker Desktop / Docker Engine with Compose   
  AISStream API key                            
  https://aisstream.io/



Use Python 3.12 and Java 17 for the tested environment.

##  Python requirements

The direct dependencies in `requirements.txt` are:

``` text
kafka-python==2.2.15
python-dotenv==1.2.4
websockets==17.4
pyspark==4.3.0
streamlit==1.67.0
pandas==3.1.1
plotly==7.1.0
requests==2.34.2
```

SQLite, JSON, datetime, and other standard-library modules do not need
separate installation.

## Configuration and installation

###  Clone the repository

``` bash
git clone https://github.com/Manimaaranp/maritime-route-risk-monitor.git
cd maritime-route-risk-monitor
```

### Create a virtual environment

macOS / Linux:

``` bash
python3.12 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

``` powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
```

###  Install Python dependencies

``` bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Configure AISStream

Create a local `.env` file from the example.

macOS / Linux:

``` bash
cp .env.example .env
```

Windows PowerShell:

``` powershell
Copy-Item .env.example .env
```

Edit `.env` and insert your own API key:

``` env
AISSTREAM_API_KEY=your_actual_aisstream_api_key
```

 The `.env.example` file must contain
placeholders only.

## Step-by-step: run the complete project

Run each long-running component in a **separate terminal**. In every
terminal, change to the repository root and activate `.venv`. Start
Docker Desktop first.

### Step 1 --- Start Kafka

From the project root:

``` bash
docker compose up -d
docker compose ps
```

The application is configured for local Kafka access, normally through
`localhost:9092`. Confirm the broker is running before starting the
Python services.

### Step 2 --- Start AIS ingestion

In Terminal 1:

``` bash
python app/ingestion.py
```

This connects to AISStream, receives live messages, performs event
preparation/weather enrichment, and writes received events to the local
spool.

### Step 3 --- Start the publisher

In Terminal 2:

``` bash
python app/publisher.py
```

This publishes pending spool events to the Kafka `vessel-events` topic.
Keep the process running.

### Step 4 --- Start Spark Structured Streaming

In Terminal 3:

``` bash
python app/spark_processor.py
```

Spark consumes Kafka events and writes processed observations to the
SQLite analytics database. Keep the process running. The configured
checkpoint allows the stream to resume from recorded progress after
restart.

### Step 5 --- Start the dashboard

In Terminal 4:

``` bash
streamlit run app/dashboard.py
```

Open the local URL printed by Streamlit, normally:

``` text
http://localhost:8501
```

### Step 6 --- Verify end-to-end operation

Check that:

1.  Ingestion connects to AISStream and receives messages.
2.  Events are written to the spool.
3.  The publisher sends pending events to Kafka.
4.  Spark consumes batches and writes analytics records.
5.  The dashboard displays available vessel, weather, and port
    analytics.

The dashboard may initially be empty or have limited weather/port
information while data is being collected and processed. Weather
coverage and port-transition results depend on the observations
available.

## Features

### Live vessel monitoring

-   Receives live AIS messages through AISStream.
-   Processes available vessel attributes such as MMSI, position, speed
    over ground, course over ground, heading, and timestamps.
-   Displays vessel positions on an interactive North Sea map.
-   Supports vessel and risk-category filters.

### Weather enrichment

-   Retrieves marine and forecast information from Open-Meteo.
-   Uses four approximate North Sea reference cells:
    -   Southern: 52.5° N, 3.5° E
    -   Central: 56.0° N, 3.0° E
    -   Northern: 59.0° N, 2.0° E
    -   German Bight: 54.0° N, 7.0° E
-   Associates observations with the nearest configured reference cell.

### Weather-exposure risk screening

-   Evaluates available wave height, wind speed, wind gust, and
    current-velocity measurements.
    
-   Applies provisional factor thresholds and combines available factor
    scores.
-   Classifies records as Low, Moderate, or High when usable weather
    data exists.
-   Reports insufficient weather data when no usable weather factors are
    available.

The score is a rule-based screening indicator. Its thresholds are not
certified maritime limits and have not been validated as an
accident-prediction model.

### Port traffic analytics

-   Uses approximate circular geofences around configured port reference
    coordinates.
-   Infers AIS-observed port entries and transitions.
-   Summarizes observed arrivals, vessel counts, activity trends, and
    port-to-port transitions.

These are AIS-derived observations, not official port-call records or
authoritative port statistics.

### Pipeline resilience

-   Persists received events before Kafka publishing.
-   Retries pending spool events when Kafka becomes available.
-   Uses deterministic event IDs and idempotent database writes to limit
    duplicate analytics rows.
-   Uses Spark checkpoints and a query restart loop to support recovery.

##  Dashboard usage

-   **Live vessel metrics:** Review current vessel counts and risk
    distribution.
-   **Risk classification:** Inspect Low, Moderate, and High screening
    categories.
-   **Weather measurements:** Review the weather variables used by the
    screening model.
-   **Vessel map:** Explore vessel positions in the configured North Sea
    area.

-   **Refresh Map:** Load a newer map snapshot using the map refresh
    control.
-   **Port traffic analytics:** Review inferred port entries, activity
    trends, frequently observed ports, and port-to-port transitions.

The live metrics, port analytics, and map have separate refresh
behaviour. Dashboard values depend on successful ingestion, publishing,
processing, and database writes.

##  Limitations and assumptions


### Weather spatial resolution

Weather enrichment uses four approximate North Sea reference cells and
nearest-cell assignment. It does not interpolate weather at the vessel's
exact coordinates. Conditions at a reference point may differ from local
vessel conditions.


### Risk methodology

The risk engine uses provisional rule-based thresholds for wave height,
wind speed, gusts, and current velocity. The thresholds and scoring
method are not a certified maritime standard and are not validated
against accident outcomes. The score is an analytical screening
indicator only.

### Port geofences and port calls

Port locations are represented by approximate reference coordinates and
radii. Entries and transitions are inferred from AIS observations and
observation gaps. They may not match official port boundaries, berth
movements, or port-authority call records. Zero detected entries in this
dataset does not mean that a port has no actual traffic.



### Safety scope

The dashboard is not intended for navigation, route authorization,
emergency response, regulatory compliance, or safety-critical decisions.
Operational decisions must use authoritative maritime, meteorological,
and navigational sources.

##  Stopping the system

Stop each Python process with `Ctrl + C` in its terminal.

Stop Docker Compose services from the project root:

``` bash
docker compose down
```

This stops containers but normally preserves named volumes. Do not use
`docker compose down -v` unless you intentionally want to delete the
volumes and stored Kafka data.

## Troubleshooting

  -----------------------------------------------------------------------
  Problem                             Checks
  ----------------------------------- -----------------------------------
  AISStream connection fails          Check `.env`, API key validity,
                                      internet connection, and ingestion
                                      logs

  Kafka connection refused            Confirm Docker is running and
                                      inspect `docker compose ps`

  Publisher has pending events        Confirm Kafka is available and
                                      publisher is running

  Spark cannot connect to Kafka       Check broker availability,
                                      bootstrap address, Java 17, and
                                      Spark logs

  Dashboard is empty                  Confirm ingestion, publisher, and
                                      Spark are running and analytics
                                      records are being written

  Weather values are missing          Check API connectivity, timestamp
                                      matching, and weather-cell
                                      assignment

  Dashboard fails to start            Activate `.venv` and reinstall
                                      `requirements.txt` if necessary
  -----------------------------------------------------------------------

Inspect the failing component's terminal output before restarting or
deleting runtime data. Do not delete databases, Kafka volumes, or Spark
checkpoints without understanding the effect on retained data and
recovery.


------------------------------------------------------------------------

**Project:** Maritime Route Risk Monitor\
**Purpose:** Academic prototype for real-time AIS streaming,
weather-exposure screening, and AIS-derived port analytics.
