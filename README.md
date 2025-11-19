# Real‑Time IoT Sensor Streaming Pipeline

This project demonstrates a **real‑time data streaming system** built with:

- **Apache Kafka** — message broker for streaming events  
- **PostgreSQL** — persistent storage for event data  
- **Streamlit** — live dashboard visualization  
- **Python** — producer, consumer, and dashboard logic  

The example domain is **IoT sensor readings** (temperature / humidity),  
but you can adapt it easily for other streaming use cases such as ride‑sharing, stock trading, or log monitoring.

---

## Architecture

IoT Sensor Producer → Kafka → Consumer → PostgreSQL → Streamlit Dashboard

- **Producer (`sensor.py`)** — generates synthetic IoT sensor data every 0.5 – 2 s  
- **Kafka** — manages the event stream  
- **Consumer (`consumer.py`)** — reads events from Kafka, saves to the database  
- **PostgreSQL** — stores all sensor readings  
- **Dashboard (`dashboard.py`)** — displays live metrics and charts  

---

### 1. sensor.py - Sensor Data Producer
Purpose: Simulates IoT sensors generating temperature and humidity readings.

Data Schema:
- sensor_id: Sensor identifier (sensor_001 to sensor_005)
- temperature: Temperature in Celsius (15-35°C)
- humidity: Humidity percentage (30-90%)
- timestamp: ISO format timestamp

Kafka Topic: sensor-data
Behavior: Sends readings every 0.5-2 seconds to simulate real sensor activity.

### 2. geo.py - Sensor Location Producer
Purpose: Generates geographic location data for sensors (for potential future geospatial analysis).

Data Schema:
- sensor_id: Sensor identifier (matches sensor.py)
- latitude: Geographic latitude coordinate
- longitude: Geographic longitude coordinate
- elevation: Elevation in meters

Kafka Topic: sensor-locations

### 3. dashboard.py - Real-Time Streamlit Dashboard
Purpose: Provides live visualization of sensor data with auto-refresh.
Features:
- KPI Metrics: Average, min, max temperature and humidity
- Time-Series Charts: Temperature and humidity trends over time
- Bar Charts: Average metrics by sensor ID
- Data Table: Latest 10 sensor readings
- Filters: Filter by specific sensor or view all sensors
- Auto-Refresh: Configurable refresh interval (2-20 seconds)
