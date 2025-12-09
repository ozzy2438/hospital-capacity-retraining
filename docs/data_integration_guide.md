# Data Source Integration Guide

## 1. External/Public Health Data

### Disease Surveillance
**CDC FluView API**
- Access: https://gis.cdc.gov/grasp/fluview/fluportaldashboard.html
- Method: REST API (free, public)
- Integration: Weekly batch job in Airflow

**COVID/RSV Data**
- Source: HHS Protect / state health departments
- Access: Public APIs or data.gov
- Frequency: Daily updates

### Weather Data
**NOAA API**
- Access: https://www.ncdc.noaa.gov/cdo-web/webservices/v2
- Method: REST API (free with token)
- Features: Temperature, air quality, precipitation

**OpenWeather API**
- Access: https://openweathermap.org/api
- Cost: Free tier (1000 calls/day)

## 2. Healthcare System Data

### Ambulance Diversion
- Source: Regional EMS coordination centers
- Access: Direct partnership or data sharing agreement
- Format: Real-time feed or hourly batch

### Scheduled Procedures
- Source: Internal EHR (Epic, Cerner)
- Access: HL7 FHIR API or database views
- Integration: Daily extract from scheduling system

### Staff Schedules
- Source: HR/workforce management system
- Access: Database query or API
- Privacy: Aggregate only (no PII)

## 3. Socioeconomic Data

### Insurance Claims
- Source: Payer partnerships or clearinghouses
- Access: Data use agreement + secure file transfer
- Lag: 30-90 days typical

### Social Determinants
- Source: Census Bureau, County Health Rankings
- Access: Public APIs (census.gov/data/developers)
- Frequency: Annual updates

### Mobility Data
- Source: SafeGraph, Google Mobility Reports
- Access: Commercial license or public aggregates
- Cost: $$ for commercial, free for research

## Implementation Steps

1. **Prioritize** - Start with Tier 1 (weather, disease surveillance)
2. **API Keys** - Register for free public APIs
3. **Partnerships** - Reach out to regional health departments
4. **ETL Pipeline** - Add Airflow tasks for each source
5. **Feature Store** - Centralize features for model training
6. **Validation** - Check data quality and freshness

## Sample Airflow Task

```python
def fetch_weather_data(**context):
    import requests
    from airflow.models import Variable
    
    api_key = Variable.get("noaa_api_key")
    url = f"https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&locationid=ZIP:12345&startdate=2025-01-01&enddate=2025-01-31"
    headers = {"token": api_key}
    response = requests.get(url, headers=headers)
    data = response.json()
    # Store to database or feature store
    return data
```

## Data Joining Strategy

1. **Time alignment** - Aggregate to daily/weekly grain
2. **Geographic keys** - Use zip code, county FIPS, or hospital service area
3. **Feature engineering** - Create lags, rolling averages, trends
4. **Storage** - Use feature store (Feast, Tecton) or data warehouse
