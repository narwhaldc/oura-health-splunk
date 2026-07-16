# Oura Health for Splunk

Visualize your Oura Ring health data in Splunk Dashboard Studio.

## Dashboards

- **Today** — Daily readiness, sleep, and activity snapshot
- **Sleep** — Sleep stages, hypnogram, efficiency, heart rate, and HRV
- **Heart Rate** — Intraday HR, resting HR trend, BPM distribution
- **Activity** — Steps, calories, activity breakdown and trends
- **Wellness** — SpO2, body temperature, HRV, and sleep timing

## Requirements

- Splunk Enterprise 10.x or Splunk Cloud Platform
- `oura` index with data ingested via the companion `oura_to_hec.py` script
- Sourcetype: `oura:ring`

## Setup

See `INSTALL.md` for full installation instructions including:
- Oura OAuth2 app registration
- Splunk HEC configuration
- Ingest script deployment and cron setup
- Log rotation

## Version

1.3.1 — July 2026
