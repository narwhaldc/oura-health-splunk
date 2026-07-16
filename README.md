# Oura Health for Splunk

An end-to-end pipeline and Splunk **Dashboard Studio** app for pulling
[Oura Ring](https://ouraring.com) data into Splunk and visualizing it —
sleep, heart rate, activity, wellness, and ring/battery status.

## Contents

| Path | What it is |
|------|------------|
| `oura_to_hec_with_pii.py` | Oura API → Splunk HEC ingest script (OAuth2 + PKCE, incremental checkpointed sync, client-side dedup, multi-target fan-out) |
| `app/` | Unpacked Splunk app source — 6 Dashboard Studio dashboards (Today, Sleep, Heart Rate, Activity, Wellness, Ring), nav, saved searches |
| `oura_health-1_8_28.spl` | Packaged app, installable via Splunk Web (Apps → Install app from file) |
| `INSTALL.md` | Full setup guide (Oura API, HEC, cron, backfill, troubleshooting) |
| `manifest.txt` | Distribution manifest |

## Requirements

- Oura Ring Gen 3 or Gen 4 + a developer app at [developer.ouraring.com](https://developer.ouraring.com)
- Splunk Enterprise 10.x or Splunk Cloud, with HTTP Event Collector enabled and an `oura` index
- Python 3.9+ on the ingest host (`pip install requests`)

## Companion custom visualizations

The Sleep/Activity **hypnograms** and the Ring **charge-status gauge** render via two
separate Dashboard Studio custom-viz apps — `hypnogram_viz` and `charge_ring_viz`.
Install those alongside this app for those panels to display; the rest of the
dashboards work with core Splunk visualizations.

## Install

See **[INSTALL.md](INSTALL.md)**.

## Notes

Personal project, provided as-is with no warranty. All configuration (credentials,
HEC tokens, hostnames) is supplied at runtime via env vars / a local targets file —
none is committed here.
