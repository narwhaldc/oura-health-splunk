# Oura Health for Splunk

An end-to-end pipeline and Splunk **Dashboard Studio** app for pulling
[Oura Ring](https://ouraring.com) data into Splunk and visualizing it —
sleep, heart rate, activity, wellness, and ring/battery status.

## Contents

| Path | What it is |
|------|------------|
| `oura_to_hec_with_phi.py` | Oura API → Splunk HEC ingest script (OAuth2 + PKCE, incremental checkpointed sync, client-side dedup, multi-target fan-out) |
| `app/` | Unpacked Splunk app source — 6 Dashboard Studio dashboards (Today, Sleep, Heart Health, Activity, Wellness, Ring), nav, saved searches |
| `oura_health-1_8_78.spl` | Packaged app, installable via Splunk Web (Apps → Install app from file) |
| `INSTALL.md` | Full setup guide (Oura API, HEC, cron, backfill, troubleshooting) |
| `manifest.txt` | Distribution manifest |

## Requirements

- Oura Ring Gen 3 or newer + a developer app at [developer.ouraring.com](https://developer.ouraring.com)
- Splunk Enterprise 10.x or Splunk Cloud, with HTTP Event Collector enabled and an `oura` index
- Python 3.9+ on the ingest host (`pip install requests`)

## Companion custom visualizations

The Sleep/Activity **hypnograms** and the Ring **charge-status gauge** render via two
separate Dashboard Studio custom-viz apps — install them alongside this app for those
panels to display (the rest of the dashboards use core Splunk visualizations):

- **[hypnogram_viz](https://github.com/narwhaldc/hypnogram_viz)** → `hypnogram_viz.hypnogram`
- **[charge_ring_viz](https://github.com/narwhaldc/charge_ring_viz)** → `charge_ring_viz.chargestatus`

## Install

See **[INSTALL.md](INSTALL.md)**.

## Scope & limitations

This release is **single-user by design.** It assumes one Oura account owner who is also
the only person with access to the Splunk `oura` index — and therefore the only one who
can see the **personal health information (PHI)** it holds (sleep, heart rate, HRV,
activity, and `personal_info` fields such as age). Access control today is effectively
"you own the index, you see the data."

Not yet included:

- **Multi-user access isolation** — no per-user separation of data within a shared index.
- **Role-based access control (RBAC)** — no roles/capabilities scoping who can read the PHI.

> Multiple *accounts* can already be **ingested** into the same index (see "Multi-user
> support" in [INSTALL.md](INSTALL.md) — separate OAuth apps/token files, joinable on
> `oura_user_id`), but that's ingest fan-in, **not** access control. Strict RBAC and true
> multi-user PHI segregation are planned future work.

## Notes

Personal project, provided as-is with no warranty. All configuration (credentials,
HEC tokens, hostnames) is supplied at runtime via env vars / a local targets file —
none is committed here.

## License

Apache License 2.0 — see [LICENSE](LICENSE). Copyright 2026 Tony Vincent.
