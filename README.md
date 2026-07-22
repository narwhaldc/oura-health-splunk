# Oura Health for Splunk

An end-to-end pipeline and Splunk **Dashboard Studio** app for pulling
[Oura Ring](https://ouraring.com) data into Splunk and visualizing it —
sleep, heart rate, activity, wellness, and ring/battery status.

> **Note (2026-07):** the Oura **ingest script** has moved to the **[TA-oura](https://github.com/narwhaldc/TA-oura)**
> add-on (`tools/oura_to_hec_with_phi.py`) — part of the multi-vendor **[wearables](https://github.com/narwhaldc/wearables)**
> platform (vendor-neutral dashboards + RBAC + Garmin via [TA-garmin](https://github.com/narwhaldc/TA-garmin)).
> This `oura_health` app remains the GA **single-vendor** dashboards; new work happens in `wearables`,
> which supersedes it. For ingest setup see **[TA-oura/INSTALL.md](https://github.com/narwhaldc/TA-oura/blob/main/INSTALL.md)**.

## GA release

Current general-availability set (all three pass Splunk Cloud AppInspect):

| App | Version | Repo |
|-----|---------|------|
| **oura_health** (this app) | 2.0.8 | [oura-health-splunk](https://github.com/narwhaldc/oura-health-splunk) |
| **hypnogram_viz** | 1.0.1 | [hypnogram_viz](https://github.com/narwhaldc/hypnogram_viz) |
| **charge_ring_viz** | 1.0.0 | [charge_ring_viz](https://github.com/narwhaldc/charge_ring_viz) |

Install the two viz add-ons before/alongside this app; a full Splunk restart makes the custom-viz JS render. Packaged `.spl` files are attached to each repo's GitHub Release.

## Contents

| Path | What it is |
|------|------------|
| _(ingest script)_ | **Moved to [TA-oura](https://github.com/narwhaldc/TA-oura)** → `tools/oura_to_hec_with_phi.py` (Oura API → HEC; OAuth2, checkpointed sync, dedup, multi-target fan-out) |
| `app/` | Unpacked Splunk app source — 6 Dashboard Studio dashboards (Today, Sleep, Heart Health, Activity, Wellness, Ring) plus an About/setup page, nav, saved searches |
| `oura_health-2_0_8.spl` | Packaged app, installable via Splunk Web (Apps → Install app from file) |
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
