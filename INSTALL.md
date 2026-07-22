# Oura Health → Splunk — Installation Guide

Complete setup guide for the Oura Ring data pipeline and Splunk Dashboard Studio app.  
**Last updated:** July 2026 | **App version:** 2.0.4 | **Script:** `oura_to_hec_with_phi.py`

> ⚠ **The Oura fetcher has moved to the [TA-oura](https://github.com/narwhaldc/TA-oura) add-on**
> (`tools/oura_to_hec_with_phi.py`). For ingest setup, use **[TA-oura/INSTALL.md](https://github.com/narwhaldc/TA-oura/blob/main/INSTALL.md)**.
> The fetcher sections below are retained for the legacy single-vendor setup (the script name is
> unchanged; only its repo home moved). New work lives in the **wearables** platform.

> **Index (recommended: `wearables`).** This app's dashboards read their index through the **`widx`
> macro** (Settings → Advanced Search → Search macros → `widx`), defined as
> `(index=oura OR index=wearables)` — it bridges the legacy `oura` index (history) and the platform
> `wearables` index (new data). To use a different index, edit that **one macro line** (e.g.
> `(index=oura OR index=<yourindex>)`); it must match the ingest target index (see
> [TA-oura/INSTALL.md](https://github.com/narwhaldc/TA-oura/blob/main/INSTALL.md)) and the wearables
> app's own `widx`. The dedup-maintenance saved searches use the literal `index=oura` — update those
> only if you rename the legacy index.

**GA release set:** oura_health **2.0.4** · hypnogram_viz **1.0.1** · charge_ring_viz **1.0.0** (all AppInspect Cloud-clean; install the two viz add-ons before/alongside this app — a full Splunk restart makes their custom-viz JS render).

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Oura API Setup](#oura-api-setup)
4. [Splunk Setup](#splunk-setup)
5. [Script Installation](#script-installation)
6. [OAuth2 Authorization](#oauth2-authorization)
7. [Environment Configuration](#environment-configuration)
8. [Target Configuration](#target-configuration)
9. [First Run and Backfill](#first-run-and-backfill)
10. [Splunk App Installation](#splunk-app-installation)
11. [Cron Automation](#cron-automation)
12. [Log Rotation](#log-rotation)
13. [Data Reference](#data-reference)
14. [Dedup Store](#dedup-store)
15. [Scheduled Dedup Maintenance](#scheduled-dedup-maintenance)
16. [Checkpoint Management](#checkpoint-management)
17. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
Oura Ring → Oura Cloud API (v2)
                ↓
        oura_to_hec_with_phi.py     (runs every 10 min via cron)
        OAuth2 + PKCE auth
        Per-type field filtering
        Client-side dedup store      (prevents duplicate HEC sends)
        HEC pre-flight validation    (verifies token per target)
                ↓
        Multi-target fan-out         (one fetch, N targets)
                ↓
        ┌─────────────────┐   ┌─────────────────┐
        │ Splunk personal │   │ Splunk demo      │
        │ index=oura      │   │ index=oura       │
        └─────────────────┘   └─────────────────┘
                ↓                       ↓
        oura_health Splunk app  (6 Dashboard Studio dashboards)
        Today / Sleep / Heart Health / Activity / Wellness / Ring
```

The script runs on a Linux host on your local network. It does **not** require Splunk to be internet-facing — it pushes data to Splunk's HEC port directly. The Oura API is the only external dependency. A single cron entry, a single Oura API fetch, and a single token file serve all targets.

### Multi-user support

Every event is tagged with `oura_user_id` (resolved from `/v2/usercollection/personal_info` on each run). This is the stable Oura **account** ID — it survives ring replacement and warranty swaps. To ingest data from a second Oura account, register a separate OAuth2 developer app, create a second `.env` file with separate `OURA_TOKEN_FILE`, `OURA_CHECKPOINT_FILE`, and `OURA_DEDUP_FILE` paths, and run a second cron entry. All data lands in the same `index=oura` and is joinable on `oura_user_id`.

---

## Prerequisites

### On the Linux host running the script

- Python 3.9 or later
- `pip install requests`
- Network access to `api.ouraring.com` (outbound HTTPS)
- Network access to your Splunk HEC port(s) (default 8088)

### Splunk

- Splunk Enterprise 10.x or Splunk Cloud (tested on Enterprise 10.4.0)
- HTTP Event Collector enabled
- An index named `oura` (or update the index in target config)
- Admin access to install the app
- `can_delete` capability (required only for scheduled dedup maintenance)

### Oura

- Oura Ring Gen 3 or newer
- Account at [cloud.ouraring.com](https://cloud.ouraring.com)
- Developer app registered at [developer.ouraring.com](https://developer.ouraring.com)

---

## Oura API Setup

### Register an OAuth2 application

1. Go to [developer.ouraring.com](https://developer.ouraring.com) and sign in
2. Click **Create New Application**
3. Set the redirect URI to: `http://localhost:8182/callback`
4. Note your **Client ID** and **Client Secret** — you will need these for the `.env` file
5. Under **Scopes**, enable: `personal`, `daily`, `heartrate`, `workout`, `tag`, `session`, `spo2`, `ring_configuration`, plus **Heart Health** (Cardiovascular Age) and **Stress** (Resilience).

> The OAuth2 authorization flow runs locally and opens a browser on port 8182. This only needs to happen once — tokens are persisted to `oura_tokens.json` and refreshed automatically on every subsequent run.
>
> **Adding scopes to an existing app:** you do **not** need a new Client ID/Secret. Enable the new scope boxes, click **Save Changes**, then re-run `--auth` (see [OAuth2 Authorization](#oauth2-authorization)) to mint a token that carries them — a token minted before a scope was added returns `401` for that scope's endpoints until re-authorized. The requested scope strings live in `OAUTH_SCOPES` in the script (override at runtime with `OURA_OAUTH_SCOPES` if Oura's exact identifiers differ from the defaults).

---

## Splunk Setup

### Enable HTTP Event Collector

1. In Splunk Web, go to **Settings → Data Inputs → HTTP Event Collector**
2. Click **Global Settings** — ensure HEC is enabled and note the port (default 8088)
3. Click **New Token**
   - Name: `oura-ring`
   - Source type: `oura:ring`
   - Index: `oura` (create the index first if it does not exist)
4. Copy the token value — you will need it for target configuration

Repeat for each Splunk instance you want to send data to (e.g. personal + demo).

### Create the index

```
Settings → Indexes → New Index
  Index name: oura
  Index type: Events
  Max size:   500 MB (a year of Oura data is well under 100 MB)
```

### Note the HEC URL(s)

A target's `hec_url` has the form `<scheme>://<host>:<port>/services/collector/event`:

```
# HTTP  — HEC "Enable SSL" is OFF
http://your-splunk-host:8088/services/collector/event

# HTTPS — HEC "Enable SSL" is ON
https://your-splunk-host:8088/services/collector/event
```

**A HEC endpoint is HTTP _or_ HTTPS — not both.** SSL is a single global HEC setting
(Settings → Data Inputs → HTTP Event Collector → Global Settings → **Enable SSL**), so the
scheme in your `hec_url` must match it: SSL on → `https://`, SSL off → `http://`. Using the
wrong scheme fails the pre-flight token check. With `https` and a self-signed cert, set
`verify_ssl: false` in the target config.

**Splunk Cloud:** the HEC host is normally **different from your search-head URL**. Splunk
Cloud receives HEC on a dedicated inputs host over port 443 (for example, on an AWS-hosted Cloud stack this would be):

```
https://http-inputs-<your-stack>.splunkcloud.com:443/services/collector/event
```

Use that inputs hostname (not `<your-stack>.splunkcloud.com`). Full details and examples for different Cloud hosting providers in Splunk's
[Set up and use HTTP Event Collector in Splunk Web](https://help.splunk.com/en/splunk-enterprise/get-started/get-data-in/10.4/get-data-with-http-event-collector/set-up-and-use-http-event-collector-in-splunk-web).

---

## Script Installation

```bash
# Create working directory
mkdir -p /opt/oura-splunk
cd /opt/oura-splunk

# Copy the script
cp oura_to_hec_with_phi.py /opt/oura-splunk/

# Install Python dependency
pip3 install requests --user
# or system-wide:
# sudo pip3 install requests

# Verify Python version
python3 --version   # must be 3.9+
```

---

## Environment Configuration

Create a `.env` file in the working directory. This file is sourced by cron before running the script. Oura credentials and file paths go here — Splunk HEC settings go in the targets config file (see next section).

```bash
cat > /opt/oura-splunk/.env << 'EOF'
# Oura OAuth2 credentials (from developer.ouraring.com)
export OURA_CLIENT_ID=your_client_id_here
export OURA_CLIENT_SECRET=your_client_secret_here

# File paths (relative to working directory or absolute)
export OURA_TOKEN_FILE=/opt/oura-splunk/oura_tokens.json
export OURA_CHECKPOINT_FILE=/opt/oura-splunk/oura_checkpoint.json
export OURA_DEDUP_FILE=/opt/oura-splunk/oura_dedup_store.json
export OURA_TARGETS_FILE=/opt/oura-splunk/oura_targets.json

# Lock file prevents concurrent runs (cron + manual overlap)
export OURA_LOCK_FILE=/opt/oura-splunk/oura_sync.lock

# How many days to re-fetch before the checkpoint on each run.
# Oura publishes daily summaries hours after midnight — 2 days ensures
# records not available on the previous run are caught on the next.
export OURA_CHECKPOINT_OVERLAP_DAYS=2

# Days to look back on the very first run (no checkpoint exists)
export OURA_LOOKBACK_DAYS=30
EOF

chmod 600 /opt/oura-splunk/.env
```

> **Security:** The `.env` file contains credentials. `chmod 600` restricts read access to your user only.

---

## Target Configuration

Create an `oura_targets.json` file to define one or more Splunk HEC targets. The script fetches data from Oura once and fans it out to all targets.

```bash
cat > /opt/oura-splunk/oura_targets.json << 'EOF'
{
  "targets": {
    "personal": {
      "hec_url":    "http://your-splunk-host:8088/services/collector/event",
      "hec_token":  "your-personal-hec-token",
      "index":      "oura",
      "sourcetype": "oura:ring",
      "verify_ssl": false
    },
    "demo": {
      "hec_url":    "https://your-demo-splunk-host:8088/services/collector/event",
      "hec_token":  "your-demo-hec-token",
      "index":      "oura",
      "sourcetype": "oura:ring",
      "verify_ssl": true
    }
  }
}
EOF

chmod 600 /opt/oura-splunk/oura_targets.json
```

### Target fields

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| `hec_url` | Yes | — | Full URL including port and path |
| `hec_token` | Yes | — | HEC token for this instance |
| `index` | No | `oura` | Splunk index name |
| `sourcetype` | No | `oura:ring` | Sourcetype for all events |
| `verify_ssl` | No | `true` | Ignored for `http://` URLs (no TLS to verify) |

### Single-target fallback

If no `oura_targets.json` exists, the script falls back to env vars (`SPLUNK_HEC_URL`, `SPLUNK_HEC_TOKEN`, `SPLUNK_INDEX`, `SPLUNK_SOURCETYPE`, `SPLUNK_VERIFY_SSL`) for backwards compatibility.

### Pre-flight validation

On every run (including `--dry-run`), the script sends a GET request to each target's HEC endpoint to validate connectivity and token authenticity before fetching any Oura data. Failed targets are skipped for that run with a clear error log. If all targets fail, the script aborts.

---

## OAuth2 Authorization

This step runs **once** on a machine with a browser. If the Linux host has no browser, run it on your Mac/laptop first, then copy `oura_tokens.json` to the server.

### On the host with a browser

```bash
cd /opt/oura-splunk
source .env
python3 oura_to_hec_with_phi.py --auth
```

A browser window opens to Oura's authorization page. Log in and approve access. The script captures the callback on `localhost:8182`, exchanges the code for tokens, and saves them to `oura_tokens.json`.

### If running on a headless server

```bash
# On your Mac/laptop — run auth there first
source .env
python3 oura_to_hec_with_phi.py --auth
# Tokens saved to ./oura_tokens.json

# Copy tokens to the server
scp oura_tokens.json youruser@your-splunk-host:/opt/oura-splunk/
```

Tokens refresh automatically on every script run. You should not need to re-authorize unless the refresh token expires (Oura refresh tokens have a long TTL — typically 30 days of non-use). Note: Oura refresh tokens are single-use. The multi-target design ensures only one cron entry touches the token file, eliminating the race condition that existed when two separate scripts shared a token.

---

## First Run and Backfill

### Test with dry-run first

```bash
cd /opt/oura-splunk
source .env
python3 oura_to_hec_with_phi.py --dry-run
```

This validates all HEC targets (connectivity + token), prints HEC payloads to stdout without sending anything, and verifies field filtering. Check that per-target counts are symmetric.

### Initial data ingest

```bash
python3 oura_to_hec_with_phi.py
```

On first run with no checkpoint file, the script fetches `OURA_LOOKBACK_DAYS` (default 30) days of history for all data types. Expect ~1,500 heart rate records plus ~30 records per daily type. The dedup store is populated automatically during this run.

### Historical backfill

To pull data further back than 30 days:

```bash
python3 oura_to_hec_with_phi.py --backfill 2026-01-01
```

This ignores the checkpoint and fetches everything from that date forward. Combine with `--reset-dedup` if you want to force re-sending records that may already be in the dedup store:

```bash
python3 oura_to_hec_with_phi.py --backfill 2026-01-01 --reset-dedup
```

To backfill a single target (e.g. after a demo wipe):

```bash
python3 oura_to_hec_with_phi.py --backfill 2026-01-01 --reset-dedup --target demo
```

### Verify data landed in Splunk

```spl
index=oura | timechart span=1d count by oura_data_type
```

You should see one row per data type per day with count = 1 for daily types and ~288 for heart_rate.

---

## Splunk App Installation

### Install the app

```bash
# Stop Splunk
$SPLUNK_HOME/bin/splunk stop

# Remove any previous version
rm -rf $SPLUNK_HOME/etc/apps/oura_health

# Also remove any user-local overrides (important — these take precedence)
rm -rf $SPLUNK_HOME/etc/users/youruser/oura_health

# Extract the app
cd $SPLUNK_HOME/etc/apps
tar -xzf /path/to/oura_health-2_0_4.spl

# Start Splunk
$SPLUNK_HOME/bin/splunk start
```

### Access the dashboards

Navigate to: `http://your-splunk-host:8000/en-US/app/oura_health/oura_today`

Or use the app nav: **Today / Sleep / Heart Health / Activity / Wellness / Ring**

### Companion custom visualizations (required for the hypnogram & charge panels)

Two panels render through separate Dashboard Studio **custom-visualization apps**. Install
them alongside `oura_health` or those panels show a "visualization not found" error — the
rest of the dashboards use core Splunk visualizations and work without them:

| App | Provides type | Powers |
|-----|---------------|--------|
| [hypnogram_viz](https://github.com/narwhaldc/hypnogram_viz) | `hypnogram_viz.hypnogram` | Sleep → Sleep Stage Timeline; Activity → HR Zone Timeline |
| [charge_ring_viz](https://github.com/narwhaldc/charge_ring_viz) | `charge_ring_viz.chargestatus` | Ring → Charge Status |

Install each the same way (Apps → Install app from file, using the `.spl` in each repo),
then bump Splunk's static-asset cache (`http://your-splunk-host:8000/en-US/_bump`) or
restart, and hard-refresh.

### App structure

```
oura_health/
├── app.conf                          # App metadata and version
├── README.md
├── metadata/
│   └── default.meta
├── static/
│   ├── appIcon.png / appIcon_2x.png
│   ├── appIconAlt.png / appIconAlt_2x.png
├── appserver/
│   └── static/
│       └── bg_health.svg             # Dashboard background artwork
└── default/
    ├── app.conf
    ├── savedsearches.conf            # Battery model, HR baseline, dedup maintenance
    └── data/ui/
        ├── nav/default.xml           # Navigation bar
        └── views/
            ├── oura_today.xml        # Today dashboard
            ├── oura_sleep.xml        # Sleep dashboard (includes hypnogram)
            ├── oura_heart_rate.xml   # Heart Health dashboard
            ├── oura_activity.xml     # Activity dashboard (incl. stacked zone chart)
            ├── oura_wellness.xml     # Wellness / SpO2 / Temperature dashboard
            └── oura_ring.xml         # Ring battery telemetry + depletion forecast
```

### Clearing stale cached dashboards

If dashboards show old data after an upgrade, Splunk may be serving a user-local cached version. Clear it:

```bash
rm -rf $SPLUNK_HOME/etc/users/YOUR_USERNAME/oura_health
$SPLUNK_HOME/bin/splunk restart
```

Then hard-refresh the browser with `Cmd+Shift+R` (Mac) or `Ctrl+Shift+R` (Linux/Windows).

---

## Cron Automation

Add the following to your crontab (`crontab -e`):

```cron
SHELL=/bin/bash

# Oura → Splunk sync — every 10 minutes, all targets
*/10 * * * *  cd /opt/oura-splunk && source .env && python3 ./oura_to_hec_with_phi.py >> /var/log/oura_sync.log 2>&1
```

One cron entry, one token file, all targets.

> **Why 10 minutes?** Oura publishes daily summaries (sleep, readiness, activity) once per day, hours after you wake up and sync your ring. Heart rate is the only real-time data type. 10 minutes is more than sufficient — even 30 or 60 minutes would work fine. The dedup store prevents duplicate events from checkpoint overlap re-fetches.

> **`SHELL=/bin/bash` placement:** This must be above any command that uses `source`. The `source` command is a bash builtin not available in `/bin/sh`. Place `SHELL=/bin/bash` at the top of the crontab to apply it to all commands.

### Set up the log file

```bash
sudo touch /var/log/oura_sync.log
sudo chown youruser:wheel /var/log/oura_sync.log
sudo chmod 664 /var/log/oura_sync.log
```

### Monitor the log

```bash
tail -f /var/log/oura_sync.log
```

A healthy multi-target run looks like:

```
2026-07-13T08:20:00 INFO     Loaded 2 target(s) from oura_targets.json: ['personal', 'demo']
2026-07-13T08:20:00 INFO     Loaded dedup store from oura_dedup_store.json (52698 entries)
2026-07-13T08:20:00 INFO     Verifying HEC targets...
2026-07-13T08:20:00 INFO       [personal] HEC reachable, token valid (http://your-splunk-host:8088/...)
2026-07-13T08:20:00 INFO       [demo] HEC reachable, token valid (https://demo:8088/...)
2026-07-13T08:20:01 INFO     Starting Oura → Splunk sync | targets=['personal', 'demo'] | types=[...] | filter=True | dry_run=False
2026-07-13T08:20:01 INFO     Processing sleep: 2026-07-11 → 2026-07-13
2026-07-13T08:20:01 INFO       Fetched 2 sleep records (2026-07-11 → 2026-07-13)
2026-07-13T08:20:01 INFO       sleep: sent=0 (personal=0, demo=0) changed=0 skipped_dup=2 fetched=2
...
2026-07-13T08:20:10 INFO     Sync complete.
```

When the dedup store is working correctly, most overlap records show `skipped_dup` rather than `sent`. New records from today and any Oura-revised records show as `sent` (with `changed` counted separately).

---

## Log Rotation

Without rotation, `/var/log/oura_sync.log` grows indefinitely. Create a logrotate config:

```bash
sudo cat > /etc/logrotate.d/oura_sync << 'EOF'
/var/log/oura_sync.log {
    weekly
    rotate 4
    compress
    missingok
    notifempty
    copytruncate
    create 0664 youruser wheel
}
EOF
```

| Option | Meaning |
|--------|---------|
| `weekly` | Rotate once per week |
| `rotate 4` | Keep 4 weeks of history |
| `compress` | Gzip rotated files |
| `missingok` | No error if log file is missing |
| `notifempty` | Skip rotation if file is empty |
| `copytruncate` | Truncate in-place rather than move — avoids write errors if cron runs during rotation |
| `create 0664 youruser wheel` | Recreate log with correct ownership after rotation |

Test the config:

```bash
sudo logrotate -d /etc/logrotate.d/oura_sync   # dry-run
sudo logrotate -f /etc/logrotate.d/oura_sync   # force rotation now
```

---

## Data Reference

### Data types and Oura API endpoints

| Script key | Oura v2 endpoint | Date field | Type |
|-----------|-----------------|------------|------|
| `sleep` | `/daily_sleep` | `day` | Daily scored summary + contributors |
| `sleep_detail` | `/sleep` | `day` | Raw sleep periods (duration, HR, HRV, stages) |
| `readiness` | `/daily_readiness` | `day` | Readiness score + contributors |
| `activity` | `/daily_activity` | `day` | Activity score + breakdown |
| `heart_rate` | `/heartrate` | `timestamp` | Per-5-minute HR samples |
| `spo2` | `/daily_spo2` | `day` | Daily blood oxygen average |
| `workouts` | `/workout` | `start_datetime` | Detected workout sessions |
| `sleep_time` | `/sleep_time` | `day` | Recommended sleep window |
| `battery` | `/ring_battery_level` | `timestamp` | Ring battery telemetry |
| `ring_config` | `/ring_configuration` | `set_up_at` | Ring hardware/software metadata |

### Why two sleep endpoints?

Oura splits sleep data across two endpoints:

- **`daily_sleep`** (`sleep` key) — the scored summary. Contains `score`, contributor sub-scores (efficiency, latency, timing, deep_sleep, rem_sleep, restfulness, total_sleep), and `readiness` sub-object. Use this for the Sleep Score and sleep quality contributors.
- **`/sleep`** (`sleep_detail` key) — raw sleep periods. Contains actual durations in seconds (`total_sleep_duration`, `deep_sleep_duration`, etc.), heart rate, HRV, and `sleep_phase_5_min` (the stage string used for the hypnogram). Use this for duration metrics and the stage timeline.

### Ingestion time overrides

Some data types have date fields that reflect historical events rather than current observation (e.g., `ring_config`'s `set_up_at` is when the ring was originally paired, possibly years ago). For these types, the Splunk `_time` is set to ingestion time rather than the record's date field, so events appear in current dashboard time windows. Currently this applies to: `ring_config`.

### Field filters (stripped before sending to Splunk)

| Data type | Stripped fields | Reason |
|-----------|----------------|--------|
| `activity` | `class_5_min`, `met.items` | 288-char and 1440-element arrays not useful in Splunk |
| `sleep` | `sleep_phase_5_min`, `hrv.items` | Raw per-5-min arrays; daily_sleep only needs summary fields |
| `sleep_detail` | `hrv.items`, `heart_rate.items`, `app_sleep_phase_5_min` | Per-5-min arrays; `sleep_phase_5_min` is kept for the hypnogram |
| `personal_info` | `email` | Stripped defensively — lands in shared multi-user index |

To disable all filtering: `python3 oura_to_hec_with_phi.py --no-filter`  
To see current filters: `python3 oura_to_hec_with_phi.py --show-filters`

### Splunk fields

All events are indexed with:
- `index = oura`
- `sourcetype = oura:ring`
- `oura_data_type = <type>` — the script key from the table above
- `oura_user_id = <id>` — stable Oura account identifier for multi-user joins
- `source = oura:ring:<type>`

---

## Dedup Store

### How it works

HEC does **not** deduplicate — every POST creates a new Splunk event regardless of content. The checkpoint overlap (`OURA_CHECKPOINT_OVERLAP_DAYS=2`) intentionally re-fetches recent records to catch late-arriving daily summaries, which previously caused massive duplication (~97% in testing).

The dedup store (`oura_dedup_store.json`) solves this at the client side. For each record fetched from the Oura API, the script computes:

1. **Identity key** — `id` field for most data types, or `timestamp` for timeseries types (heart_rate, battery) that lack an `id`. Prefixed with `data_type::` to avoid cross-type collisions.
2. **Content hash** — SHA-256 of the full filtered record (sorted JSON). Detects payload changes if Oura revises a record after initial publication.

### Per-target tracking

Each dedup store entry tracks which targets have received the record:

```json
{
  "sleep::abc123-def456": {
    "hash": "a1b2c3d4...",
    "date": "2026-07-12",
    "sent_to": ["personal", "demo"]
  }
}
```

Three outcomes per record per target:

| Identity key | Content hash | Target in sent_to | Action | Log reason |
|-------------|-------------|-------------------|--------|------------|
| New | — | — | Send to all targets | `new` |
| Exists | Changed | — | Re-send to all targets | `changed` |
| Exists | Matches | No | Send to this target only | `new_target` |
| Exists | Matches | Yes | Skip | `duplicate` |

The checkpoint advances only when **all** targets have received all records up to that date. If one target fails, the checkpoint stalls until it catches up — but the healthy target continues receiving new data (tracked in the dedup store, so it won't get dups on the retry).

### Pruning

On each run, entries with a `date` older than `CHECKPOINT_OVERLAP_DAYS + OURA_LOOKBACK_DAYS` are automatically removed. With defaults (2 + 30 = 32 days), the file stays well under 1 MB for a single user.

### CLI flags

| Flag | Effect |
|------|--------|
| `--reset-dedup` | Clears the entire dedup store. All records in the fetch window will be re-sent on the next run. |
| `--reset-dedup --target demo` | Removes only `demo` from all `sent_to` lists. Personal's state is untouched. Use after a demo wipe. |

---

## Scheduled Dedup Maintenance

The app includes three saved searches for index-level dedup maintenance. These handle cases where duplicates enter the index despite client-side dedup (e.g., re-ingestion after an error, backfill overlaps, dedup store reset).

### Saved searches

| Search | Schedule | Purpose |
|--------|----------|---------|
| **Pass 1: Tag Duplicates** | Daily 2:00 AM | `streamstats count by _raw`, writes duplicate `_cd` values to `oura_dup_events.csv`. Also writes run metadata to `oura_dedup_status.csv` for observability. |
| **Pass 2: Delete Duplicates** | Daily 2:15 AM | Joins `oura_dup_events.csv` against the index and pipes matches to `| delete`. If Pass 1 found no dups, the lookup is empty and nothing is deleted. |
| **Verify** | On-demand | Reports `total_events`, `unique_events`, `dup_events`, `dup_pct`. Run anytime as a health check. |

### Requirements

Pass 2 requires the `can_delete` capability on the user/role running the saved search. Without it, Pass 1 still runs and produces the lookup — you get visibility into the dup count without anything being deleted. The `can_delete` requirement acts as an intentional deployment gate.

### Monitoring

Check when Pass 1 last ran and what it found:

```spl
| inputlookup oura_dedup_status.csv
```

---

## Checkpoint Management

The checkpoint file (`oura_checkpoint.json`) tracks the last successfully ingested date per data type:

```json
{
  "sleep": "2026-07-12",
  "sleep_detail": "2026-07-12",
  "readiness": "2026-07-12",
  "activity": "2026-07-12",
  "heart_rate": "2026-07-13",
  "spo2": "2026-07-12",
  "workouts": "2026-07-10",
  "sleep_time": "2026-07-11",
  "battery": "2026-07-13",
  "ring_config": "2026-07-13",
  "personal_info": "2026-07-13"
}
```

### Checkpoint overlap

`OURA_CHECKPOINT_OVERLAP_DAYS=2` causes the script to re-fetch 2 days before the checkpoint on every run. This catches records that were not yet published by Oura when the previous run executed (Oura publishes daily summaries hours after midnight, not at midnight exactly). The dedup store prevents these re-fetched records from creating duplicate Splunk events.

### Checkpoint and multi-target

The checkpoint advances only when all active targets have received all records up to that date. If a target is down, the checkpoint stalls at the last date all targets agreed on. When the target comes back, the re-fetch window covers the gap and the dedup store ensures only the lagging target receives the records.

### Reset a single data type

```bash
python3 - << 'EOF'
import json
with open('oura_checkpoint.json') as f:
    cp = json.load(f)
cp.pop('sleep_detail', None)   # remove this type's checkpoint
with open('oura_checkpoint.json', 'w') as f:
    json.dump(cp, f, indent=2)
print('Done:', cp)
EOF
```

### Full reset

```bash
rm oura_checkpoint.json
# Next run fetches OURA_LOOKBACK_DAYS of history for all types
```

### Backfill from a specific date

```bash
python3 oura_to_hec_with_phi.py --backfill 2026-06-01
```

---

## Troubleshooting

### No data appearing in Splunk

1. Verify HEC is reachable: `curl -k -H "Authorization: Splunk $SPLUNK_HEC_TOKEN" $SPLUNK_HEC_URL`
2. Check the log: `tail -50 /var/log/oura_sync.log`
3. Run dry-run to confirm the script fetches data: `source .env && python3 oura_to_hec_with_phi.py --dry-run`

### HEC pre-flight failures

If the log shows `HEC token REJECTED` or `HEC unreachable`:
1. Verify the token in `oura_targets.json` matches the HEC token in Splunk
2. Verify the URL is correct (including port and `/services/collector/event` path)
3. Check network connectivity to the Splunk instance
4. For `https` targets with self-signed certs, set `verify_ssl: false`

### OAuth2 token errors

If you see `401 Unauthorized` from the Oura API, the refresh token has likely expired. Re-run the auth flow:

```bash
source .env
python3 oura_to_hec_with_phi.py --auth
```

### Dashboard shows N/A or wrong data

1. Run the underlying SPL query directly in Splunk Search to verify field names
2. Check that `index=oura oura_data_type=sleep_detail | head 1 | fieldsummary` returns expected fields
3. If you recently wiped the index: delete the checkpoint and re-ingest, then clear the user-local dashboard cache (see [Clearing stale cached dashboards](#clearing-stale-cached-dashboards))

### Dashboard still shows old version after app upgrade

Splunk caches dashboard definitions per-user. Clear with:

```bash
rm -rf $SPLUNK_HOME/etc/users/youruser/oura_health
$SPLUNK_HOME/bin/splunk restart
```

### Script runs but sends 0 records

Check the log output. If you see `skipped_dup=N` with `sent=0`, the dedup store already has those records — this is normal for overlap re-fetches. If the checkpoint is ahead of available data, either:
- Reset the checkpoint (see above)
- Use `--backfill YYYY-MM-DD` to specify a start date explicitly

### Dedup store out of sync with index

If you deleted events from the index (via `| delete` or index wipe) but the dedup store still has their hashes, new copies won't be sent. Fix with:

```bash
# Reset all targets
python3 oura_to_hec_with_phi.py --reset-dedup

# Or reset a single target after a wipe
python3 oura_to_hec_with_phi.py --reset-dedup --target demo
```

### One target down, other still working

The script logs a warning and skips the failed target for that run. The healthy target continues receiving data. The checkpoint does not advance past the point where the failed target fell behind. When the failed target recovers, the next run catches it up — the dedup store ensures the healthy target doesn't get duplicates.

### `source: not found` in cron

Ensure `SHELL=/bin/bash` is at the top of the crontab. The `source` command is a bash builtin unavailable in `/bin/sh` which is cron's default shell on many distros.

### heart_rate checkpoint advancing to today with no new records

This is normal. `heart_rate` is the only real-time data type — if no new HR samples exist since the last run, the checkpoint advances to today to avoid re-querying the full history window on every cron execution.

### CLI quick reference

| Command | Purpose |
|---------|---------|
| `--auth` | Run OAuth2 authorization flow (once) |
| `--dry-run` | Print HEC payloads without sending (still validates targets) |
| `--backfill YYYY-MM-DD` | Ignore checkpoint, fetch from date |
| `--data-types TYPE [...]` | Limit to specific data types |
| `--target NAME` | Operate on a single target only |
| `--no-filter` | Send complete raw records (no field stripping) |
| `--show-filters` | Print configured field filters and exit |
| `--list-types` | List available data types and exit |
| `--reset-dedup` | Clear dedup store (force re-send); combine with `--target` for selective reset |
