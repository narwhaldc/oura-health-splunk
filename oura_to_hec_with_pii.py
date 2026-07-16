#!/usr/bin/env python3
"""
Oura Ring API → Splunk HEC Forwarder
- OAuth2 authentication with automatic token refresh (PAT also supported as fallback)
- Pulls all Oura v2 data types since last checkpoint
- Per-record field filtering with per-type strip lists
- Multi-target HEC support (send to multiple Splunk instances per run)
- Client-side dedup store with per-target tracking
- Checkpoint saved per-type so partial failures don't lose progress

Usage:
    # First run: perform OAuth2 authorization (opens browser)
    python oura_to_hec_with_pii.py --auth

    # Normal incremental sync
    python oura_to_hec_with_pii.py

    # Dry run — print HEC payloads, send nothing
    python oura_to_hec_with_pii.py --dry-run

    # Historical backfill from a date
    python oura_to_hec_with_pii.py --backfill 2024-01-01

    # Reset dedup store (re-send everything in the fetch window)
    python oura_to_hec_with_pii.py --reset-dedup

    # Operate on a single target only
    python oura_to_hec_with_pii.py --target demo --reset-dedup

    # Show what fields are stripped per type
    python oura_to_hec_with_pii.py --show-filters

Required env vars (single-target mode — no targets config file):
    OURA_CLIENT_ID      - OAuth2 client ID from developer.ouraring.com
    OURA_CLIENT_SECRET  - OAuth2 client secret
    SPLUNK_HEC_URL      - e.g. https://splunk.example.com:8088/services/collector/event
    SPLUNK_HEC_TOKEN    - Splunk HEC token

Multi-target mode:
    Set OURA_TARGETS_FILE (default: ./oura_targets.json) pointing to a JSON
    config file. SPLUNK_HEC_URL/TOKEN/INDEX/SOURCETYPE env vars are ignored
    when a targets file exists. See INSTALL.md for format.

Optional env vars:
    OURA_PAT              - Legacy Personal Access Token (fallback if no OAuth2 creds)
    OURA_TOKEN_FILE       - Path to persisted OAuth2 tokens (default: ./oura_tokens.json)
    OURA_CHECKPOINT_FILE  - Path to checkpoint JSON (default: ./oura_checkpoint.json)
    OURA_DEDUP_FILE       - Path to dedup store JSON (default: ./oura_dedup_store.json)
    OURA_TARGETS_FILE     - Path to multi-target config (default: ./oura_targets.json)
    SPLUNK_INDEX          - Splunk index (default: main) — single-target only
    SPLUNK_SOURCETYPE     - Sourcetype (default: oura:ring) — single-target only
    SPLUNK_VERIFY_SSL     - Set "false" to skip TLS verification — single-target only
    OURA_LOOKBACK_DAYS    - Days to look back on first run (default: 30)
"""

import os
import sys
import json
import logging
import argparse
import time
import threading
import webbrowser
import secrets
import hashlib
import base64
import fcntl
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, urlencode

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("oura_to_splunk")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OURA_BASE_URL      = "https://api.ouraring.com/v2/usercollection"
OURA_AUTH_URL      = "https://cloud.ouraring.com/oauth/authorize"
OURA_TOKEN_URL     = "https://api.ouraring.com/oauth/token"
OAUTH_REDIRECT_URI = "http://localhost:8182/callback"
# Scope strings requested during OAuth authorization. `heart_health` (Cardiovascular
# Age) and `stress` (Stress/Resilience) are newer scopes the app must be granted — a
# token minted before these were added returns 401 for those endpoints until you
# re-run --auth. Override via OURA_OAUTH_SCOPES if Oura's exact scope strings differ
# (the consent screen errors with invalid_scope and names the correct one).
OAUTH_SCOPES       = os.getenv(
    "OURA_OAUTH_SCOPES",
    # Newer scopes carry an `extapi:` prefix (confirmed from the dev-console
    # checkbox value, e.g. value="extapi:heart_health"); the legacy 8 stay bare.
    # Bare `heart_health`/`stress` are silently dropped by the authorize endpoint.
    "personal daily heartrate workout tag session spo2 ring_configuration "
    "extapi:heart_health extapi:stress",
)

DEFAULT_LOOKBACK_DAYS = int(os.getenv("OURA_LOOKBACK_DAYS", "30"))

# Number of days to re-fetch before the checkpoint on every run.
# Oura publishes daily summaries (sleep, readiness, activity) hours after
# midnight — if the script ran before the record was available, the
# checkpoint would advance past it. Re-fetching the last 2 days catches
# any records that weren't ready on the previous run.
# HEC does NOT deduplicate — every POST creates a new event regardless of
# content. Dedup is handled client-side by the seen-records store below.
CHECKPOINT_OVERLAP_DAYS = int(os.getenv("OURA_CHECKPOINT_OVERLAP_DAYS", "2"))
CHECKPOINT_FILE    = Path(os.getenv("OURA_CHECKPOINT_FILE", "./oura_checkpoint.json"))
DEDUP_FILE         = Path(os.getenv("OURA_DEDUP_FILE",       "./oura_dedup_store.json"))
TARGETS_FILE       = Path(os.getenv("OURA_TARGETS_FILE",    "./oura_targets.json"))
LOCK_FILE          = Path(os.getenv("OURA_LOCK_FILE",       "./oura_sync.lock"))
TOKEN_FILE         = Path(os.getenv("OURA_TOKEN_FILE",      "./oura_tokens.json"))
SPLUNK_INDEX       = os.getenv("SPLUNK_INDEX",      "main")
SPLUNK_SOURCETYPE  = os.getenv("SPLUNK_SOURCETYPE", "oura:ring")
SPLUNK_VERIFY_SSL  = os.getenv("SPLUNK_VERIFY_SSL", "true").lower() != "false"

# ---------------------------------------------------------------------------
# Endpoint registry
# (endpoint_path, date_field, is_timeseries)
# ---------------------------------------------------------------------------
ENDPOINTS: dict[str, tuple[str, str, bool]] = {
    # (v2 endpoint path, date field, is_timeseries)
    # Endpoint paths verified against Oura v2 API — June 2026
    # "sleep" key maps to daily_sleep (scored summary with contributors/efficiency)
    # "readiness" key maps to daily_readiness
    # "spo2" key maps to daily_spo2
    "sleep":        ("daily_sleep", "day",       False),  # scored daily summary
    "sleep_detail": ("sleep",        "day",       False),  # raw sleep period (duration, HR, HRV, stages)
    "readiness":  ("daily_readiness", "day",       False),
    "activity":   ("daily_activity",  "day",       False),
    "heart_rate": ("heartrate",       "timestamp", True),
    "spo2":       ("daily_spo2",      "day",       False),
    # Cardiovascular Age (Heart Health scope) and Resilience (Stress scope).
    # Endpoint paths verified present via probe 2026-07-15 (returned 401 = exists
    # but token lacked scope, vs 404 for nonexistent paths). Both are daily
    # summaries keyed on "day". Require the heart_health / stress OAuth scopes —
    # re-run --auth after adding them (see OAUTH_SCOPES above).
    "cardio_age": ("daily_cardiovascular_age", "day", False),
    "resilience": ("daily_resilience",         "day", False),
    # "sessions" and "tags" omitted — not used by this ring owner
    # Re-add if needed: "sessions": ("session", "day", False)
    # Re-add if needed: "tags":     ("tag",     "day", False)
    "workouts":   ("workout",         "start_datetime", False),
    "sleep_time": ("sleep_time",      "day",       False),
    # Ring battery level — added to test whether Oura populates this
    # promptly enough to be useful as a low-battery alert source.
    # NOTE: some third-party clients document this endpoint as taking
    # start_datetime/end_datetime instead of start_date/end_date. We're
    # trying the same start_date/end_date convention as every other
    # endpoint first, since that's the documented v2 standard — if it
    # 400s or silently ignores the range, that's the first thing to check.
    "battery":    ("ring_battery_level", "timestamp", True),
    # Ring hardware/software metadata — color, design, firmware version,
    # hardware type, size, and original setup date. Low volume, changes
    # rarely (firmware updates, ring replacement) — cheap to pull on the
    # same schedule as everything else.
    "ring_config": ("ring_configuration", "set_up_at", False),
}

ALL_DATA_TYPES = list(ENDPOINTS.keys())

# Data types whose configured date_field reflects something that happened
# far in the past (e.g. ring_config's set_up_at — when a ring was originally
# paired, possibly years ago) rather than when we're observing the record
# right now. For these, the Splunk event time uses ingestion time instead —
# otherwise events land permanently timestamped to a stale historical date
# and fall outside any reasonable dashboard time window. The date_field
# itself is untouched and still drives checkpoint bookkeeping; this only
# changes what gets used for the indexed _time.
INGESTION_TIME_TYPES = {"ring_config"}

# ---------------------------------------------------------------------------
# Field filters
#
# Fields to STRIP per data type before sending to Splunk.
# These are high-cardinality arrays that bloat events without adding search
# value. All other fields are kept.
#
# Rationale per field:
#   activity.class_5_min   — 288-char string classifying activity per 5 min;
#                            useful for charting in the Oura app, not Splunk
#   activity.met.items     — 1,440 per-minute MET floats; the summary fields
#                            (high/med/low_activity_met_minutes) are sufficient
#   sleep.sleep_phase_5_min — same pattern: raw 5-min phase string
#   sleep.hrv.items        — per-5-min HRV array during sleep; hrv_balance
#                            in readiness contributors covers the summary
#   sessions.heart_rate.items         — per-minute HR during session
#   sessions.heart_rate_variability.items — per-minute HRV during session
#   sessions.motion_count.items       — per-minute motion during session
#
# To keep a stripped field, remove it from the list below.
# To strip additional fields, add them using dot notation for nested keys.
# ---------------------------------------------------------------------------
FIELD_FILTERS: dict[str, list[str]] = {
    "activity": [
        "class_5_min",       # 288-char 5-min activity classification string
        "met.items",         # 1,440-element per-minute MET array
    ],
    "sleep": [
        "sleep_phase_5_min", # raw 5-min sleep stage string
        "hrv.items",         # per-5-min HRV timeseries during sleep
    ],
    "sleep_detail": [
        # sleep_phase_5_min kept — needed for hypnogram stage timeline chart
        # sleep_phase_30_sec kept — higher resolution stage data
        "hrv.items",                     # per-5-min HRV array
        "heart_rate.items",              # per-5-min HR array during sleep
        "app_sleep_phase_5_min",         # duplicate of sleep_phase_5_min
    ],
    "sessions": [
        "heart_rate.items",              # per-minute HR during session
        "heart_rate_variability.items",  # per-minute HRV during session
        "motion_count.items",            # per-minute motion count
    ],
    "personal_info": [
        "email",  # only present if the 'email' OAuth scope is ever granted
                  # (it isn't, currently) — stripped defensively regardless,
                  # since this lands in a shared multi-user index.
    ],
}


# ---------------------------------------------------------------------------
# Target management — multi-instance HEC support
# ---------------------------------------------------------------------------

def load_targets(target_filter: Optional[str] = None) -> dict[str, dict]:
    """
    Load Splunk HEC targets. Checks for a JSON config file first; falls back
    to env vars for backwards-compatible single-target operation.

    Config file format (oura_targets.json):
    {
        "targets": {
            "personal": {
                "hec_url":    "https://splunk:8088/services/collector/event",
                "hec_token":  "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                "index":      "oura",
                "sourcetype": "oura:ring",
                "verify_ssl": false
            },
            "demo": { ... }
        }
    }

    Returns a dict of {name: config} for active targets.
    """
    targets: dict[str, dict] = {}

    if TARGETS_FILE.exists():
        try:
            data = json.loads(TARGETS_FILE.read_text())
            raw = data.get("targets", {})
            for name, cfg in raw.items():
                if not cfg.get("hec_url") or not cfg.get("hec_token"):
                    log.warning("Target '%s' missing hec_url or hec_token — skipping", name)
                    continue
                targets[name] = {
                    "hec_url":    cfg["hec_url"],
                    "hec_token":  cfg["hec_token"],
                    "index":      cfg.get("index", "oura"),
                    "sourcetype": cfg.get("sourcetype", "oura:ring"),
                    "verify_ssl": cfg.get("verify_ssl", True),
                }
            log.info("Loaded %d target(s) from %s: %s",
                     len(targets), TARGETS_FILE, list(targets.keys()))
        except Exception as e:
            log.error("Failed to read targets file %s: %s", TARGETS_FILE, e)
            sys.exit(1)
    else:
        # Fallback: build a single "default" target from env vars
        hec_url   = os.getenv("SPLUNK_HEC_URL")
        hec_token = os.getenv("SPLUNK_HEC_TOKEN")
        if hec_url and hec_token:
            targets["default"] = {
                "hec_url":    hec_url,
                "hec_token":  hec_token,
                "index":      SPLUNK_INDEX,
                "sourcetype": SPLUNK_SOURCETYPE,
                "verify_ssl": SPLUNK_VERIFY_SSL,
            }
            log.info("Using single target from env vars (no %s found)", TARGETS_FILE)

    # Apply --target filter
    if target_filter:
        if target_filter not in targets:
            log.error("Target '%s' not found. Available: %s",
                      target_filter, list(targets.keys()))
            sys.exit(1)
        targets = {target_filter: targets[target_filter]}
        log.info("Filtered to target: %s", target_filter)

    return targets


def apply_field_filter(record: dict, data_type: str) -> dict:
    """
    Remove configured fields from a record before sending to Splunk.
    Supports dot notation for nested keys (e.g. "met.items" removes
    record["met"]["items"] while preserving record["met"]["interval"]).
    Modifies a shallow copy — does not mutate the original.
    """
    strips = FIELD_FILTERS.get(data_type, [])
    if not strips:
        return record

    record = dict(record)  # shallow copy — top-level only

    for dotpath in strips:
        parts = dotpath.split(".", 1)
        if len(parts) == 1:
            record.pop(parts[0], None)
        else:
            parent_key, child_key = parts
            if parent_key in record and isinstance(record[parent_key], dict):
                # Copy the nested dict before mutating
                record[parent_key] = dict(record[parent_key])
                record[parent_key].pop(child_key, None)

    return record


# ---------------------------------------------------------------------------
# OAuth2 — PKCE authorization code flow
# ---------------------------------------------------------------------------

def _pkce_pair() -> tuple[str, str]:
    """Generate a PKCE code_verifier and code_challenge (S256)."""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
    digest   = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


class _CallbackHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler that captures the OAuth2 callback code."""
    code: Optional[str] = None
    state: Optional[str] = None

    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        _CallbackHandler.code  = params.get("code",  [None])[0]
        _CallbackHandler.state = params.get("state", [None])[0]
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<h2>Authorization complete. You can close this tab.</h2>")

    def log_message(self, *_):
        pass  # suppress default request logging


def oauth2_authorize(client_id: str, client_secret: str) -> dict:
    """
    Run the OAuth2 PKCE authorization code flow:
      1. Start a local HTTP server on port 8182
      2. Open the Oura authorization URL in the browser
      3. Wait for the redirect callback
      4. Exchange the code for tokens
    Returns the token dict (access_token, refresh_token, expires_in, ...).
    """
    verifier, challenge = _pkce_pair()
    state = secrets.token_urlsafe(16)

    params = {
        "response_type":         "code",
        "client_id":             client_id,
        "redirect_uri":          OAUTH_REDIRECT_URI,
        "scope":                 OAUTH_SCOPES,
        "state":                 state,
        "code_challenge":        challenge,
        "code_challenge_method": "S256",
    }
    auth_url = f"{OURA_AUTH_URL}?{urlencode(params)}"

    # Start local callback server in a background thread
    server = HTTPServer(("localhost", 8182), _CallbackHandler)
    thread = threading.Thread(target=server.handle_request)
    thread.daemon = True
    thread.start()

    print(f"\nOpening browser for Oura authorization...")
    print(f"If it doesn't open automatically, visit:\n  {auth_url}\n")
    webbrowser.open(auth_url)

    thread.join(timeout=120)
    server.server_close()

    if not _CallbackHandler.code:
        log.error("No authorization code received within 120s")
        sys.exit(1)
    if _CallbackHandler.state != state:
        log.error("OAuth2 state mismatch — possible CSRF. Aborting.")
        sys.exit(1)

    # Exchange code for tokens
    resp = requests.post(
        OURA_TOKEN_URL,
        data={
            "grant_type":    "authorization_code",
            "code":          _CallbackHandler.code,
            "redirect_uri":  OAUTH_REDIRECT_URI,
            "client_id":     client_id,
            "client_secret": client_secret,
            "code_verifier": verifier,
        },
        timeout=15,
    )
    resp.raise_for_status()
    tokens = resp.json()
    tokens["obtained_at"] = datetime.utcnow().isoformat()
    log.info("OAuth2 authorization successful")
    return tokens


def oauth2_refresh(client_id: str, client_secret: str, refresh_token: str) -> dict:
    """
    Exchange a refresh token for a new access token.
    CRITICAL: Oura refresh tokens are single-use. The response contains a NEW
    refresh_token that must be persisted — the old one is immediately invalidated.
    """
    resp = requests.post(
        OURA_TOKEN_URL,
        data={
            "grant_type":    "refresh_token",
            "refresh_token": refresh_token,
            "client_id":     client_id,
            "client_secret": client_secret,
        },
        timeout=15,
    )
    resp.raise_for_status()
    tokens = resp.json()
    tokens["obtained_at"] = datetime.utcnow().isoformat()
    log.info("OAuth2 tokens refreshed successfully")
    return tokens


def load_tokens() -> Optional[dict]:
    if TOKEN_FILE.exists():
        try:
            return json.loads(TOKEN_FILE.read_text())
        except Exception as e:
            log.warning("Could not read token file: %s", e)
    return None


def save_tokens(tokens: dict) -> None:
    tmp = TOKEN_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(tokens, indent=2))
    tmp.replace(TOKEN_FILE)
    TOKEN_FILE.chmod(0o600)  # owner read/write only
    log.debug("Tokens saved to %s", TOKEN_FILE)


def is_token_expired(tokens: dict, buffer_seconds: int = 300) -> bool:
    """Return True if the access token expires within buffer_seconds."""
    obtained_at = tokens.get("obtained_at")
    expires_in  = tokens.get("expires_in")
    if not obtained_at or not expires_in:
        return True  # assume expired if we can't tell
    obtained  = datetime.fromisoformat(obtained_at)
    expires   = obtained + timedelta(seconds=int(expires_in))
    return datetime.utcnow() >= (expires - timedelta(seconds=buffer_seconds))


def build_oura_session(client_id: str, client_secret: str) -> requests.Session:
    """
    Build an authenticated requests.Session for the Oura API.
    Handles token load, refresh, and PAT fallback automatically.
    """
    oura_pat = os.getenv("OURA_PAT")

    # OAuth2 path
    if client_id and client_secret:
        tokens = load_tokens()
        if not tokens:
            log.error(
                "No OAuth2 tokens found. Run:  python oura_to_splunk.py --auth"
            )
            sys.exit(1)

        if is_token_expired(tokens):
            log.info("Access token expired — refreshing...")
            try:
                tokens = oauth2_refresh(client_id, client_secret, tokens["refresh_token"])
                save_tokens(tokens)
            except requests.HTTPError as e:
                log.error("Token refresh failed (%s). Re-run --auth to re-authorize.", e)
                sys.exit(1)

        access_token = tokens["access_token"]

    # PAT fallback
    elif oura_pat:
        log.warning(
            "Using legacy Personal Access Token. PATs were deprecated by Oura "
            "in Dec 2025 — migrate to OAuth2 by setting OURA_CLIENT_ID and "
            "OURA_CLIENT_SECRET and running --auth."
        )
        access_token = oura_pat

    else:
        log.error(
            "No Oura credentials found. Set OURA_CLIENT_ID + OURA_CLIENT_SECRET "
            "(OAuth2) or OURA_PAT (legacy) environment variables."
        )
        sys.exit(1)

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {access_token}",
        "Accept":        "application/json",
    })
    return session


# ---------------------------------------------------------------------------
# Checkpoint management
# ---------------------------------------------------------------------------

def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        try:
            data = json.loads(CHECKPOINT_FILE.read_text())
            log.info("Loaded checkpoint from %s", CHECKPOINT_FILE)
            return data
        except Exception as e:
            log.warning("Could not read checkpoint (%s) — starting fresh", e)
    return {}


def save_checkpoint(checkpoint: dict) -> None:
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(checkpoint, indent=2, default=str))
    tmp.replace(CHECKPOINT_FILE)


# ---------------------------------------------------------------------------
# Dedup store — tracks (data_type, record_key) → content_hash to avoid
# re-sending unchanged records during overlap re-fetches.
# ---------------------------------------------------------------------------

def load_dedup_store() -> dict:
    """
    Load the dedup store from disk. Structure:
    {
        "<data_type>::<record_key>": {
            "hash": "<sha256 hex>",
            "date": "<YYYY-MM-DD>"   # date_field value for pruning
        },
        ...
    }
    """
    if DEDUP_FILE.exists():
        try:
            data = json.loads(DEDUP_FILE.read_text())
            log.info("Loaded dedup store from %s (%d entries)", DEDUP_FILE, len(data))
            return data
        except Exception as e:
            log.warning("Could not read dedup store (%s) — starting fresh", e)
    return {}


def save_dedup_store(store: dict) -> None:
    tmp = DEDUP_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(store, separators=(",", ":"), default=str))
    tmp.replace(DEDUP_FILE)


def prune_dedup_store(store: dict, max_age_days: int = None) -> dict:
    """
    Remove entries whose date is older than max_age_days before today.
    Keeps the file from growing without bound.
    """
    if max_age_days is None:
        max_age_days = CHECKPOINT_OVERLAP_DAYS + DEFAULT_LOOKBACK_DAYS
    cutoff = (date.today() - timedelta(days=max_age_days)).isoformat()
    before = len(store)
    store = {k: v for k, v in store.items() if v.get("date", "") >= cutoff}
    pruned = before - len(store)
    if pruned:
        log.info("Pruned %d stale entries from dedup store (cutoff=%s)", pruned, cutoff)
    return store


def compute_record_key(record: dict, data_type: str) -> str:
    """
    Compute a stable identity key for a record.
    - If the record has an 'id' field, use it (most Oura data types).
    - For timeseries without 'id' (heart_rate, battery), use the
      date_field value which is the timestamp.
    Prefixed with data_type to avoid cross-type collisions.
    """
    record_id = record.get("id")
    if record_id:
        return f"{data_type}::{record_id}"
    _, date_field, _ = ENDPOINTS[data_type]
    date_val = str(record.get(date_field, ""))
    return f"{data_type}::{date_val}"


def compute_content_hash(record: dict) -> str:
    """
    SHA-256 of the record's JSON (sorted keys) to detect payload changes.
    If Oura revises a record (recalculated sleep score, etc.), the hash
    changes and we re-send it — the dashboards pick up the latest via
    dedup id or latest().
    """
    canonical = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def should_send(store: dict, record_key: str, content_hash: str) -> tuple[bool, str]:
    """
    Check whether a record should be sent to HEC (single-target compat).
    Returns (should_send, reason) for logging.
    """
    entry = store.get(record_key)
    if entry is None:
        return True, "new"
    if entry.get("hash") != content_hash:
        return True, "changed"
    return False, "duplicate"


def targets_needing_record(
    store: dict,
    record_key: str,
    content_hash: str,
    all_targets: list[str],
) -> dict[str, str]:
    """
    Determine which targets need this record.
    Returns {target_name: reason} for targets that should receive it.
    Empty dict means all targets already have the current version.
    """
    entry = store.get(record_key)

    if entry is None:
        # Brand new record — all targets need it
        return {t: "new" for t in all_targets}

    if entry.get("hash") != content_hash:
        # Content changed — all targets need the updated version
        return {t: "changed" for t in all_targets}

    # Content unchanged — only targets that haven't received it yet
    sent_to = set(entry.get("sent_to", []))
    needed = {}
    for t in all_targets:
        if t not in sent_to:
            needed[t] = "new_target"
    return needed


def get_start_date(checkpoint: dict, data_type: str,
                   backfill_date: Optional[str] = None) -> str:
    if backfill_date:
        return backfill_date
    if data_type in checkpoint:
        last_date = checkpoint[data_type][:10]
        # Always look back CHECKPOINT_OVERLAP_DAYS before the checkpoint to
        # catch daily summary records (sleep, readiness, activity) that were
        # not yet published by Oura when the previous run executed.
        # These records are timestamped at midnight UTC so they can appear
        # hours after the checkpoint date was recorded.
        overlap_start = (date.fromisoformat(last_date) - timedelta(days=CHECKPOINT_OVERLAP_DAYS)).isoformat()
        log.debug("Checkpoint for %s is %s, fetching from %s (overlap=%dd)",
                  data_type, last_date, overlap_start, CHECKPOINT_OVERLAP_DAYS)
        return overlap_start
    return (date.today() - timedelta(days=DEFAULT_LOOKBACK_DAYS)).isoformat()


# ---------------------------------------------------------------------------
# Oura API fetch
# ---------------------------------------------------------------------------

def fetch_personal_info(session: requests.Session) -> dict:
    """
    Fetch the Oura account's personal_info document. Unlike every other
    endpoint in this script, this is a single-document GET — no start_date/
    end_date, no pagination, no "data" wrapper; the API returns the object
    directly.

    The returned 'id' is tied to the Oura *account*, not the physical ring —
    it survives ring replacement/warranty swaps, which is exactly why it's
    the right field to key multi-user data on rather than any ring-hardware
    identifier from ring_configuration.
    """
    url = f"{OURA_BASE_URL}/personal_info"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.HTTPError as e:
        status = e.response.status_code
        if status == 403:
            log.error(
                "403 fetching personal_info — the 'personal' OAuth scope is "
                "required to tag events with a user id. Re-run --auth if this "
                "app's scopes were changed after the original authorization."
            )
        raise
    return resp.json()


def build_user_context(session: requests.Session) -> tuple[dict, dict]:
    """
    Resolve the stable oura_user_id to stamp on every event this run, and
    return the full personal_info document alongside it so callers (the
    daily snapshot) can reuse it without a second API call.

    Display names are intentionally NOT handled here — that's presentation
    metadata that belongs in a Splunk lookup table (oura_user_id -> display
    name), maintained centrally, not baked into every event at ingest time.
    """
    info = fetch_personal_info(session)
    user_id = info.get("id")
    if not user_id:
        raise RuntimeError(
            "personal_info response had no 'id' field — cannot tag events "
            "with a stable user identifier. Aborting rather than sending "
            "untagged or mistagged data."
        )
    log.info("Resolved Oura account: oura_user_id=%s", user_id)
    return {"oura_user_id": user_id}, info


def fetch_oura_records(
    session: requests.Session,
    data_type: str,
    start_date: str,
    end_date: str,
    user_context: dict,
) -> list[dict]:
    path, _, _ = ENDPOINTS[data_type]
    url    = f"{OURA_BASE_URL}/{path}"
    params: dict = {"start_date": start_date, "end_date": end_date}
    records: list[dict] = []

    while True:
        try:
            resp = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.HTTPError as e:
            status = e.response.status_code
            if status == 429:
                retry_after = int(e.response.headers.get("Retry-After", "60"))
                log.warning("Oura rate-limited — sleeping %ds", retry_after)
                time.sleep(retry_after)
                continue
            elif status == 401:
                log.error(
                    "Oura API 401 Unauthorized for %s. "
                    "If using OAuth2, run --auth to re-authorize.", data_type
                )
                raise
            else:
                log.error("Oura HTTP %d for %s: %s", status, data_type, e)
                raise
        except requests.RequestException as e:
            log.error("Oura request error for %s: %s", data_type, e)
            raise

        payload = resp.json()
        batch   = payload.get("data", [])
        for rec in batch:
            rec["oura_data_type"] = data_type
            rec["oura_user_id"] = user_context["oura_user_id"]
        records.extend(batch)

        next_token = payload.get("next_token")
        if not next_token:
            break
        params["next_token"] = next_token

    log.info("  Fetched %d %s records (%s → %s)",
             len(records), data_type, start_date, end_date)
    return records


# ---------------------------------------------------------------------------
# Splunk HEC
# ---------------------------------------------------------------------------

def send_personal_info_snapshot(
    splunk_session: requests.Session,
    targets:        dict[str, dict],
    info:           dict,
    user_context:   dict,
    checkpoint:     dict,
    dry_run:        bool,
) -> dict:
    """
    Sends a personal_info snapshot at most once per calendar day. Reuses the
    document already fetched for oura_user_id resolution in main() — this
    does NOT make a second API call. Throttled via checkpoint["personal_info"]
    (a date string), the same pattern every other data type already uses.

    personal_info has no natural per-record date field (it's a single
    current-state document, not a history), so unlike everything else in
    this script, the event's Splunk time is just "when we sent it" rather
    than something parsed out of the record itself.
    """
    today = date.today().isoformat()
    if checkpoint.get("personal_info") == today:
        return checkpoint

    record = dict(info)
    record["oura_data_type"] = "personal_info"
    record["oura_user_id"]   = user_context["oura_user_id"]
    record = apply_field_filter(record, "personal_info")

    if dry_run:
        for tname, tcfg in targets.items():
            hec_event = {
                "sourcetype": tcfg["sourcetype"],
                "index":      tcfg["index"],
                "source":     "oura:ring:personal_info",
                "event":      record,
                "time":       time.time(),
            }
            print(f"# target: {tname}")
            print(json.dumps(hec_event, indent=2, default=str))
        log.info("  personal_info: sent=1 to %d target(s) (dry-run)", len(targets))
        checkpoint["personal_info"] = today
        return checkpoint

    all_ok = True
    for tname, tcfg in targets.items():
        hec_event = {
            "sourcetype": tcfg["sourcetype"],
            "index":      tcfg["index"],
            "source":     "oura:ring:personal_info",
            "event":      record,
            "time":       time.time(),
        }
        if not send_to_splunk(splunk_session, tcfg, hec_event, target_name=tname):
            log.warning("  personal_info: send failed for target '%s' — will retry next run", tname)
            all_ok = False

    if all_ok:
        log.info("  personal_info: sent=1 to %d target(s) (daily snapshot)", len(targets))
        checkpoint["personal_info"] = today

    return checkpoint


def record_to_hec_event(record: dict, data_type: str, target: dict) -> dict:
    _, date_field, _ = ENDPOINTS[data_type]
    event_time = None

    if data_type not in INGESTION_TIME_TYPES:
        raw_time = record.get(date_field)
        if raw_time:
            try:
                raw_str = str(raw_time)
                if "T" in raw_str:
                    dt = datetime.fromisoformat(raw_str.replace("Z", "+00:00"))
                else:
                    dt = datetime.fromisoformat(raw_str)
                event_time = dt.timestamp()
            except ValueError:
                pass

    hec_event: dict = {
        "sourcetype": target["sourcetype"],
        "index":      target["index"],
        "source":     f"oura:ring:{data_type}",
        "event":      record,
        "time":       event_time if event_time else time.time(),
    }
    return hec_event


def send_to_splunk(
    session: requests.Session,
    target: dict,
    hec_event: dict,
    target_name: str = "",
    max_retries: int = 3,
) -> bool:
    headers = {
        "Authorization": f"Splunk {target['hec_token']}",
        "Content-Type":  "application/json",
    }
    payload = json.dumps(hec_event, default=str)
    label = f"[{target_name}] " if target_name else ""

    for attempt in range(1, max_retries + 1):
        try:
            # SSL verification is irrelevant for plain HTTP
            verify = target.get("verify_ssl", True) if target["hec_url"].startswith("https") else False
            resp = session.post(
                target["hec_url"], data=payload, headers=headers,
                verify=verify, timeout=15,
            )
            if resp.status_code == 200:
                return True
            elif resp.status_code in (400, 403):
                log.error("%sSplunk HEC permanent error %d: %s",
                          label, resp.status_code, resp.text)
                return False
            else:
                log.warning("%sSplunk HEC %d (attempt %d/%d): %s",
                            label, resp.status_code, attempt, max_retries, resp.text)
        except requests.ConnectionError as e:
            log.warning("%sSplunk HEC connection error (attempt %d/%d): %s",
                        label, attempt, max_retries, e)

        if attempt < max_retries:
            time.sleep(2 ** attempt)

    log.error("%sFailed to send to Splunk after %d attempts", label, max_retries)
    return False


def verify_targets(session: requests.Session, targets: dict[str, dict]) -> dict[str, dict]:
    """
    Pre-flight check: validate connectivity and token for each HEC target.
    Sends a GET to the HEC endpoint — a valid token returns 400/405 (no data),
    while a bad token returns 401/403. Connection errors are also caught.

    Returns only the targets that passed validation. Logs errors for failures.
    """
    verified: dict[str, dict] = {}
    for tname, tcfg in targets.items():
        headers = {"Authorization": f"Splunk {tcfg['hec_token']}"}
        verify = tcfg.get("verify_ssl", True) if tcfg["hec_url"].startswith("https") else False
        try:
            resp = session.get(tcfg["hec_url"], headers=headers, verify=verify, timeout=10)
            if resp.status_code in (200, 400, 405):
                # Token accepted — 400/405 just means "no event data" which is expected for GET
                log.info("  [%s] HEC reachable, token valid (%s)", tname, tcfg["hec_url"])
                verified[tname] = tcfg
            elif resp.status_code in (401, 403):
                log.error("  [%s] HEC token REJECTED (HTTP %d) — check hec_token in targets config: %s",
                          tname, resp.status_code, tcfg["hec_url"])
            else:
                log.warning("  [%s] HEC unexpected status %d — proceeding anyway: %s",
                            tname, resp.status_code, tcfg["hec_url"])
                verified[tname] = tcfg
        except requests.ConnectionError as e:
            log.error("  [%s] HEC unreachable — connection failed: %s", tname, e)
        except Exception as e:
            log.error("  [%s] HEC pre-flight error: %s", tname, e)

    failed = set(targets) - set(verified)
    if failed:
        log.error("Pre-flight failed for target(s): %s — they will be SKIPPED this run", list(failed))

    return verified


# ---------------------------------------------------------------------------
# Per-type pipeline
# ---------------------------------------------------------------------------

def process_data_type(
    oura_session:   requests.Session,
    splunk_session: requests.Session,
    targets:        dict[str, dict],
    data_type:      str,
    checkpoint:     dict,
    dedup_store:    dict,
    backfill_date:  Optional[str],
    dry_run:        bool,
    no_filter:      bool,
    user_context:   dict,
) -> dict:
    end_date   = date.today().isoformat()
    start_date = get_start_date(checkpoint, data_type, backfill_date)

    if start_date > end_date:
        log.info("  %s already current (checkpoint: %s)",
                 data_type, checkpoint.get(data_type))
        return checkpoint

    log.info("Processing %s: %s → %s", data_type, start_date, end_date)

    try:
        records = fetch_oura_records(oura_session, data_type, start_date, end_date, user_context)
    except Exception as e:
        log.error("Skipping %s — fetch error: %s", data_type, e)
        return checkpoint

    if not records:
        checkpoint[data_type] = end_date
        return checkpoint

    _, date_field, _ = ENDPOINTS[data_type]
    latest_date = checkpoint.get(data_type, start_date)
    all_target_names = list(targets.keys())

    # Per-target counters
    sent_counts: dict[str, int] = {t: 0 for t in all_target_names}
    skipped_dup = 0
    sent_changed = 0
    any_failure = False

    strips = FIELD_FILTERS.get(data_type, [])
    if strips and not no_filter:
        log.info("  Stripping fields for %s: %s", data_type, strips)

    for record in records:
        # Apply field filter unless suppressed
        if not no_filter:
            record = apply_field_filter(record, data_type)

        # --- Dedup check (target-aware) ---
        record_key = compute_record_key(record, data_type)
        content_hash = compute_content_hash(record)
        needed = targets_needing_record(dedup_store, record_key, content_hash, all_target_names)

        record_date = str(record.get(date_field, ""))[:10]

        if not needed:
            # All targets already have this record
            skipped_dup += 1
            if record_date > latest_date and not any_failure:
                latest_date = record_date
            continue

        if any(r == "changed" for r in needed.values()):
            sent_changed += 1

        # --- Send to each target that needs it ---
        record_ok = True
        record_succeeded: set[str] = set()
        for tname, reason in needed.items():
            tcfg = targets[tname]

            if dry_run:
                hec_event = record_to_hec_event(record, data_type, tcfg)
                print(f"# target: {tname} ({reason})")
                print(json.dumps(hec_event, indent=2, default=str))
                sent_counts[tname] += 1
                record_succeeded.add(tname)
            else:
                hec_event = record_to_hec_event(record, data_type, tcfg)
                if send_to_splunk(splunk_session, tcfg, hec_event, target_name=tname):
                    sent_counts[tname] += 1
                    record_succeeded.add(tname)
                else:
                    log.warning("[%s] Send failed for %s record_key=%s",
                                tname, data_type, record_key)
                    record_ok = False
                    any_failure = True

        # --- Update dedup store ---
        # Track which targets successfully received this record.
        # On content change, reset sent_to to only the targets that got the new version.
        entry = dedup_store.get(record_key)
        if entry is None or entry.get("hash") != content_hash:
            # New or changed — start fresh sent_to with only this record's successes
            dedup_store[record_key] = {
                "hash": content_hash,
                "date": record_date,
                "sent_to": list(record_succeeded),
            }
        else:
            # Same hash — append newly successful targets
            existing_sent = set(entry.get("sent_to", []))
            existing_sent |= record_succeeded
            entry["sent_to"] = list(existing_sent)

        # Only advance checkpoint if ALL targets have this record
        all_have_it = set(dedup_store[record_key].get("sent_to", [])) >= set(all_target_names)
        if all_have_it and record_date > latest_date:
            latest_date = record_date

    # --- Summary ---
    total_sent = sum(sent_counts.values())
    per_target = ", ".join(f"{t}={c}" for t, c in sent_counts.items())
    log.info("  %s: sent=%d (%s) changed=%d skipped_dup=%d fetched=%d",
             data_type, total_sent, per_target, sent_changed, skipped_dup, len(records))

    if total_sent > 0 or skipped_dup > 0:
        checkpoint[data_type] = latest_date
        log.info("  Checkpoint for %s → %s", data_type, latest_date)
    else:
        # No records sent but also no error — advance anyway to avoid re-querying
        checkpoint[data_type] = end_date

    return checkpoint


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Oura Ring API → Splunk HEC forwarder",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--auth", action="store_true",
        help="Run OAuth2 authorization flow (opens browser). Required on first run.",
    )
    parser.add_argument(
        "--data-types", nargs="+", default=ALL_DATA_TYPES,
        choices=ALL_DATA_TYPES, metavar="TYPE",
        help=f"Data types to pull (default: all). Choices: {', '.join(ALL_DATA_TYPES)}",
    )
    parser.add_argument(
        "--backfill", metavar="YYYY-MM-DD",
        help="Ignore checkpoint and pull from this date forward.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print HEC payloads to stdout — do not send to Splunk.",
    )
    parser.add_argument(
        "--no-filter", action="store_true",
        help="Disable field stripping — send complete raw records.",
    )
    parser.add_argument(
        "--show-filters", action="store_true",
        help="Print configured field filters per data type and exit.",
    )
    parser.add_argument(
        "--list-types", action="store_true",
        help="List available data types and exit.",
    )
    parser.add_argument(
        "--reset-dedup", action="store_true",
        help="Clear the dedup store and re-send all records in the fetch window.",
    )
    parser.add_argument(
        "--target", metavar="NAME",
        help="Operate on a single target only (e.g. --target demo). "
             "Useful with --reset-dedup or --backfill to affect one instance.",
    )
    args = parser.parse_args()

    # -- Informational exits --------------------------------------------------
    if args.list_types:
        print("Available data types:")
        for name, (path, date_field, is_series) in ENDPOINTS.items():
            print(f"  {name:<12} endpoint=/{path:<16} date_field={date_field}")
        sys.exit(0)

    if args.show_filters:
        print("Field filters (stripped before sending to Splunk):")
        any_filters = False
        for dt in ALL_DATA_TYPES:
            strips = FIELD_FILTERS.get(dt, [])
            if strips:
                any_filters = True
                print(f"  {dt}:")
                for f in strips:
                    print(f"    - {f}")
        if not any_filters:
            print("  (none configured)")
        print("\nTo disable all filters: --no-filter")
        print("To edit filters: modify FIELD_FILTERS in the script.")
        sys.exit(0)

    # -- OAuth2 authorization flow --------------------------------------------
    client_id     = os.getenv("OURA_CLIENT_ID", "")
    client_secret = os.getenv("OURA_CLIENT_SECRET", "")

    if args.auth:
        if not client_id or not client_secret:
            log.error("OURA_CLIENT_ID and OURA_CLIENT_SECRET must be set to run --auth")
            sys.exit(1)
        tokens = oauth2_authorize(client_id, client_secret)
        save_tokens(tokens)
        print(f"\nTokens saved to {TOKEN_FILE}")
        print("You can now run the script without --auth for normal syncs.")
        sys.exit(0)

    # -- Acquire exclusive lock -----------------------------------------------
    # Prevents two instances (e.g. cron + manual) from racing on the same
    # checkpoint/dedup store files. Uses fcntl.flock() which auto-releases
    # on process exit or crash — no stale lock cleanup needed.
    lock_fp = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log.error("Another instance is already running (lock file: %s). Exiting.", LOCK_FILE)
        sys.exit(0)
    lock_fp.write(str(os.getpid()))
    lock_fp.flush()

    # -- Load targets ---------------------------------------------------------
    targets = load_targets(target_filter=args.target)
    if not targets and not args.dry_run:
        log.error(
            "No Splunk targets configured. Either create %s or set "
            "SPLUNK_HEC_URL + SPLUNK_HEC_TOKEN env vars.", TARGETS_FILE
        )
        sys.exit(1)
    if not targets and args.dry_run:
        # Dry-run with no targets — create a placeholder so record formatting works
        targets = {"dry_run": {
            "hec_url": "", "hec_token": "",
            "index": SPLUNK_INDEX, "sourcetype": SPLUNK_SOURCETYPE,
            "verify_ssl": True,
        }}

    # -- Build sessions -------------------------------------------------------
    oura_session   = build_oura_session(client_id, client_secret)
    splunk_session = requests.Session()

    # -- Pre-flight: verify HEC connectivity and tokens -----------------------
    if targets.get("dry_run", {}).get("hec_url") == "":
        log.info("Dry-run with no targets file — skipping HEC pre-flight")
    else:
        log.info("Verifying HEC targets...")
        targets = verify_targets(splunk_session, targets)
        if not targets:
            log.error("All targets failed pre-flight — aborting")
            sys.exit(1)

    checkpoint     = load_checkpoint()
    dedup_store    = load_dedup_store()
    if args.reset_dedup:
        if args.target:
            # Selective reset: remove only the specified target from sent_to lists
            log.info("Resetting dedup store for target '%s' (--reset-dedup --target %s)",
                     args.target, args.target)
            for key, entry in dedup_store.items():
                sent_to = entry.get("sent_to", [])
                if args.target in sent_to:
                    sent_to.remove(args.target)
                    entry["sent_to"] = sent_to
            save_dedup_store(dedup_store)
        else:
            log.info("Resetting dedup store (--reset-dedup)")
            dedup_store = {}
            save_dedup_store(dedup_store)
    else:
        dedup_store = prune_dedup_store(dedup_store)

    if args.backfill and not args.reset_dedup:
        log.warning("--backfill without --reset-dedup: records already in the dedup store "
                    "(%d entries) will be SKIPPED. If you want to re-send everything, "
                    "add --reset-dedup. If you're catching up a new target with --target, "
                    "this is expected.", len(dedup_store))

    try:
        user_context, personal_info = build_user_context(oura_session)
    except Exception as e:
        log.error("Could not resolve Oura account identity — aborting sync "
                   "rather than sending untagged data: %s", e)
        sys.exit(1)

    checkpoint = send_personal_info_snapshot(
        splunk_session=splunk_session,
        targets=targets,
        info=personal_info,
        user_context=user_context,
        checkpoint=checkpoint,
        dry_run=args.dry_run,
    )
    if not args.dry_run:
        save_checkpoint(checkpoint)

    log.info("Starting Oura → Splunk sync | targets=%s | types=%s | filter=%s | dry_run=%s",
             list(targets.keys()), args.data_types, not args.no_filter, args.dry_run)

    # -- Run per-type pipeline ------------------------------------------------
    for data_type in args.data_types:
        checkpoint = process_data_type(
            oura_session=oura_session,
            splunk_session=splunk_session,
            targets=targets,
            data_type=data_type,
            checkpoint=checkpoint,
            dedup_store=dedup_store,
            backfill_date=args.backfill,
            dry_run=args.dry_run,
            no_filter=args.no_filter,
            user_context=user_context,
        )
        if not args.dry_run:
            save_checkpoint(checkpoint)
            save_dedup_store(dedup_store)

    log.info("Sync complete.")
    if args.dry_run:
        log.info("(dry-run — nothing sent, checkpoint unchanged)")


if __name__ == "__main__":
    main()
