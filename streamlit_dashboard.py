"""
Real-time Food Delivery KPI Dashboard (v2 — 15 KPIs)
====================================================

Reorganized, slimmed-down dashboard that reads Parquet outputs written by the
three Colab notebooks (Food_order, Courier, Cross_stream_KPI) from an Azure
Blob container. Refreshes on an interval.

Changes vs. the previous 21-KPI version:
  - Removed KPIs that were commented out in the notebooks:
      * delivered_revenue              (Food_order)
      * courier_session_progress       (Courier)
      * offline_mid_delivery_alerts    (Courier)
      * low_battery_active_couriers    (Courier)
  - Dropped the heaviest queries that caused "dying queries" in class demos.
  - Reworked the layout: cleaner top KPIs row, tabbed sections inside each
    view, consistent chart sizing, subtle color palette, helpful captions.
  - Kept all the robustness helpers (decategorize, flatten window, numeric
    fillna, graceful empty-folder banners).

Run locally:
    pip install -r requirements.txt
    streamlit run streamlit_dashboard.py
"""

from __future__ import annotations

import base64
import binascii
import datetime as dt
import os
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# -----------------------------------------------------------------------------
# Azure Blob config -- EDIT THESE if your storage account changes
# -----------------------------------------------------------------------------
ACCOUNT_NAME = "iesstsabbadbaa"
ACCOUNT_KEY = (
    # paste your Azure storage account key here
    "GfD8mpJmqw6gTqzyRpmV5tbHZ7RP1xkiO9X9hgmaMTdnHL1PL62AVmlejOmhHPFkBr2Pfl9DvmUC+AStYJXlzA=="
)
CONTAINER = "group8"

ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT", ACCOUNT_NAME).strip()
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_KEY", ACCOUNT_KEY).strip()
CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", CONTAINER).strip()

STORAGE_OPTIONS = {
    "account_name": ACCOUNT_NAME,
    "account_key": ACCOUNT_KEY,
}


def _validate_azure_config() -> Optional[str]:
    """Return a human-readable configuration error, or None if config looks usable."""
    if not ACCOUNT_NAME or not CONTAINER:
        return "Missing `ACCOUNT_NAME` or `CONTAINER`."
    if not ACCOUNT_KEY or ACCOUNT_KEY == "REPLACE_WITH_ACCOUNT_KEY":
        return (
            "Missing Azure storage key. Set `ACCOUNT_KEY` in the file or export "
            "`AZURE_STORAGE_KEY`."
        )
    try:
        base64.b64decode(ACCOUNT_KEY, validate=True)
    except (binascii.Error, ValueError):
        return (
            "Invalid Azure storage key format. The key must be a full base64 value "
            "copied from Azure Portal (Storage account -> Access keys)."
        )
    return None


CONFIG_ERROR = _validate_azure_config()


def abfs(folder: str) -> str:
    return f"abfs://{CONTAINER}@{ACCOUNT_NAME}.dfs.core.windows.net/{folder}"


# -----------------------------------------------------------------------------
# 15 KPI folder names (must match the *active* notebook writers)
# -----------------------------------------------------------------------------
KPI_FOLDERS = {
    # --- Food-order notebook (7) ---
    "orders_per_minute_zone":      ["orders_per_minute_zone"],
    "prep_kpi":                    ["prep_kpi"],
    "order_anomalies":             ["order_anomalies"],
    "orders_demand_zone":          ["orders_demand_zone"],
    "cancel_refund_hotspots":      ["cancel_refund_hotspots"],
    "conversion_funnel_zone":      ["conversion_funnel_zone", "conversion_funnel_zore"],
    "aov_by_zone":                 ["aov_by_zone", "aov_by_zore"],
    # --- Courier notebook (6) ---
    "available_couriers_zone":     ["available_couriers_zone", "available_curiers_zone"],
    "active_delivery_load_zone":   ["active_delivery_load_zone"],
    "courier_dropoff_hotspots":    ["courier_dropoff_hotspots", "courier_dropof_hotspots"],
    "courier_idle_time_by_vehicle":["courier_idle_time_by_vehicle"],
    "courier_utilization":         ["courier_utilization"],
    "courier_throughput_hourly":   ["courier_throughput_hourly"],
    # --- Cross-stream notebook (2) ---
    "supply_demand_imbalance":     ["supply_demand_imbalance"],
    "pickup_wait_per_order":       ["pickup_wait_per_order"],
}

DEFAULT_REFRESH_SEC = 10

# Friendlier palette for plots (colour-blind-friendly-ish)
PALETTE = px.colors.qualitative.Set2

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _decategorize(df: pd.DataFrame) -> pd.DataFrame:
    """Cast every categorical column to string (prevents fillna / merge blowups)."""
    if df.empty:
        return df
    cat_cols = df.select_dtypes(include=["category"]).columns.tolist()
    for c in cat_cols:
        df[c] = df[c].astype("string")
    return df


def _flatten_window(df: pd.DataFrame) -> pd.DataFrame:
    """Expand a struct-like `window` column to window_start / window_end."""
    if "window" in df.columns and df["window"].dtype == "object":
        try:
            win = pd.json_normalize(df["window"])
            if {"start", "end"}.issubset(win.columns):
                df = df.drop(columns=["window"]).copy()
                df["window_start"] = pd.to_datetime(win["start"])
                df["window_end"] = pd.to_datetime(win["end"])
        except Exception:
            pass
    return df


def _fillna_numeric(df: pd.DataFrame, value=0) -> pd.DataFrame:
    """Numeric-only fillna — keeps categorical / string columns untouched."""
    if df.empty:
        return df
    num = df.select_dtypes(include="number").columns
    if len(num):
        df[num] = df[num].fillna(value)
    return df


@st.cache_data(ttl=45, show_spinner=False)
def load_kpi(folder_key: str) -> pd.DataFrame:
    """Read a KPI parquet folder from Azure Blob."""
    folders = KPI_FOLDERS[folder_key]
    if CONFIG_ERROR:
        return pd.DataFrame({"_error": [f"ConfigurationError: {CONFIG_ERROR}"]})

    last_err: Optional[Exception] = None
    for folder in folders:
        url = abfs(folder)
        try:
            df = pd.read_parquet(url, storage_options=STORAGE_OPTIONS)
            break
        except Exception as err:
            last_err = err
    else:
        return pd.DataFrame({"_error": [f"{type(last_err).__name__}: {last_err}"]})

    df = _decategorize(df)
    df = _flatten_window(df)
    for col in ("window_end", "event_time", "hour"):
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col])
                df = df.sort_values(col)
            except Exception:
                pass
            break
    return df


def latest_window(df: pd.DataFrame) -> pd.DataFrame:
    """Return only rows from the most recent window_end."""
    if df.empty or "window_end" not in df.columns:
        return df
    latest = df["window_end"].max()
    return df[df["window_end"] == latest].copy()


def error_banner(df: pd.DataFrame, name: str) -> bool:
    """Friendly banner if the frame is empty / errored. Returns True if handled."""
    if df.empty:
        st.info(f"⏳ No data yet for **{name}** — streams may still be warming up.")
        return True
    if "_error" in df.columns:
        st.warning(
            f"⚠️ **{name}** is not readable yet. First batch probably hasn't landed.\n\n"
            f"`{df['_error'].iloc[0]}`"
        )
        return True
    return False


def _section_header(title: str, subtitle: str = "") -> None:
    st.markdown(f"### {title}")
    if subtitle:
        st.caption(subtitle)


# -----------------------------------------------------------------------------
# Page setup + light theming
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Food Delivery Realtime KPIs",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    /* tighten up the default Streamlit padding a bit */
    .block-container { padding-top: 1.4rem; padding-bottom: 2rem; }
    /* subtle card look for metric rows */
    div[data-testid="stMetric"] {
        background: rgba(120, 120, 120, 0.06);
        border: 1px solid rgba(120, 120, 120, 0.15);
        border-radius: 10px;
        padding: 14px 16px;
    }
    div[data-testid="stMetricLabel"] { opacity: 0.75; font-weight: 500; }
    /* pill-style tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 4px; }
    .stTabs [data-baseweb="tab"] {
        background: rgba(120, 120, 120, 0.08);
        border-radius: 8px 8px 0 0;
        padding: 8px 14px;
    }
    /* alert cards for Overview */
    .alert-card {
        border-radius: 10px;
        padding: 14px 16px;
        margin-bottom: 10px;
        border-left: 4px solid #999;
        background: rgba(120, 120, 120, 0.06);
    }
    .alert-card.red    { border-left-color: #e45756; background: rgba(228, 87, 86, 0.10); }
    .alert-card.amber  { border-left-color: #f2a93b; background: rgba(242, 169, 59, 0.10); }
    .alert-card.green  { border-left-color: #4c9a6a; background: rgba(76, 154, 106, 0.08); }
    .alert-card.gray   { border-left-color: #9aa0a6; background: rgba(154, 160, 166, 0.08); }
    .alert-title { font-weight: 600; font-size: 0.95rem; margin-bottom: 2px; }
    .alert-body  { font-size: 0.85rem; opacity: 0.85; }
    .status-pill {
        display: inline-block; padding: 2px 9px; border-radius: 999px;
        font-size: 0.75rem; font-weight: 600; margin-right: 8px;
    }
    .status-pill.red    { background: #e45756; color: white; }
    .status-pill.amber  { background: #f2a93b; color: white; }
    .status-pill.green  { background: #4c9a6a; color: white; }
    .status-pill.gray   { background: #9aa0a6; color: white; }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------
st.sidebar.title("⚙️ Dashboard Settings")

section = st.sidebar.radio(
    "View",
    ["📊 Overview", "📡 Operational", "📦 Orders", "🛵 Couriers", "🔀 Cross-stream"],
    help="Only the selected view pulls data — keeps refresh snappy.",
)

refresh_sec = st.sidebar.slider(
    "Refresh interval (seconds)", 3, 60, DEFAULT_REFRESH_SEC
)
st_autorefresh(interval=refresh_sec * 1000, key="kpi_refresh")

if st.sidebar.button("🔄 Clear cache & reload", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.caption(f"**Container:** `{CONTAINER}`")
st.sidebar.caption(f"**Account:** `{ACCOUNT_NAME}`")
st.sidebar.caption(f"**Last refresh:** {dt.datetime.now().strftime('%H:%M:%S')}")
st.sidebar.caption("15 KPIs · 7 orders · 6 couriers · 2 cross-stream")

if CONFIG_ERROR:
    st.sidebar.error(f"Azure config issue: {CONFIG_ERROR}")

# -----------------------------------------------------------------------------
# Header
# -----------------------------------------------------------------------------
st.title("🚚 Food Delivery — Realtime KPI Dashboard")
st.caption(
    f"Streaming Parquet from Azure Blob · auto-refresh every **{refresh_sec}s** · "
    f"showing view: **{section}**"
)
st.divider()


# =============================================================================
# OVERVIEW
# =============================================================================
if section == "📊 Overview":

    # -------------------------------------------------------------------------
    # BUSINESS OVERVIEW — alert-style, latest window only, GM / Ops-lead audience
    # Priorities: Supply/demand balance + Customer experience
    # -------------------------------------------------------------------------

    imbalance = load_kpi("supply_demand_imbalance")
    wait      = load_kpi("pickup_wait_per_order")
    cancel    = load_kpi("cancel_refund_hotspots")
    funnel    = load_kpi("conversion_funnel_zone")
    prep      = load_kpi("prep_kpi")

    # ---- Threshold config (locked in with Trinity) --------------------------
    SD_RATIO_RED   = 0.8   # below this = under-supplied
    SD_RATIO_AMBER = 1.2   # above this = healthy/over-supplied
    WAIT_RED_SEC   = 300   # P90 pickup wait
    WAIT_AMBER_SEC = 180
    PREP_RED_SEC   = 900   # 15 min avg prep
    PREP_AMBER_SEC = 600   # 10 min avg prep

    def _status(value, red_thr, amber_thr, higher_is_worse=True):
        """Return ('red'|'amber'|'green', label) based on thresholds."""
        if value is None or pd.isna(value):
            return ("gray", "No data")
        if higher_is_worse:
            if value >= red_thr:   return ("red", "Critical")
            if value >= amber_thr: return ("amber", "Watch")
            return ("green", "Healthy")
        else:
            if value <= red_thr:   return ("red", "Critical")
            if value <= amber_thr: return ("amber", "Watch")
            return ("green", "Healthy")

    def _alert_card(status, title, body):
        st.markdown(
            f'<div class="alert-card {status}">'
            f'<div class="alert-title">'
            f'<span class="status-pill {status}">{status.upper()}</span>{title}'
            f'</div>'
            f'<div class="alert-body">{body}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown("## 🚨 Business Overview — alerts for the latest window")
    st.caption(
        "Surface-level status across the platform. Green = healthy · Amber = watch · "
        "Red = act now. All values are from the most recent streaming window only."
    )

    # =========================================================================
    # HEADLINE STATUS STRIP — 4 big metrics with colored status
    # =========================================================================
    hl1, hl2, hl3, hl4 = st.columns(4)

    # --- Supply/Demand balance (platform-wide, latest window) ----------------
    platform_ratio = None
    zones_under = 0
    zones_total = 0
    if not imbalance.empty and "_error" not in imbalance.columns:
        i_latest = latest_window(imbalance) if "window_end" in imbalance.columns else imbalance
        demand_col = next((c for c in ("orders_demand", "demand", "orders") if c in i_latest.columns), None)
        supply_col = next((c for c in ("available_couriers", "supply", "couriers") if c in i_latest.columns), None)
        ratio_col  = next((c for c in ("supply_demand_ratio", "ratio") if c in i_latest.columns), None)
        if demand_col and supply_col:
            total_d = i_latest[demand_col].sum()
            total_s = i_latest[supply_col].sum()
            platform_ratio = (total_s / total_d) if total_d > 0 else None
            if ratio_col and "zone_id" in i_latest.columns:
                zones_total = i_latest["zone_id"].nunique()
                zones_under = (i_latest[ratio_col] < SD_RATIO_RED).sum()

    sd_status, sd_label = _status(
        platform_ratio, SD_RATIO_RED, SD_RATIO_AMBER, higher_is_worse=False
    )
    with hl1:
        ratio_text = f"{platform_ratio:.2f}" if platform_ratio is not None else "—"
        if zones_total:
            sub_text = f"{zones_under}/{zones_total} zones under-supplied"
        else:
            sub_text = "Couriers ÷ orders"
        st.markdown(
            f'<div class="alert-card {sd_status}">'
            f'<div class="alert-title">⚖️ Supply / Demand</div>'
            f'<div style="font-size:1.6rem;font-weight:700;margin:4px 0;">{ratio_text}</div>'
            f'<div class="alert-body">'
            f'<span class="status-pill {sd_status}">{sd_label}</span>{sub_text}'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # --- Pickup wait (customer experience) -----------------------------------
    p90_wait = None
    n_orders_wait = 0
    if not wait.empty and "_error" not in wait.columns:
        w_latest = latest_window(wait) if "window_end" in wait.columns else wait.tail(500)
        wait_col = next(
            (c for c in ("pickup_wait_sec", "pickup_wait", "wait_sec") if c in w_latest.columns),
            None,
        )
        if wait_col and len(w_latest):
            p90_wait = float(w_latest[wait_col].quantile(0.9))
            n_orders_wait = len(w_latest)

    wait_status, wait_label = _status(
        p90_wait, WAIT_RED_SEC, WAIT_AMBER_SEC, higher_is_worse=True
    )
    with hl2:
        wait_text = f"{p90_wait:.0f}s" if p90_wait is not None else "—"
        st.markdown(
            f'<div class="alert-card {wait_status}">'
            f'<div class="alert-title">⏱ Pickup wait (P90)</div>'
            f'<div style="font-size:1.6rem;font-weight:700;margin:4px 0;">{wait_text}</div>'
            f'<div class="alert-body">'
            f'<span class="status-pill {wait_status}">{wait_label}</span>'
            f'Across {n_orders_wait} orders'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # --- Avg prep time (customer experience, kitchen side) -------------------
    avg_prep = None
    if not prep.empty and "_error" not in prep.columns:
        p_latest = latest_window(prep) if "window_end" in prep.columns else prep
        prep_col = next(
            (c for c in ("avg_prep_sec", "avg_prep_time_sec", "avg_prep_time", "prep_time")
             if c in p_latest.columns),
            None,
        )
        if prep_col and len(p_latest):
            avg_prep = float(p_latest[prep_col].mean())

    prep_status, prep_label = _status(
        avg_prep, PREP_RED_SEC, PREP_AMBER_SEC, higher_is_worse=True
    )
    with hl3:
        prep_text = f"{avg_prep:.0f}s" if avg_prep is not None else "—"
        st.markdown(
            f'<div class="alert-card {prep_status}">'
            f'<div class="alert-title">🍳 Avg prep time</div>'
            f'<div style="font-size:1.6rem;font-weight:700;margin:4px 0;">{prep_text}</div>'
            f'<div class="alert-body">'
            f'<span class="status-pill {prep_status}">{prep_label}</span>'
            f'Kitchen performance'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # --- Cancellation rate (from conversion funnel) --------------------------
    cancel_rate = None
    cancel_count_total = 0
    placed_total = 0
    if not funnel.empty and "_error" not in funnel.columns:
        f_latest = latest_window(funnel) if "window_end" in funnel.columns else funnel
        placed_col    = next((c for c in ("placed", "n_placed", "orders_placed") if c in f_latest.columns), None)
        cancelled_col = next((c for c in ("cancelled", "n_cancelled", "cancellations") if c in f_latest.columns), None)
        if placed_col and cancelled_col and len(f_latest):
            placed_total = float(f_latest[placed_col].sum())
            cancel_count_total = float(f_latest[cancelled_col].sum())

            # If the latest window has no order activity, use the most recent
            # non-empty window so the card still reflects real data in Blob.
            if placed_total <= 0 and "window_end" in funnel.columns:
                active = funnel[(funnel[placed_col].fillna(0) > 0) | (funnel[cancelled_col].fillna(0) > 0)]
                if not active.empty:
                    f_latest = latest_window(active)
                    placed_total = float(f_latest[placed_col].sum())
                    cancel_count_total = float(f_latest[cancelled_col].sum())

            if placed_total > 0:
                cancel_rate = cancel_count_total / placed_total

    # Cancellation thresholds: >10% red, >5% amber
    cx_status, cx_label = _status(
        cancel_rate, 0.10, 0.05, higher_is_worse=True
    )
    with hl4:
        cancel_text = f"{cancel_rate*100:.1f}%" if cancel_rate is not None else "—"
        st.markdown(
            f'<div class="alert-card {cx_status}">'
            f'<div class="alert-title">❌ Cancel rate</div>'
            f'<div style="font-size:1.6rem;font-weight:700;margin:4px 0;">{cancel_text}</div>'
            f'<div class="alert-body">'
            f'<span class="status-pill {cx_status}">{cx_label}</span>'
            f'{int(cancel_count_total)} of {int(placed_total)} orders'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    st.divider()

    # =========================================================================
    # ZONE-LEVEL ALERTS — what needs a GM's attention RIGHT NOW
    # =========================================================================
    left, right = st.columns(2)

    # --- LEFT: supply/demand zone alerts -------------------------------------
    with left:
        _section_header(
            "⚖️ Supply vs demand — zone alerts",
            f"Red if ratio < {SD_RATIO_RED} · Amber if < {SD_RATIO_AMBER} · else Healthy.",
        )
        if not error_banner(imbalance, "supply_demand_imbalance"):
            i_latest = latest_window(imbalance) if "window_end" in imbalance.columns else imbalance
            ratio_col  = next((c for c in ("supply_demand_ratio", "ratio") if c in i_latest.columns), None)
            demand_col = next((c for c in ("orders_demand", "demand", "orders") if c in i_latest.columns), None)
            supply_col = next((c for c in ("available_couriers", "supply", "couriers") if c in i_latest.columns), None)

            if ratio_col and "zone_id" in i_latest.columns:
                z = (
                    i_latest.groupby("zone_id", as_index=False, observed=True)
                    .agg({
                        ratio_col: "mean",
                        **({demand_col: "sum"} if demand_col else {}),
                        **({supply_col: "sum"} if supply_col else {}),
                    })
                    .sort_values(ratio_col)
                )

                shown = 0
                for _, row in z.iterrows():
                    r_val = row[ratio_col]
                    s, label = _status(r_val, SD_RATIO_RED, SD_RATIO_AMBER, higher_is_worse=False)
                    if s == "green" and shown >= 3:
                        continue  # don't flood with healthy zones after first 3
                    parts = [f"Ratio <b>{r_val:.2f}</b>"]
                    if demand_col: parts.append(f"demand {int(row[demand_col])}")
                    if supply_col: parts.append(f"supply {int(row[supply_col])}")
                    if s == "red":
                        action = "→ Move couriers in, trigger surge, or throttle new orders."
                    elif s == "amber":
                        action = "→ Monitor; consider nudging couriers to this zone."
                    else:
                        action = "→ No action needed."
                    _alert_card(
                        s,
                        f"Zone <b>{row['zone_id']}</b> — {label}",
                        " · ".join(parts) + f"<br>{action}",
                    )
                    shown += 1
                if shown == 0:
                    st.info("No zones in the latest window.")
            else:
                st.caption("Missing `ratio` or `zone_id` columns — can't build zone alerts.")

    # --- RIGHT: customer experience alerts -----------------------------------
    with right:
        _section_header(
            "🙋 Customer experience — zone alerts",
            f"Pickup wait P90: red > {WAIT_RED_SEC}s · amber > {WAIT_AMBER_SEC}s. "
            "Plus any zone with cancel/refund spikes.",
        )

        # Pickup-wait by zone (P90)
        ce_alerts_shown = 0
        if not wait.empty and "_error" not in wait.columns:
            w_latest = latest_window(wait) if "window_end" in wait.columns else wait.tail(500)
            wait_col = next(
                (c for c in ("pickup_wait_sec", "pickup_wait", "wait_sec") if c in w_latest.columns),
                None,
            )
            if wait_col and "zone_id" in w_latest.columns:
                z_wait = (
                    w_latest.groupby("zone_id", as_index=False, observed=True)[wait_col]
                    .quantile(0.9)
                    .rename(columns={wait_col: "p90"})
                    .sort_values("p90", ascending=False)
                )
                for _, row in z_wait.iterrows():
                    s, label = _status(row["p90"], WAIT_RED_SEC, WAIT_AMBER_SEC, higher_is_worse=True)
                    if s == "green" and ce_alerts_shown >= 2:
                        continue
                    if s == "red":
                        action = "→ Kitchen timing or courier coverage is broken in this zone."
                    elif s == "amber":
                        action = "→ Food is cooling on counters. Watch next window."
                    else:
                        action = "→ Healthy."
                    _alert_card(
                        s,
                        f"Zone <b>{row['zone_id']}</b> pickup wait — {label}",
                        f"P90 wait <b>{row['p90']:.0f}s</b><br>{action}",
                    )
                    ce_alerts_shown += 1

        # Cancel/refund hotspots
        if not cancel.empty and "_error" not in cancel.columns:
            c_latest = latest_window(cancel) if "window_end" in cancel.columns else cancel
            count_col = next(
                (c for c in ("cancel_refund_count", "cancel_count", "count") if c in c_latest.columns),
                None,
            )
            if count_col and "zone_id" in c_latest.columns:
                c_z = (
                    c_latest.groupby("zone_id", as_index=False, observed=True)[count_col]
                    .sum()
                    .sort_values(count_col, ascending=False)
                )
                top_cancel = c_z.head(3)
                for _, row in top_cancel.iterrows():
                    n = int(row[count_col])
                    if n == 0:
                        continue
                    # Absolute-count thresholds: >=10 red, >=5 amber
                    if n >= 10:
                        s, label = "red", "Critical"
                        action = "→ Investigate: supply shock, pricing bug, or restaurant outage."
                    elif n >= 5:
                        s, label = "amber", "Watch"
                        action = "→ Cluster forming. Check if it's one restaurant or system-wide."
                    else:
                        s, label = "green", "Healthy"
                        action = "→ Normal noise."
                    _alert_card(
                        s,
                        f"Zone <b>{row['zone_id']}</b> cancels/refunds — {label}",
                        f"<b>{n}</b> events in latest window<br>{action}",
                    )
                    ce_alerts_shown += 1

        if ce_alerts_shown == 0:
            st.info("No customer-experience signal in the latest window.")

    st.divider()

    # =========================================================================
    # FOOTER — quick legend so the GM knows what each color means
    # =========================================================================
    st.caption(
        "**Thresholds**  ·  "
        f"Supply/demand: red < {SD_RATIO_RED}, amber < {SD_RATIO_AMBER}  ·  "
        f"Pickup wait P90: red > {WAIT_RED_SEC}s, amber > {WAIT_AMBER_SEC}s  ·  "
        f"Prep time: red > {PREP_RED_SEC}s, amber > {PREP_AMBER_SEC}s  ·  "
        "Cancel rate: red > 10%, amber > 5%  ·  "
        "Cancel hotspot: red ≥ 10 events, amber ≥ 5."
    )


# =============================================================================
# OPERATIONAL  (the old overview — top-line metrics + live trends)
# =============================================================================
elif section == "📡 Operational":

    opm   = load_kpi("orders_per_minute_zone")
    cour  = load_kpi("available_couriers_zone")
    util  = load_kpi("courier_utilization")
    aov   = load_kpi("aov_by_zone")

    _section_header(
        "Top-line health",
        "Latest streaming window across all zones.",
    )

    c1, c2, c3, c4 = st.columns(4)

    # Orders / min
    if not opm.empty and "_error" not in opm.columns:
        last = latest_window(opm)
        opm_col = next(
            (c for c in ("orders_per_minute", "orders", "order_count") if c in last.columns),
            None,
        )
        c1.metric("Orders / min", int(last[opm_col].sum()) if opm_col else "—")
    else:
        c1.metric("Orders / min", "—")

    # Available couriers
    if not cour.empty and "_error" not in cour.columns:
        last = latest_window(cour)
        col = next(
            (c for c in ("available_couriers", "couriers", "n_couriers") if c in last.columns),
            None,
        )
        c2.metric("Available couriers", int(last[col].sum()) if col else "—")
    else:
        c2.metric("Available couriers", "—")

    # Avg utilization
    if not util.empty and "_error" not in util.columns:
        util_col = next(
            (c for c in ("utilization", "utilization_pct", "utilization_rate") if c in util.columns),
            None,
        )
        if util_col:
            scale = 100 if util[util_col].max() <= 1 else 1
            c3.metric("Avg courier utilization", f"{util[util_col].mean() * scale:.1f}%")
        else:
            c3.metric("Avg utilization", "—")
    else:
        c3.metric("Avg utilization", "—")

    # Avg order value (latest window)
    if not aov.empty and "_error" not in aov.columns:
        a_last = latest_window(aov)
        aov_col = next(
            (c for c in ("avg_order_value", "aov", "average_order_value") if c in a_last.columns),
            None,
        )
        if aov_col:
            c4.metric("Avg order value", f"${a_last[aov_col].mean():,.2f}")
        else:
            c4.metric("Avg order value", "—")
    else:
        c4.metric("Avg order value", "—")

    st.divider()

    # Two-up live charts
    l, r = st.columns(2)

    with l:
        _section_header("📈 Orders per minute by zone")
        if not error_banner(opm, "orders_per_minute_zone"):
            opm_col = next(
                (c for c in ("orders_per_minute", "orders", "order_count") if c in opm.columns),
                None,
            )
            if opm_col:
                fig = px.line(
                    opm, x="window_end", y=opm_col, color="zone_id",
                    markers=True, height=360,
                    color_discrete_sequence=PALETTE,
                )
                fig.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.dataframe(opm.tail(50), use_container_width=True)

    with r:
        _section_header("🛵 Available couriers by zone")
        if not error_banner(cour, "available_couriers_zone"):
            col = next(
                (c for c in ("available_couriers", "couriers", "n_couriers") if c in cour.columns),
                None,
            )
            if col:
                fig = px.line(
                    cour, x="window_end", y=col, color="zone_id",
                    markers=True, height=360,
                    color_discrete_sequence=PALETTE,
                )
                fig.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.dataframe(cour.tail(50), use_container_width=True)


# =============================================================================
# ORDERS  (7 KPIs)
# =============================================================================
elif section == "📦 Orders":

    prep    = load_kpi("prep_kpi")
    anom    = load_kpi("order_anomalies")
    demand  = load_kpi("orders_demand_zone")
    cancel  = load_kpi("cancel_refund_hotspots")
    funnel  = load_kpi("conversion_funnel_zone")
    aov     = load_kpi("aov_by_zone")
    opm     = load_kpi("orders_per_minute_zone")

    tab_volume, tab_quality, tab_money = st.tabs(
        ["📈 Volume & demand", "🧪 Quality & anomalies", "💰 Revenue & value"]
    )

    # --- Volume & demand ---
    with tab_volume:
        col1, col2 = st.columns(2)

        with col1:
            _section_header("Orders per minute by zone")
            if not error_banner(opm, "orders_per_minute_zone"):
                opm_col = next(
                    (c for c in ("orders_per_minute", "orders", "order_count") if c in opm.columns),
                    None,
                )
                if opm_col and "window_end" in opm.columns:
                    fig = px.line(
                        opm, x="window_end", y=opm_col, color="zone_id",
                        markers=True, height=340,
                        color_discrete_sequence=PALETTE,
                    )
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(opm.tail(30), use_container_width=True)

        with col2:
            _section_header("Demand per zone (latest window)")
            if not error_banner(demand, "orders_demand_zone"):
                d = latest_window(demand)
                demand_col = next(
                    (c for c in ("orders_demand", "orders", "demand") if c in d.columns),
                    None,
                )
                if demand_col and "zone_id" in d.columns:
                    d = d.groupby("zone_id", as_index=False, observed=True)[demand_col].sum()
                    d = d.sort_values(demand_col, ascending=False)
                    fig = px.bar(
                        d, x="zone_id", y=demand_col, height=340,
                        color="zone_id", color_discrete_sequence=PALETTE,
                    )
                    fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(d.tail(20), use_container_width=True)

    # --- Quality & anomalies ---
    with tab_quality:
        col1, col2 = st.columns(2)

        with col1:
            _section_header("Prep-time KPI")
            if not error_banner(prep, "prep_kpi"):
                prep_col = next(
                    (c for c in ("avg_prep_sec", "avg_prep_time_sec", "avg_prep_time", "prep_time")
                     if c in prep.columns),
                    None,
                )
                if prep_col and "window_end" in prep.columns:
                    fig = px.line(
                        prep, x="window_end", y=prep_col,
                        color="zone_id" if "zone_id" in prep.columns else None,
                        markers=True, height=340,
                        color_discrete_sequence=PALETTE,
                    )
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(prep.tail(30), use_container_width=True)

        with col2:
            _section_header("Cancel / refund hotspots")
            if not error_banner(cancel, "cancel_refund_hotspots"):
                c_df = latest_window(cancel) if "window_end" in cancel.columns else cancel
                count_col = next(
                    (c for c in ("cancel_refund_count", "cancel_count", "count") if c in c_df.columns),
                    None,
                )
                if count_col and "zone_id" in c_df.columns:
                    c_df = c_df.groupby("zone_id", as_index=False, observed=True)[count_col].sum()
                    c_df = c_df.sort_values(count_col, ascending=False)
                    fig = px.bar(
                        c_df, x="zone_id", y=count_col, height=340,
                        color="zone_id", color_discrete_sequence=PALETTE,
                    )
                    fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(c_df.tail(20), use_container_width=True)

        st.divider()
        _section_header("Recent order anomalies", "Rows flagged by the streaming job.")
        if not error_banner(anom, "order_anomalies"):
            st.dataframe(anom.tail(25), use_container_width=True, height=300)

    # --- Revenue & value ---
    with tab_money:
        col1, col2 = st.columns(2)

        with col1:
            _section_header("Average order value by zone", "Latest window, ordered desc.")
            if not error_banner(aov, "aov_by_zone"):
                a = latest_window(aov)
                aov_col = next(
                    (c for c in ("avg_order_value", "aov", "average_order_value") if c in a.columns),
                    None,
                )
                if aov_col and "zone_id" in a.columns:
                    a = a.groupby("zone_id", as_index=False, observed=True)[aov_col].mean()
                    a = a.sort_values(aov_col, ascending=False)
                    fig = px.bar(
                        a, x="zone_id", y=aov_col, height=340,
                        color="zone_id", color_discrete_sequence=PALETTE,
                    )
                    fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(a.tail(20), use_container_width=True)

        with col2:
            _section_header("AOV trend by zone")
            if not error_banner(aov, "aov_by_zone"):
                aov_col = next(
                    (c for c in ("avg_order_value", "aov", "average_order_value") if c in aov.columns),
                    None,
                )
                if aov_col and "window_end" in aov.columns:
                    fig = px.line(
                        aov, x="window_end", y=aov_col,
                        color="zone_id" if "zone_id" in aov.columns else None,
                        markers=True, height=340,
                        color_discrete_sequence=PALETTE,
                    )
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(aov.tail(30), use_container_width=True)

        st.divider()
        _section_header(
            "Conversion funnel",
            "placed → accepted → prepared → picked-up → delivered (by zone).",
        )
        if not error_banner(funnel, "conversion_funnel_zone"):
            st.dataframe(funnel.tail(30), use_container_width=True, height=280)


# =============================================================================
# COURIERS  (6 KPIs)
# =============================================================================
elif section == "🛵 Couriers":

    avail  = load_kpi("available_couriers_zone")
    load   = load_kpi("active_delivery_load_zone")
    drop   = load_kpi("courier_dropoff_hotspots")
    idle   = load_kpi("courier_idle_time_by_vehicle")
    util   = load_kpi("courier_utilization")
    thr    = load_kpi("courier_throughput_hourly")

    tab_supply, tab_perf = st.tabs(
        ["🧍 Supply & load", "⚡ Performance"]
    )

    # --- Supply & load ---
    with tab_supply:
        col1, col2 = st.columns(2)

        with col1:
            _section_header("Available couriers by zone")
            if not error_banner(avail, "available_couriers_zone"):
                col = next(
                    (c for c in ("available_couriers", "couriers", "n_couriers") if c in avail.columns),
                    None,
                )
                if col and "window_end" in avail.columns:
                    fig = px.line(
                        avail, x="window_end", y=col, color="zone_id",
                        markers=True, height=340,
                        color_discrete_sequence=PALETTE,
                    )
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(avail.tail(30), use_container_width=True)

        with col2:
            _section_header("Active delivery load by zone", "Latest window.")
            if not error_banner(load, "active_delivery_load_zone"):
                l_df = latest_window(load)
                load_col = next(
                    (c for c in ("active_delivery_load", "active_deliveries", "deliveries", "load")
                     if c in l_df.columns),
                    None,
                )
                if load_col and "zone_id" in l_df.columns:
                    l_df = l_df.groupby("zone_id", as_index=False, observed=True)[load_col].sum()
                    l_df = l_df.sort_values(load_col, ascending=False)
                    fig = px.bar(
                        l_df, x="zone_id", y=load_col, height=340,
                        color="zone_id", color_discrete_sequence=PALETTE,
                    )
                    fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(l_df.tail(20), use_container_width=True)

        st.divider()
        _section_header("Courier drop-off hotspots", "Zones with the most drop-offs in the latest window.")
        if not error_banner(drop, "courier_dropoff_hotspots"):
            d_df = latest_window(drop) if "window_end" in drop.columns else drop
            drop_col = next(
                (c for c in ("courier_dropoff_count", "dropoff_count", "count")
                 if c in d_df.columns),
                None,
            )
            if drop_col and "zone_id" in d_df.columns:
                d_df = d_df.groupby("zone_id", as_index=False, observed=True)[drop_col].sum()
                d_df = d_df.sort_values(drop_col, ascending=False)
                fig = px.bar(
                    d_df, x="zone_id", y=drop_col, height=300,
                    color="zone_id", color_discrete_sequence=PALETTE,
                )
                fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.dataframe(drop.tail(30), use_container_width=True, height=300)

    # --- Performance ---
    with tab_perf:
        col1, col2 = st.columns(2)

        with col1:
            _section_header("Courier utilization trend")
            if not error_banner(util, "courier_utilization"):
                util_col = next(
                    (c for c in ("utilization", "utilization_pct", "utilization_rate") if c in util.columns),
                    None,
                )
                if util_col and "window_end" in util.columns:
                    fig = px.line(
                        util, x="window_end", y=util_col,
                        color="zone_id" if "zone_id" in util.columns else None,
                        markers=True, height=340,
                        color_discrete_sequence=PALETTE,
                    )
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(util.tail(30), use_container_width=True)

        with col2:
            _section_header("Idle time by vehicle type", "Avg idle (seconds) for the latest window.")
            if not error_banner(idle, "courier_idle_time_by_vehicle"):
                idle_col = next(
                    (c for c in ("idle_time_sec", "idle_sec", "avg_idle_sec", "idle_time")
                     if c in idle.columns),
                    None,
                )
                vt_col = "vehicle_type" if "vehicle_type" in idle.columns else None
                if idle_col and vt_col:
                    i_df = latest_window(idle) if "window_end" in idle.columns else idle
                    i_df = i_df.groupby(vt_col, as_index=False, observed=True)[idle_col].mean()
                    i_df = i_df.sort_values(idle_col, ascending=False)
                    fig = px.bar(
                        i_df, x=vt_col, y=idle_col, height=340,
                        color=vt_col, color_discrete_sequence=PALETTE,
                    )
                    fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(idle.tail(20), use_container_width=True)

        st.divider()
        _section_header("Hourly throughput", "Deliveries completed per hour, split by vehicle type.")
        if not error_banner(thr, "courier_throughput_hourly"):
            thr_col = next(
                (c for c in ("throughput", "deliveries", "n_deliveries", "delivered")
                 if c in thr.columns),
                None,
            )
            x_col = "hour" if "hour" in thr.columns else ("window_end" if "window_end" in thr.columns else None)
            color_col = next(
                (c for c in ("vehicle_type", "zone_id") if c in thr.columns),
                None,
            )
            if thr_col and x_col:
                fig = px.bar(
                    thr, x=x_col, y=thr_col, color=color_col,
                    height=340, color_discrete_sequence=PALETTE,
                    barmode="group",
                )
                fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.dataframe(thr.tail(30), use_container_width=True)


# =============================================================================
# CROSS-STREAM  (2 KPIs)
# =============================================================================
elif section == "🔀 Cross-stream":

    imbalance = load_kpi("supply_demand_imbalance")
    wait      = load_kpi("pickup_wait_per_order")

    tab_balance, tab_wait = st.tabs(
        ["⚖️ Supply vs demand", "⏱ Pickup-wait distribution"]
    )

    # --- Supply vs demand ---
    with tab_balance:
        if not error_banner(imbalance, "supply_demand_imbalance"):
            i_latest = latest_window(imbalance) if "window_end" in imbalance.columns else imbalance

            # Top-line ratio chart
            ratio_col = next(
                (c for c in ("supply_demand_ratio", "ratio") if c in i_latest.columns),
                None,
            )
            demand_col = next(
                (c for c in ("orders_demand", "demand", "orders") if c in i_latest.columns),
                None,
            )
            supply_col = next(
                (c for c in ("available_couriers", "supply", "couriers") if c in i_latest.columns),
                None,
            )

            col1, col2 = st.columns(2)

            with col1:
                _section_header(
                    "Supply / demand ratio by zone",
                    "Values < 1 mean the zone is under-supplied.",
                )
                if ratio_col and "zone_id" in i_latest.columns:
                    r_df = i_latest.sort_values(ratio_col)
                    fig = px.bar(
                        r_df, x="zone_id", y=ratio_col, height=340,
                        color=ratio_col,
                        color_continuous_scale="RdYlGn",
                    )
                    fig.add_hline(y=1, line_dash="dash", line_color="gray")
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), coloraxis_showscale=False)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.caption("No ratio column found — showing raw frame.")

            with col2:
                _section_header("Demand vs supply (grouped)")
                if demand_col and supply_col and "zone_id" in i_latest.columns:
                    fig = px.bar(
                        i_latest, x="zone_id", y=[demand_col, supply_col],
                        barmode="group", height=340,
                        color_discrete_sequence=PALETTE,
                    )
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10),
                                      legend_title_text="")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.caption("Need both demand and supply columns for the grouped view.")

            st.divider()
            _section_header("Imbalance table", "Latest computed window.")
            st.dataframe(i_latest, use_container_width=True, height=260)
        else:
            # Fallback: compute on the fly by merging demand + supply.
            st.caption(
                "Cross-stream output not available yet — falling back to client-side merge of "
                "`orders_demand_zone` and `available_couriers_zone`."
            )
            demand = load_kpi("orders_demand_zone")
            supply = load_kpi("available_couriers_zone")
            if not (demand.empty or "_error" in demand.columns or
                    supply.empty or "_error" in supply.columns):
                demand_col = next(
                    (c for c in ("orders_demand", "orders", "demand") if c in demand.columns),
                    None,
                )
                supply_col = next(
                    (c for c in ("available_couriers", "couriers", "n_couriers") if c in supply.columns),
                    None,
                )
                if (demand_col and supply_col and
                        "zone_id" in demand.columns and "zone_id" in supply.columns):
                    d = latest_window(demand).groupby(
                        "zone_id", as_index=False, observed=True
                    )[demand_col].sum()
                    s = latest_window(supply).groupby(
                        "zone_id", as_index=False, observed=True
                    )[supply_col].sum()
                    d["zone_id"] = d["zone_id"].astype("string")
                    s["zone_id"] = s["zone_id"].astype("string")
                    merged = d.merge(s, on="zone_id", how="outer")
                    merged = _fillna_numeric(merged, 0)
                    fig = px.bar(
                        merged, x="zone_id", y=[demand_col, supply_col],
                        barmode="group", height=360,
                        color_discrete_sequence=PALETTE,
                        title="Demand vs supply (latest window, client-side merge)",
                    )
                    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10),
                                      legend_title_text="")
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(merged, use_container_width=True)

    # --- Pickup-wait distribution ---
    with tab_wait:
        _section_header(
            "Pickup-wait distribution",
            "Seconds between READY_FOR_PICKUP and the courier pickup event.",
        )
        if not error_banner(wait, "pickup_wait_per_order"):
            wait_col = next(
                (c for c in ("pickup_wait_sec", "pickup_wait", "wait_sec") if c in wait.columns),
                None,
            )
            if wait_col:
                if "window_end" in wait.columns:
                    w_latest = latest_window(wait)
                else:
                    w_latest = wait.tail(500)

                col1, col2, col3 = st.columns(3)
                col1.metric("Median wait", f"{w_latest[wait_col].median():.0f}s")
                col2.metric("P90 wait",    f"{w_latest[wait_col].quantile(0.9):.0f}s")
                col3.metric("Max wait",    f"{w_latest[wait_col].max():.0f}s")

                fig = px.histogram(
                    w_latest, x=wait_col, nbins=30,
                    color="zone_id" if "zone_id" in w_latest.columns else None,
                    height=360,
                    color_discrete_sequence=PALETTE,
                )
                fig.update_layout(margin=dict(l=10, r=10, t=10, b=10),
                                  legend_title_text="zone")
                st.plotly_chart(fig, use_container_width=True)

                st.divider()
                _section_header("Recent orders", "Most recent pickup-wait rows.")
                st.dataframe(w_latest.tail(30), use_container_width=True, height=260)
            else:
                st.dataframe(wait.tail(30), use_container_width=True)


st.sidebar.markdown("---")
st.sidebar.caption("Built for Streaming Analytics class demo · 2026")
