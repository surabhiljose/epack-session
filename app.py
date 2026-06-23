"""
ePack Session — live battery-session board (single-file Streamlit app).

Standalone project. Queries Databricks server-side (no CORS), auto-refreshes,
highlights the live session in green. Credentials are hardcoded (throwaway token).

Run:
    pip install streamlit
    streamlit run app.py
"""
import os
import json
import time
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import streamlit as st

# ----------------------------------------------------------------------------- config
st.set_page_config(page_title="ePack Session", page_icon="⚡", layout="wide")

# Host + warehouse id are not secret. The PAT is read from Streamlit Secrets (hosted)
# or an env var (local/Docker) — never committed to the repo.
HOST = "adb-7405606878126025.5.azuredatabricks.net"
WAREHOUSE_ID = "f9d24f1f426c649f"


def _token():
    try:
        if "DATABRICKS_TOKEN" in st.secrets:
            return st.secrets["DATABRICKS_TOKEN"]
    except Exception:
        pass
    return os.environ.get("DATABRICKS_TOKEN")


TOKEN = _token()
if not TOKEN:
    st.error(
        "**DATABRICKS_TOKEN** is not set. On Streamlit Cloud, open the app's "
        "**Settings → Secrets** and add:\n\n```toml\nDATABRICKS_TOKEN = \"dapi…\"\n```\n\n"
        "Locally, run with `DATABRICKS_TOKEN=dapi… streamlit run app.py`."
    )
    st.stop()

TABLE = "main.default.epackmaverickv1_can_parsed_2026_04_23"
IST = ZoneInfo("Asia/Kolkata")
REFRESH_EVERY = 8  # seconds

SQL_TEMPLATE = """
with src as (
    select * from {table}
    where device_id = {device} and source_date > '{since}' and bms_state between 1 and 3
),
sessions as (
    select
        case when bms_state = 1 then 'charging' when bms_state = 2 then 'discharging' when bms_state = 3 then 'standby' end as battery_status,
        device_id,
        try_cast(max(bms_junction_box_unique_key) as bigint) as epack_id,
        min_by(bms_actual_soc, source_timestamp) filter (where bms_actual_soc is not null) as soc_start,
        max_by(bms_actual_soc, source_timestamp) filter (where bms_actual_soc is not null) as soc_end,
        min(source_timestamp) as session_started_at,
        max(source_timestamp) as session_ended_at,
        min_by(odo_read, source_timestamp) filter (where odo_read != 0) as odo_read_start,
        max_by(odo_read, source_timestamp) filter (where odo_read != 0) as odo_read_end,
        (max_by(b2v_totdischgenergy, source_timestamp) filter (where b2v_totdischgenergy is not null)
         - min_by(b2v_totdischgenergy, source_timestamp) filter (where b2v_totdischgenergy is not null)) / 1000 as energy_throughput_kwh,
        case when min_by(odo_read, source_timestamp) filter (where odo_read != 0) is not null
              and max_by(odo_read, source_timestamp) filter (where odo_read != 0) is not null
              and min_by(odo_read, source_timestamp) filter (where odo_read != 0) != max_by(odo_read, source_timestamp) filter (where odo_read != 0)
            then ((max_by(b2v_totdischgenergy, source_timestamp) filter (where b2v_totdischgenergy is not null)
                   - min_by(b2v_totdischgenergy, source_timestamp) filter (where b2v_totdischgenergy is not null)) / 1000)
                 / (max_by(odo_read, source_timestamp) filter (where odo_read != 0) - min_by(odo_read, source_timestamp) filter (where odo_read != 0)) end as kwh_per_km,
        unix_timestamp(max(source_timestamp)) - unix_timestamp(min(source_timestamp)) as session_duration_seconds
    from src
    group by bms_state, device_id, iot_project, session_window(source_timestamp, '4 minutes')
)
select (s.session_ended_at = m.max_end) as is_live, s.battery_status, s.device_id, s.epack_id,
       s.soc_start, s.soc_end, s.session_started_at, s.session_ended_at,
       s.odo_read_start, s.odo_read_end, s.energy_throughput_kwh, s.kwh_per_km
from sessions s cross join (select max(session_ended_at) as max_end from sessions) m
where s.session_duration_seconds >= 240 or s.session_ended_at = m.max_end
order by s.session_started_at
"""


# ----------------------------------------------------------------------------- query (stdlib only)
def _post_json(url, headers, payload=None, method="GET"):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def run_query(device, since):
    sql = SQL_TEMPLATE.format(table=TABLE, device=device, since=since)
    base = f"https://{HOST}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    data = _post_json(base + "/", headers, {
        "warehouse_id": WAREHOUSE_ID, "statement": sql,
        "format": "JSON_ARRAY", "disposition": "INLINE",
        "wait_timeout": "30s", "on_wait_timeout": "CONTINUE",
    }, method="POST")
    sid = data.get("statement_id")
    t0 = time.time()
    while data.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if time.time() - t0 > 120:
            raise TimeoutError("Databricks query timed out")
        time.sleep(1.2)
        data = _post_json(f"{base}/{sid}", headers, method="GET")
    state = data.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(data.get("status", {}).get("error", {}).get("message", state))
    cols = [c["name"] for c in data["manifest"]["schema"]["columns"]]
    rows = data.get("result", {}).get("data_array", []) or []
    return [dict(zip(cols, r)) for r in rows]


@st.cache_data(ttl=REFRESH_EVERY - 1, show_spinner=False)
def cached_query(device, since):
    return run_query(device, since)


# ----------------------------------------------------------------------------- helpers
def num(v):
    try:
        return None if v in (None, "") else float(v)
    except (TypeError, ValueError):
        return None


def t_ist(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(IST)


def fmt_time(iso):
    return t_ist(iso).strftime("%H:%M")


def fmt_dur(a, b):
    s = max(0, int((t_ist(b) - t_ist(a)).total_seconds()))
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m = s // 60
    if d:
        return f"{d}d {h}h {m}m"
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


# ----------------------------------------------------------------------------- styles
st.markdown("""
<style>
  #MainMenu, header[data-testid="stHeader"], footer { display: none; }
  .block-container { padding: 1.4rem 2rem 2rem; max-width: 1280px; }
  .stApp { background: #ffffff; }
  /* Keep content crisp during the background refresh — no blur/fade on stale elements */
  [data-stale="true"] { opacity: 1 !important; transition: none !important; filter: none !important; }
  .stApp [data-testid="stStatusWidget"] { display: none !important; }
  [data-testid="stSpinner"] { display: none !important; }
  :root {
    --ink:#16181d; --muted:#6b7280; --faint:#9aa1ad; --line:#ededf1; --line-2:#f4f5f7;
    --yellow:#f5c518; --yellow-dk:#c79700; --yellow-sf:#fff7d6;
    --green:#15a34a; --green-dk:#0f7a37; --green-sf:#ecfdf3;
  }
  .ep-head { display:flex; align-items:center; gap:15px; margin-bottom:20px; font-family:"Inter",-apple-system,"Segoe UI",sans-serif; }
  .ep-bolt { width:44px; height:44px; border-radius:13px; display:grid; place-items:center; background:var(--yellow); color:#181a1f; font-size:23px; box-shadow:0 6px 18px -8px rgba(245,197,24,.85); }
  .ep-title { font-size:23px; font-weight:750; letter-spacing:-.4px; color:var(--ink); }
  .ep-title .pk { color:var(--yellow-dk); }
  .ep-sub { font-size:13px; color:var(--muted); margin-top:2px; }
  .ep-stat { margin-left:auto; display:flex; align-items:center; gap:8px; font-size:12.5px; color:var(--muted); background:#fff; border:1px solid var(--line); padding:10px 14px; border-radius:999px; white-space:nowrap; }
  .ep-stat .d { width:8px; height:8px; border-radius:50%; background:var(--green); }

  .board { background:#fff; border:1px solid var(--line); border-radius:20px; box-shadow:0 1px 2px rgba(16,18,25,.04),0 10px 30px -18px rgba(16,18,25,.22); overflow:hidden; font-family:"Inter",-apple-system,"Segoe UI",sans-serif; }
  .board-head { display:flex; align-items:center; gap:11px; padding:16px 22px; border-bottom:1px solid var(--line-2); }
  .board-head .accent { width:28px; height:4px; border-radius:2px; background:var(--yellow); }
  .board-head h2 { margin:0; font-size:14.5px; font-weight:750; color:var(--ink); }
  .board-head .count { font-size:12px; color:var(--faint); }
  .fsbtn { margin-left:auto; text-decoration:none; font-size:12.5px; font-weight:600; color:var(--ink); background:#fff; border:1px solid var(--line); border-radius:9px; padding:6px 12px; white-space:nowrap; }
  .fsbtn:hover { border-color:var(--yellow); background:var(--yellow-sf); }

  .sess { display:flex; align-items:center; padding:18px 22px; border-bottom:1px solid var(--line-2); }
  .sess:last-child { border-bottom:none; }
  .ident { display:flex; align-items:center; gap:13px; min-width:150px; }
  .stcol { width:5px; align-self:stretch; min-height:38px; border-radius:3px; background:var(--faint); }
  .sess.charging .stcol { background:#f59e0b; }
  .sess.discharging .stcol { background:#ef7234; }
  .sess.standby .stcol { background:#b6bcc7; }
  .id-main { display:flex; flex-direction:column; gap:6px; }
  .id-top { display:flex; align-items:center; gap:8px; }
  .pill { font-size:11.5px; font-weight:700; text-transform:capitalize; padding:3px 10px; border-radius:7px; background:#f1f2f4; color:#4b5563; width:fit-content; }
  .pill.charging { background:#fef3da; color:#b45309; }
  .pill.discharging { background:#fde7dc; color:#c2410c; }
  .pill.standby { background:#eef0f3; color:#5b6472; }
  .live-chip { display:inline-flex; align-items:center; gap:6px; font-size:11px; font-weight:800; letter-spacing:.8px; text-transform:uppercase; color:#fff; background:var(--green); border-radius:7px; padding:3px 9px; }
  .live-chip .pulse { width:6px; height:6px; border-radius:50%; background:#fff; animation:ping 1.3s infinite; }
  .epk { font-size:14px; font-weight:700; letter-spacing:-.2px; color:var(--ink); }
  .epk .dev { color:var(--faint); font-weight:500; font-size:12px; margin-left:7px; }

  .metrics { display:flex; align-items:center; flex:1; }
  .seg { flex:1; padding:0 22px; border-left:1px solid var(--line-2); min-width:90px; }
  .seg:first-child { border-left:none; }
  .seg .lbl { font-size:9.5px; text-transform:uppercase; letter-spacing:.6px; color:var(--faint); margin-bottom:5px; }
  .seg .val { font-size:15px; font-weight:700; letter-spacing:-.3px; font-variant-numeric:tabular-nums; white-space:nowrap; color:var(--ink); }
  .seg .val .u { font-size:11px; color:var(--muted); font-weight:500; margin-left:2px; }
  .seg .val .arr { color:var(--faint); margin:0 5px; font-weight:500; }
  .delta { font-size:11px; font-weight:600; margin-top:3px; }
  .delta.up { color:var(--green); } .delta.down { color:#ef7234; }
  .faint { color:var(--faint); }

  .sess.live { background:linear-gradient(90deg,var(--green-sf),#fff 72%); box-shadow:inset 5px 0 0 var(--green); }
  .sess.live .stcol { display:none; }
  .sess.live .ident { padding-left:5px; }
  .live-pill { display:inline-flex; align-items:center; gap:6px; width:fit-content; font-size:11px; font-weight:800; letter-spacing:.9px; text-transform:uppercase; color:#fff; background:var(--green); border-radius:7px; padding:4px 11px; }
  .live-pill .pulse { width:7px; height:7px; border-radius:50%; background:#fff; animation:ping 1.3s infinite; }
  @keyframes ping { 0%{box-shadow:0 0 0 0 rgba(255,255,255,.9);} 70%{box-shadow:0 0 0 6px rgba(255,255,255,0);} 100%{box-shadow:0 0 0 0 rgba(255,255,255,0);} }
  .sess.live .seg .val { color:var(--green-dk); }
  .sess.live .seg .val .u { color:#5b9c74; }
</style>
""", unsafe_allow_html=True)


def session_html(r):
    st_ = r["battery_status"]
    live = str(r["is_live"]).lower() == "true"
    socS, socE = num(r["soc_start"]), num(r["soc_end"])
    dsoc = (socE - socS) if (socS is not None and socE is not None) else None
    energy = num(r["energy_throughput_kwh"])
    odoS, odoE = num(r["odo_read_start"]), num(r["odo_read_end"])
    dist = max(0.0, odoE - odoS) if (odoS is not None and odoE is not None) else None
    kpk = num(r["kwh_per_km"])
    fx = lambda v, dp: f"{v:.{dp}f}" if v is not None else "—"
    kpk_str = f"{kpk:.2f}" if kpk is not None else '<span class="faint">—</span>'
    # always show the status (charging/discharging/standby); live row also gets a green LIVE chip
    status_pill = f'<span class="pill {st_}">{st_}</span>'
    live_chip = '<span class="live-chip"><span class="pulse"></span>Live</span>' if live else ''
    top = f'<div class="id-top">{status_pill}{live_chip}</div>'
    delta = (f'<div class="delta {"up" if dsoc >= 0 else "down"}">{"▲" if dsoc >= 0 else "▼"} {abs(dsoc):.1f}%</div>'
             if dsoc is not None else "")
    return f"""
      <div class="sess {st_}{' live' if live else ''}">
        <div class="ident">
          <div class="stcol"></div>
          <div class="id-main">{top}</div>
        </div>
        <div class="metrics">
          <div class="seg"><div class="lbl">Time</div><div class="val">{fmt_time(r['session_started_at'])}<span class="arr">→</span>{fmt_time(r['session_ended_at'])}</div></div>
          <div class="seg"><div class="lbl">Duration</div><div class="val">{fmt_dur(r['session_started_at'], r['session_ended_at'])}</div></div>
          <div class="seg"><div class="lbl">SoC start → end</div><div class="val">{fx(socS,1)}<span class="arr">→</span>{fx(socE,1)}<span class="u">%</span></div>{delta}</div>
          <div class="seg"><div class="lbl">KMs travelled</div><div class="val">{fx(dist,1)}<span class="u">km</span></div></div>
          <div class="seg"><div class="lbl">Energy</div><div class="val">{fx(energy,2)}<span class="u">kWh</span></div></div>
          <div class="seg"><div class="lbl">kWh / km</div><div class="val">{kpk_str}</div></div>
        </div>
      </div>"""


# ----------------------------------------------------------------------------- controls
c1, c2, _ = st.columns([1, 1, 6])
device = c1.text_input("Device ID", value="11")
since = c2.text_input("Since", value="2026-06-21")

# Fullscreen = the sessions board only, as a full-page overlay (toggled via ?fs=1).
FULLSCREEN = st.query_params.get("fs") == "1"
if FULLSCREEN:
    st.markdown("""
    <style>
      .ep-head, div[data-testid="stHorizontalBlock"] { display: none !important; }
      .block-container { padding: 0 !important; max-width: 100% !important; }
      .board { position: fixed; inset: 0; z-index: 99999; border: none; border-radius: 0;
               height: 100vh; overflow-y: auto; box-shadow: none; }
      .board-head { position: sticky; top: 0; background: #fff; z-index: 5; }
    </style>
    """, unsafe_allow_html=True)


# ----------------------------------------------------------------------------- board (auto-refresh)
@st.fragment(run_every=REFRESH_EVERY)
def board():
    try:
        rows = cached_query(int(device), since.strip())
        st.session_state["rows"] = rows
        st.session_state["ts"] = datetime.now(timezone.utc)
    except Exception:
        pass  # keep showing last good data on a transient hiccup

    rows = st.session_state.get("rows", [])
    ts = st.session_state.get("ts")
    dev = rows[0]["device_id"] if rows else device
    when = ""
    if ts:
        secs = int((datetime.now(timezone.utc) - ts).total_seconds())
        ago = f"{secs}s ago" if secs < 60 else f"{secs // 60}m ago"
        when = f"live · updated {ago}"

    st.markdown(f"""
      <div class="ep-head">
        <div class="ep-bolt">⚡</div>
        <div>
          <div class="ep-title">e<span class="pk">Pack</span> Session</div>
          <div class="ep-sub">Live battery sessions for device <b>{dev}</b> · newest first</div>
        </div>
        <div class="ep-stat"><span class="d"></span>{when or 'connecting…'}</div>
      </div>
    """, unsafe_allow_html=True)

    if not rows:
        st.markdown('<div class="board"><div class="sess">Querying Databricks… first query can take up to ~90s while the warehouse wakes.</div></div>', unsafe_allow_html=True)
        return

    ordered = list(reversed(rows))  # live / newest first
    cards = "".join(session_html(r) for r in ordered)
    fs_now = st.query_params.get("fs") == "1"
    fs_link = ('<a class="fsbtn" href="?fs=0" target="_self">✕ Exit fullscreen</a>' if fs_now
               else '<a class="fsbtn" href="?fs=1" target="_self">⛶ Fullscreen</a>')
    st.markdown(f"""
      <div class="board">
        <div class="board-head"><span class="accent"></span><h2>Sessions</h2><span class="count">{len(rows)} total</span>{fs_link}</div>
        {cards}
      </div>
    """, unsafe_allow_html=True)


board()
st.caption("Tip: press F11 (Windows) or ⌃⌘F (Mac) for fullscreen.")
