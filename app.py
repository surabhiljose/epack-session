"""
Maverick Session — live discharging-trip board (single-file Streamlit app).

Standalone project. Queries Databricks server-side (no CORS), auto-refreshes.
Shows DISCHARGING (riding) sessions only: the latest/live trip as a hero card
with a route map, and the rest as a past-trips table. Map appears on the live
card only.

Run:
    pip install streamlit
    DATABRICKS_TOKEN=dapi... streamlit run app.py
"""
import os
import json
import time
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import streamlit as st

# ----------------------------------------------------------------------------- config
st.set_page_config(page_title="Maverick Session", page_icon="⚡", layout="wide")

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
GPS_TABLE = "main.default.epackmaverickv1_c2c_gps_2026_06_23"
IST = ZoneInfo("Asia/Kolkata")
REFRESH_EVERY = 8  # seconds

# Per-minute GPS track for the device over the range (sliced per session in Python).
GPS_SQL = """
select date_trunc('minute', source_timestamp) as minute,
       avg(latitude) as lat, avg(longitude) as lon, max(speed) as spd
from {gtable}
where device_id = {device} and source_date >= '{since}'
  and latitude between 5 and 40 and longitude between 60 and 100
group by 1 order by 1
"""

SQL_TEMPLATE = """
with src as (
    select * from {table}
    where device_id = {device} and source_date >= '{since}' and bms_state between 1 and 3
),
-- sessionize by time-ordered STATE CHANGES so sessions are sequential & never overlap:
-- a new session starts whenever the state changes or there's a >1-min gap.
ordered as (
    select *,
        lag(bms_state) over (partition by device_id order by source_timestamp) as prev_state,
        lag(source_timestamp) over (partition by device_id order by source_timestamp) as prev_ts
    from src
),
marked as (
    select *,
        case when prev_state is null or bms_state != prev_state
                  or unix_timestamp(source_timestamp) - unix_timestamp(prev_ts) > 60
             then 1 else 0 end as is_new
    from ordered
),
grp as (
    select *,
        sum(is_new) over (partition by device_id order by source_timestamp
                          rows between unbounded preceding and current row) as sid
    from marked
),
sessions as (
    select
        case when bms_state = 1 then 'charging' when bms_state = 2 then 'discharging' when bms_state = 3 then 'standby' end as battery_status,
        device_id,
        try_cast(max(bms_junction_box_unique_key) as bigint) as epack_id,
        min_by(bms_user_soc, source_timestamp) filter (where bms_user_soc is not null) as soc_start,
        max_by(bms_user_soc, source_timestamp) filter (where bms_user_soc is not null) as soc_end,
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
    from grp
    group by sid, bms_state, device_id
)
select (s.session_ended_at = m.max_end) as is_live, s.battery_status, s.device_id, s.epack_id,
       s.soc_start, s.soc_end, s.session_started_at, s.session_ended_at,
       s.odo_read_start, s.odo_read_end, s.energy_throughput_kwh, s.kwh_per_km
from sessions s cross join (select max(session_ended_at) as max_end from sessions) m
where s.session_duration_seconds >= 60 or s.session_ended_at = m.max_end
order by s.session_ended_at
"""


# ----------------------------------------------------------------------------- query (stdlib only)
def _post_json(url, headers, payload=None, method="GET"):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def _exec(sql):
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
    return _exec(SQL_TEMPLATE.format(table=TABLE, device=device, since=since))


@st.cache_data(ttl=REFRESH_EVERY - 1, show_spinner=False)
def cached_gps(device, since):
    return _exec(GPS_SQL.format(gtable=GPS_TABLE, device=device, since=since))


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


def fmt_date(iso):
    return t_ist(iso).strftime("%b %d · %H:%M")


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


def gps_track(gps_rows, start_iso, end_iso):
    """Per-minute lat/lon points within [start, end] -> list of [lat, lon]."""
    a, b = t_ist(start_iso), t_ist(end_iso)
    pts = []
    for g in gps_rows:
        m = g.get("minute")
        lat, lon = num(g.get("lat")), num(g.get("lon"))
        if m is None or lat is None or lon is None:
            continue
        mt = t_ist(m)
        if a <= mt <= b:
            pts.append([round(lat, 6), round(lon, 6)])
    return pts


# ----------------------------------------------------------------------------- live hero card (self-contained iframe w/ Leaflet)
LIVE_CARD = """
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bai+Jamjuree:wght@300;400;500;600;700&display=swap');
  *{box-sizing:border-box;margin:0;padding:0;font-family:'Bai Jamjuree',-apple-system,'Segoe UI',sans-serif;}
  :root{--surface:#fff;--surface-2:#F7F7F7;--border:#E8E8E8;--border-mid:#CFCFCF;
        --t1:#212121;--t2:#666;--t3:#ADADAD;--amber:#E8960E;--amber-mid:#FFB000;--ground:#F2F2F2;}
  body{background:transparent;}
  .live-card{background:var(--surface);border:.5px solid var(--border);border-radius:16px;padding:22px;
             position:relative;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.05);}
  .live-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;
             background:linear-gradient(90deg,var(--amber) 0%,var(--amber-mid) 50%,transparent 100%);opacity:.7;}
  .top{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:18px;}
  .eyebrow{font-size:10px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--t3);margin-bottom:4px;}
  .tid{font-size:16px;font-weight:700;color:var(--t1);letter-spacing:-.02em;}
  .tmeta{font-size:12px;color:var(--t2);margin-top:3px;}
  .badge{background:var(--surface-2);color:var(--t2);border:.5px solid var(--border);border-radius:20px;
         padding:4px 12px;font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;white-space:nowrap;}
  .soc-row{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;align-items:end;margin-bottom:12px;}
  .soc-block{text-align:center;}
  .soc-lbl{font-size:10px;font-weight:700;color:var(--t3);letter-spacing:.10em;text-transform:uppercase;margin-bottom:6px;}
  .soc-num{font-size:52px;font-weight:700;line-height:1;letter-spacing:-.04em;color:var(--t1);font-variant-numeric:tabular-nums;}
  .soc-num.cur{color:var(--amber);}
  .soc-unit{font-size:20px;font-weight:300;color:var(--t3);margin-left:1px;}
  .soc-arrow{font-size:26px;color:var(--border-mid);padding-bottom:10px;justify-self:center;}
  .soc-consumed{display:block;width:fit-content;margin:0 auto 20px;font-size:11px;font-weight:600;color:var(--t2);
           background:var(--surface-2);border:.5px solid var(--border);border-radius:20px;padding:4px 12px;}
  .bar-wrap{margin-bottom:20px;}
  .bar-track{height:8px;background:var(--ground);border:.5px solid var(--border);border-radius:4px;position:relative;}
  .bar-fill{height:100%;background:linear-gradient(90deg,#E8960E 0%,#FFB000 60%,#FFD985 100%);border-radius:4px;
            box-shadow:0 0 10px rgba(255,176,0,.4);position:relative;}
  .bar-fill::after{content:'';position:absolute;right:-5px;top:50%;transform:translateY(-50%);width:10px;height:10px;
            border-radius:50%;background:#FFB000;border:2px solid #fff;box-shadow:0 0 8px rgba(255,176,0,.6);}
  .bar-lbls{display:flex;justify-content:space-between;margin-top:6px;}
  .bar-lbls span{font-size:10px;color:var(--t3);}
  .grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:18px;}
  .mcard{background:var(--surface-2);border:.5px solid var(--border);border-radius:12px;padding:15px;position:relative;overflow:hidden;}
  .mcard::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--amber) 0%,transparent 70%);opacity:.5;}
  .m-lbl{font-size:10px;font-weight:700;color:var(--t3);letter-spacing:.08em;text-transform:uppercase;margin-bottom:7px;}
  .m-val{font-size:26px;font-weight:700;line-height:1;letter-spacing:-.025em;color:var(--amber);font-variant-numeric:tabular-nums;}
  .m-unit{font-size:12px;font-weight:400;color:var(--t2);margin-left:2px;}
  #map{height:200px;border-radius:12px;overflow:hidden;border:.5px solid var(--border);background:#eef0f3;}
  .leaflet-container{background:#eef0f3;}
  .nomap{height:200px;border-radius:12px;border:.5px dashed var(--border);background:var(--surface-2);
         display:flex;align-items:center;justify-content:center;color:var(--t3);font-size:12px;}
</style>
<div class="live-card">
  <div class="top">
    <div>
      <div class="eyebrow">__EYEBROW__</div>
      <div class="tid">__TID__</div>
      <div class="tmeta">__META__</div>
    </div>
    <div class="badge">&#9660;&nbsp;Discharging</div>
  </div>
  <div class="soc-row">
    <div class="soc-block left"><div class="soc-lbl">SOC start</div><div class="soc-num">__SOC_S__<span class="soc-unit">%</span></div></div>
    <div class="soc-arrow">&rarr;</div>
    <div class="soc-block right"><div class="soc-lbl">SOC now</div><div class="soc-num cur">__SOC_E__<span class="soc-unit">%</span></div></div>
  </div>
  <div class="soc-consumed">&#9660; __DELTA__% consumed</div>
  <div class="bar-wrap">
    <div class="bar-track"><div class="bar-fill" style="width:__BARW__%"></div></div>
    <div class="bar-lbls"><span>0%</span><span>State of Charge</span><span>100%</span></div>
  </div>
  <div class="grid">
    <div class="mcard"><div class="m-lbl">Distance</div><div class="m-val">__DIST__<span class="m-unit">km</span></div></div>
    <div class="mcard"><div class="m-lbl">Energy consumed</div><div class="m-val">__ENERGY__<span class="m-unit">kWh</span></div></div>
    <div class="mcard"><div class="m-lbl">Efficiency</div><div class="m-val">__EFF__<span class="m-unit">kWh/km</span></div></div>
  </div>
  __MAP__
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
(function(){
  var PTS = __PTS__;
  if (!PTS.length || !document.getElementById('map')) return;
  function init(){
    var map = L.map('map', {zoomControl:true, attributionControl:true});
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
      {maxZoom:20, attribution:'&copy; OpenStreetMap &copy; CARTO'}).addTo(map);
    if (PTS.length > 1) L.polyline(PTS, {color:'#E8960E', weight:4, opacity:.95}).addTo(map);
    L.circleMarker(PTS[0], {radius:4, color:'#7C5A12', fillColor:'#7C5A12', fillOpacity:1, weight:1}).addTo(map);
    var last = PTS[PTS.length-1];
    L.circleMarker(last, {radius:7, color:'#09101F', weight:2, fillColor:'#88CCAE', fillOpacity:1}).addTo(map);
    function fit(){ if (PTS.length===1){ map.setView(PTS[0],15);} else { map.fitBounds(PTS,{padding:[26,26],maxZoom:16}); } }
    fit(); setTimeout(function(){ map.invalidateSize(); fit(); }, 200);
  }
  if (window.L) init(); else {
    var t=setInterval(function(){ if(window.L){clearInterval(t); init();} }, 60);
  }
})();
</script>
"""


def render_live_card(r, gps, live):
    socS, socE = num(r["soc_start"]), num(r["soc_end"])
    delta = (socS - socE) if (socS is not None and socE is not None) else 0.0
    energy = num(r["energy_throughput_kwh"])
    odoS, odoE = num(r["odo_read_start"]), num(r["odo_read_end"])
    dist = max(0.0, odoE - odoS) if (odoS is not None and odoE is not None) else None
    kpk = num(r["kwh_per_km"])
    ended = r["session_ended_at"] or (datetime.now(timezone.utc).isoformat() if live else r["session_ended_at"])
    pts = gps_track(gps, r["session_started_at"], ended) if gps else []

    meta = (f"{fmt_time(r['session_started_at'])} &rarr; "
            f"{'<b style=color:#2a7a5a>now</b>' if live else fmt_time(ended)}"
            f" &nbsp;&middot;&nbsp; {fmt_dur(r['session_started_at'], ended)} {'elapsed' if live else ''}")
    # The hero (live card) always shows its route map.
    map_html = '<div id="map"></div>' if pts else '<div class="nomap">No GPS track for this trip</div>'
    repl = {
        "__EYEBROW__": "Current trip" if live else "Latest trip",
        "__TID__": f"ePack #{r['epack_id']}" if r.get("epack_id") else f"Device {r['device_id']}",
        "__META__": meta,
        "__SOC_S__": f"{socS:.0f}" if socS is not None else "—",
        "__SOC_E__": f"{socE:.1f}" if socE is not None else "—",
        "__DELTA__": f"{abs(delta):.1f}",
        "__BARW__": f"{socE:.1f}" if socE is not None else "0",
        "__DIST__": f"{dist:.1f}" if dist is not None else "—",
        "__ENERGY__": f"{energy:.2f}" if energy is not None else "—",
        "__EFF__": f"{kpk:.2f}" if kpk is not None else "—",
        "__MAP__": map_html,
        "__PTS__": json.dumps(pts),
    }
    html = LIVE_CARD
    for k, v in repl.items():
        html = html.replace(k, v)
    st.components.v1.html(html, height=660)


PAST_TABLE = """
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bai+Jamjuree:wght@300;400;500;600;700&display=swap');
  *{box-sizing:border-box;margin:0;padding:0;font-family:'Bai Jamjuree',-apple-system,'Segoe UI',sans-serif;}
  body{background:transparent;}
  .pcard{background:#fff;border:.5px solid #E8E8E8;border-radius:16px;box-shadow:0 1px 4px rgba(0,0,0,.05);padding:2px 22px;}
  table{width:100%;border-collapse:collapse;table-layout:fixed;}
  th{font-size:10px;font-weight:700;color:#ADADAD;letter-spacing:.08em;text-transform:uppercase;
     padding:16px 0 11px;border-bottom:.5px solid #E8E8E8;text-align:right;white-space:nowrap;}
  td{font-size:12.5px;color:#212121;padding:13px 0;border-bottom:.5px solid #F1F1F1;text-align:right;
     font-variant-numeric:tabular-nums;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
  th.l,td.l{text-align:left;}
  tbody tr:last-child td{border-bottom:none;}
  tbody tr:hover td{background:rgba(255,176,0,.06);}
  .tc{color:#666;} .dur{font-weight:600;} .sf{color:#666;}
  .sa{color:#CFCFCF;margin:0 4px;} .u{color:#ADADAD;font-size:10px;} .eff{color:#CC8800;font-weight:700;}
  .muted{color:#CFCFCF;}
</style>
<div class="pcard">
  <table>
    <colgroup><col style="width:25%"><col style="width:13%"><col style="width:20%">
      <col style="width:15%"><col style="width:15%"><col style="width:12%"></colgroup>
    <thead><tr><th class="l">Time</th><th>Duration</th><th>SOC</th><th>Distance</th><th>Energy</th><th>kWh/km</th></tr></thead>
    <tbody>__ROWS__</tbody>
  </table>
</div>
"""


def render_past_trips(rows):
    out = []
    for r in rows:
        socS, socE = num(r["soc_start"]), num(r["soc_end"])
        energy = num(r["energy_throughput_kwh"])
        odoS, odoE = num(r["odo_read_start"]), num(r["odo_read_end"])
        dist = max(0.0, odoE - odoS) if (odoS is not None and odoE is not None) else None
        kpk = num(r["kwh_per_km"])
        soc = (f"{socS:.0f}<span class='sa'>&rarr;</span>{socE:.0f}%"
               if (socS is not None and socE is not None) else "<span class='muted'>—</span>")
        dist_c = f"{round(dist, 1):g}<span class='u'> km</span>" if dist is not None else "<span class='muted'>—</span>"
        en_c = f"{energy:.1f}<span class='u'> kWh</span>" if energy is not None else "<span class='muted'>—</span>"
        eff_c = f"<span class='eff'>{kpk:.2f}</span>" if kpk is not None else "<span class='muted'>—</span>"
        out.append(
            f"<tr><td class='l tc'>{fmt_date(r['session_started_at'])}</td>"
            f"<td class='dur'>{fmt_dur(r['session_started_at'], r['session_ended_at'])}</td>"
            f"<td class='sf'>{soc}</td><td>{dist_c}</td><td>{en_c}</td><td>{eff_c}</td></tr>"
        )
    html = PAST_TABLE.replace("__ROWS__", "".join(out))
    st.components.v1.html(html, height=66 + len(rows) * 44)


# ----------------------------------------------------------------------------- page styles
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bai+Jamjuree:wght@300;400;500;600;700&display=swap');
  #MainMenu, header[data-testid="stHeader"], footer { display:none; }
  [data-testid="stStatusWidget"], [data-testid="stSpinner"] { display:none !important; }
  [data-stale="true"] { opacity:1 !important; transition:none !important; filter:none !important; }
  .stApp { background:#F2F2F2; }
  .block-container { padding:1.6rem 2rem 3rem; max-width:880px; font-family:'Bai Jamjuree',-apple-system,'Segoe UI',sans-serif; }
  :root{--surface:#fff;--surface-2:#F7F7F7;--border:#E8E8E8;--border-mid:#CFCFCF;
        --t1:#212121;--t2:#666;--t3:#ADADAD;--brand:#FFB000;--amber:#CC8800;--amber-dim:rgba(255,176,0,.07);--mint:#88CCAE;--mint-shade:#2a7a5a;}
  .mv-head{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:6px;
           font-family:'Bai Jamjuree',sans-serif;}
  .mv-left{display:flex;align-items:center;gap:10px;}
  .mv-logo{width:34px;height:34px;background:var(--brand);border-radius:8px;display:flex;align-items:center;justify-content:center;
           color:#fff;font-size:17px;box-shadow:0 2px 8px rgba(255,176,0,.35);}
  .mv-title{font-size:15px;font-weight:700;color:var(--t1);letter-spacing:-.02em;}
  .mv-dev{font-size:12px;font-weight:600;color:var(--t1);background:#fff;border:.5px solid var(--border);border-radius:8px;padding:4px 10px;}
  .mv-right{display:flex;align-items:center;gap:10px;}
  .mv-upd{font-size:11px;color:var(--t3);font-variant-numeric:tabular-nums;}
  .mv-upd.fresh{color:var(--mint-shade);}
  .mv-live{display:flex;align-items:center;gap:6px;background:rgba(136,204,174,.18);border:.5px solid var(--mint);
           border-radius:20px;padding:4px 11px;font-size:10px;font-weight:700;color:#2a7a5a;letter-spacing:.10em;text-transform:uppercase;}
  .mv-dot{width:7px;height:7px;border-radius:50%;background:var(--mint);animation:lp 2.2s infinite;}
  @keyframes lp{0%,100%{opacity:1}50%{opacity:.35}}
  .mv-fs{text-decoration:none;font-size:12px;font-weight:600;color:var(--t2);background:#fff;border:.5px solid var(--border);
         border-radius:8px;padding:5px 11px;white-space:nowrap;}
  .mv-fs:hover{border-color:var(--amber);color:var(--amber);}
  .sec-row{display:flex;align-items:center;justify-content:space-between;margin:18px 0 10px;}
  .sec-title{font-size:13px;font-weight:700;color:var(--t1);}
  .sec-count{font-size:11px;color:var(--t3);}
  .past-card{background:#fff;border:.5px solid var(--border);border-radius:16px;overflow:hidden;
             box-shadow:0 1px 4px rgba(0,0,0,.05);padding:0 20px;}
  .past-table{width:100%;border-collapse:collapse;font-size:12px;table-layout:fixed;font-family:'Bai Jamjuree',sans-serif;}
  .past-table td,.past-table th{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
  .col-time{width:24%;} .col-dur{width:13%;} .col-soc{width:21%;}
  .col-dist{width:14%;} .col-energy{width:16%;} .col-eff{width:12%;}
  .past-table th{font-size:10px;font-weight:700;color:var(--t3);letter-spacing:.08em;text-transform:uppercase;
                 padding:15px 8px 11px 0;text-align:left;border-bottom:.5px solid var(--border);}
  .past-table th:last-child,.past-table td:last-child{text-align:right;padding-right:0;}
  .past-table td{padding:12px 8px;color:var(--t1);border-bottom:.5px solid var(--border);padding-left:0;vertical-align:middle;}
  .past-table tr:last-child td{border-bottom:none;}
  .past-table tbody tr:hover td{background:var(--amber-dim);}
  .tc{color:var(--t2);} .dc{font-weight:600;} .sf{color:var(--t2);} .sa{color:var(--border-mid);margin:0 4px;}
  .nu{color:var(--t3);font-size:10px;} .ev{color:var(--amber);font-weight:700;}
  .empty-card{background:#fff;border:.5px solid var(--border);border-radius:16px;padding:48px;text-align:center;color:var(--t2);font-size:13px;}
  div[data-testid="stTextInput"] input{font-family:'Bai Jamjuree',sans-serif;}
</style>
""", unsafe_allow_html=True)

# ----------------------------------------------------------------------------- controls
c1, c2, _ = st.columns([1, 1, 6])
device = c1.text_input("Device", value="13")
since = c2.text_input("Since", value="2026-06-23")

FULLSCREEN = st.query_params.get("fs") == "1"
if FULLSCREEN:
    st.markdown("""
    <style>
      div[data-testid="stHorizontalBlock"] { display:none !important; }
      .block-container { position:fixed; inset:0; max-width:100% !important; padding:1.2rem 2rem !important;
                         overflow-y:auto; background:#F2F2F2; z-index:99999; }
    </style>
    """, unsafe_allow_html=True)


# ----------------------------------------------------------------------------- board (auto-refresh)
@st.fragment(run_every=REFRESH_EVERY)
def board():
    try:
        rows = cached_query(int(device), since.strip())
        gps = cached_gps(int(device), since.strip())
        st.session_state["rows"] = rows
        st.session_state["gps"] = gps
        st.session_state["ts"] = datetime.now(timezone.utc)
    except Exception:
        pass  # keep last good data on a transient hiccup

    rows = st.session_state.get("rows", [])
    gps = st.session_state.get("gps", [])
    ts = st.session_state.get("ts")
    dev = rows[0]["device_id"] if rows else device

    secs = int((datetime.now(timezone.utc) - ts).total_seconds()) if ts else None
    if secs is None:
        upd = "connecting…"
    elif secs <= 3:
        upd = "Updated just now"
    elif secs < 60:
        upd = f"Updated {secs}s ago"
    else:
        upd = f"Updated {secs // 60}m ago"
    fresh = " fresh" if (secs is not None and secs <= 3) else ""
    fs_now = st.query_params.get("fs") == "1"
    fs_link = ('<a class="mv-fs" href="?fs=0" target="_self">✕ Exit</a>' if fs_now
               else '<a class="mv-fs" href="?fs=1" target="_self">⤢ Fullscreen</a>')

    def _dist(r):
        a, b = num(r["odo_read_start"]), num(r["odo_read_end"])
        return (b - a) if (a is not None and b is not None) else 0.0

    def _is(r, k):
        return str(r[k]).lower() == "true" if k == "is_live" else str(r["battery_status"]).lower() == k

    # The currently-LIVE session, shown as the hero even if it hasn't moved yet.
    live = next((r for r in rows if _is(r, "is_live")), None)
    live_discharge = live if (live and _is(live, "discharging")) else None
    # Older trips: discharging AND travelled more than 1 km.
    trips = [r for r in reversed(rows) if _is(r, "discharging") and _dist(r) > 1]

    st.markdown(f"""
      <div class="mv-head">
        <div class="mv-left">
          <div class="mv-logo">⚡</div>
          <div class="mv-title">Maverick Session</div>
          <div class="mv-dev">Device {dev}</div>
        </div>
        <div class="mv-right">
          <span class="mv-upd{fresh}">{upd}</span>
          <div class="mv-live"><div class="mv-dot"></div>Live</div>
          {fs_link}
        </div>
      </div>
    """, unsafe_allow_html=True)

    if not rows:
        st.info("Querying Databricks… the first query can take up to ~90s while the warehouse wakes.")
        return

    # Hero = the live discharging trip (any distance); else the latest >1km trip.
    if live_discharge is not None:
        hero, hero_live = live_discharge, True
        past = [t for t in trips if t is not live_discharge]
    elif trips:
        hero, hero_live = trips[0], False
        past = trips[1:]
    else:
        st.markdown('<div class="empty-card">No active trip, and no past trips over 1 km yet.</div>',
                    unsafe_allow_html=True)
        return

    render_live_card(hero, gps, hero_live)

    st.markdown(f'<div class="sec-row"><div class="sec-title">Past trips</div>'
                f'<div class="sec-count">{len(past)} total</div></div>', unsafe_allow_html=True)
    if past:
        render_past_trips(past)
    else:
        st.markdown('<div class="empty-card">No past trips yet.</div>', unsafe_allow_html=True)


board()
