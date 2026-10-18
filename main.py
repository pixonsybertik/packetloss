import asyncio
import json
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import httpx

app = FastAPI()

COUNTRIES = [
    {"name": "UAE", "code": "AE"}, {"name": "Turkey", "code": "TR"},
    {"name": "USA", "code": "US"}, {"name": "France", "code": "FR"},
    {"name": "Germany", "code": "DE"}, {"name": "Netherlands", "code": "NL"},
    {"name": "UK", "code": "GB"}, {"name": "Canada", "code": "CA"},
    {"name": "Japan", "code": "JP"}, {"name": "South Korea", "code": "KR"},
    {"name": "Singapore", "code": "SG"}, {"name": "Australia", "code": "AU"},
    {"name": "Brazil", "code": "BR"}, {"name": "India", "code": "IN"},
    {"name": "Italy", "code": "IT"}, {"name": "Spain", "code": "ES"},
    {"name": "Poland", "code": "PL"}, {"name": "Sweden", "code": "SE"},
    {"name": "Switzerland", "code": "CH"}, {"name": "Russia", "code": "RU"},
    {"name": "South Africa", "code": "ZA"}, {"name": "Egypt", "code": "EG"},
    {"name": "Saudi Arabia", "code": "SA"}, {"name": "Israel", "code": "IL"},
    {"name": "Indonesia", "code": "ID"}, {"name": "Malaysia", "code": "MY"},
    {"name": "Thailand", "code": "TH"}, {"name": "Vietnam", "code": "VN"},
    {"name": "Mexico", "code": "MX"}, {"name": "Argentina", "code": "AR"},
    {"name": "Chile", "code": "CL"}, {"name": "Colombia", "code": "CO"},
    {"name": "Portugal", "code": "PT"}, {"name": "Greece", "code": "GR"},
    {"name": "Czechia", "code": "CZ"}, {"name": "Austria", "code": "AT"},
    {"name": "Belgium", "code": "BE"}, {"name": "Denmark", "code": "DK"},
    {"name": "Norway", "code": "NO"}, {"name": "Finland", "code": "FI"},
    {"name": "Ireland", "code": "IE"}, {"name": "New Zealand", "code": "NZ"},
    {"name": "Philippines", "code": "PH"}, {"name": "Pakistan", "code": "PK"},
    {"name": "Bangladesh", "code": "BD"}, {"name": "Nigeria", "code": "NG"},
    {"name": "Kenya", "code": "KE"}, {"name": "Morocco", "code": "MA"},
    {"name": "Romania", "code": "RO"}, {"name": "Hungary", "code": "HU"},
]

TARGET = "8.8.8.8"
HTTP_TARGET = "https://www.google.com"
SEMAPHORE = asyncio.Semaphore(5)

async def run_ping_from_country(client: httpx.AsyncClient, country_code: str):
    async with SEMAPHORE:
        try:
            create_resp = await client.post(
                "https://api.globalping.io/v1/measurements",
                json={
                    "target": TARGET,
                    "type": "ping",
                    "locations": [{"country": country_code}],
                    "limit": 1,
                    "measurementOptions": {"packets": 3},
                },
                timeout=10.0,
            )
            if create_resp.status_code != 202:
                return None
            measurement_id = create_resp.json()["id"]
            for _ in range(8):
                await asyncio.sleep(0.8)
                get_resp = await client.get(
                    f"https://api.globalping.io/v1/measurements/{measurement_id}",
                    timeout=8.0,
                )
                if get_resp.status_code == 200:
                    data = get_resp.json()
                    if data.get("status") == "finished":
                        results = data.get("results", [])
                        if results:
                            stats = results[0].get("result", {}).get("stats", {})
                            return {
                                "loss": float(stats.get("loss", 0)) if stats.get("loss") is not None else None,
                                "min": float(stats.get("min", 0)) if stats.get("min") is not None else None,
                                "avg": float(stats.get("avg", 0)) if stats.get("avg") is not None else None,
                                "max": float(stats.get("max", 0)) if stats.get("max") is not None else None,
                                "share_id": measurement_id,
                            }
            return None
        except Exception:
            return None

@app.get("/api/mtr")
async def run_mtr(country_code: str):
    """MTR: حذف measurementOptions برای جلوگیری از خطای اعتبارسنجی"""
    async with httpx.AsyncClient() as client:
        try:
            create_resp = await client.post(
                "https://api.globalping.io/v1/measurements",
                json={
                    "target": TARGET,
                    "type": "mtr",
                    "locations": [{"country": country_code}],
                    "limit": 1,
                    # بدون measurementOptions، API از مقادیر پیش‌فرض استفاده می‌کند
                },
                timeout=10.0,
            )
            if create_resp.status_code != 202:
                err = create_resp.text
                return JSONResponse({"error": f"Create failed: {err}"}, status_code=400)
            mid = create_resp.json()["id"]
            for _ in range(20):
                await asyncio.sleep(1.0)
                get_resp = await client.get(
                    f"https://api.globalping.io/v1/measurements/{mid}",
                    timeout=10.0,
                )
                if get_resp.status_code == 200:
                    data = get_resp.json()
                    if data.get("status") == "finished":
                        results = data.get("results", [])
                        if results:
                            hops = results[0].get("result", {}).get("hops", [])
                            return JSONResponse({"hops": hops, "measurement_id": mid})
            return JSONResponse({"error": "MTR timeout"}, status_code=408)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/http")
async def run_http(country_code: str):
    """HTTP: دریافت زمان‌بندی کامل Total، DNS، TCP، TLS، First Byte، Download"""
    async with httpx.AsyncClient() as client:
        try:
            create_resp = await client.post(
                "https://api.globalping.io/v1/measurements",
                json={
                    "target": HTTP_TARGET,
                    "type": "http",
                    "locations": [{"country": country_code}],
                    "limit": 1,
                    "measurementOptions": {
                        "protocol": "HTTPS",
                        "request": {"method": "GET"}
                    }
                },
                timeout=10.0,
            )
            if create_resp.status_code != 202:
                err = create_resp.text
                return JSONResponse({"error": f"Create failed: {err}"}, status_code=400)
            mid = create_resp.json()["id"]
            for _ in range(15):
                await asyncio.sleep(1.0)
                get_resp = await client.get(
                    f"https://api.globalping.io/v1/measurements/{mid}",
                    timeout=10.0,
                )
                if get_resp.status_code == 200:
                    data = get_resp.json()
                    if data.get("status") == "finished":
                        results = data.get("results", [])
                        if results:
                            timings = results[0].get("result", {}).get("timings", {})
                            status = results[0].get("result", {}).get("statusCode", "N/A")
                            return JSONResponse({
                                "timings": timings,
                                "statusCode": status,
                                "measurement_id": mid
                            })
            return JSONResponse({"error": "HTTP timeout"}, status_code=408)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    async with httpx.AsyncClient() as client:
        tasks = [run_ping_from_country(client, c["code"]) for c in COUNTRIES]
        results = await asyncio.gather(*tasks)

    rows_data = []
    for country, res in zip(COUNTRIES, results):
        if res and res["loss"] is not None:
            rows_data.append({
                "name": country["name"], "code": country["code"],
                "loss": round(res["loss"], 2),
                "min": round(res["min"], 1) if res["min"] is not None else None,
                "avg": round(res["avg"], 1) if res["avg"] is not None else None,
                "max": round(res["max"], 1) if res["max"] is not None else None,
                "share_id": res.get("share_id"),
            })
        else:
            rows_data.append({"name": country["name"], "code": country["code"], "loss": None, "min": None, "avg": None, "max": None, "share_id": None})

    valid = [r for r in rows_data if r["loss"] is not None]
    avg_loss = round(sum(r["loss"] for r in valid) / len(valid), 2) if valid else 0
    worst = max(valid, key=lambda r: r["loss"]) if valid else None
    healthy = sum(1 for r in valid if r["loss"] < 5)

    labels = [r["name"] for r in rows_data]
    losses = [r["loss"] if r["loss"] is not None else 0 for r in rows_data]

    table_rows = ""
    for r in sorted(rows_data, key=lambda x: (x["loss"] is None, -(x["loss"] or 0))):
        if r["loss"] is None:
            table_rows += f"""
            <tr data-code="{r['code']}" data-name="{r['name']}">
                <td class="name">{r['name']}</td><td class="loss na">N/A</td>
                <td class="lat">-</td><td class="lat">-</td><td class="lat">-</td>
                <td class="actions"><button class="mtr-btn" disabled>MTR</button><button class="http-btn" disabled>HTTP</button></td>
            </tr>"""
        else:
            lv = r["loss"]
            cls = "high" if lv > 20 else ("med" if lv > 5 else "low")
            share_link = f"https://globalping.io?measurement={r['share_id']}" if r["share_id"] else "#"
            table_rows += f"""
            <tr data-code="{r['code']}" data-name="{r['name']}">
                <td class="name">{r['name']}</td>
                <td class="loss {cls}">{lv:.1f}%</td>
                <td class="lat">{r['min'] if r['min'] is not None else '-'}</td>
                <td class="lat">{r['avg'] if r['avg'] is not None else '-'}</td>
                <td class="lat">{r['max'] if r['max'] is not None else '-'}</td>
                <td class="actions">
                    <button class="mtr-btn" data-code="{r['code']}" data-name="{r['name']}">MTR</button>
                    <button class="http-btn" data-code="{r['code']}" data-name="{r['name']}">HTTP</button>
                    <a href="{share_link}" target="_blank" class="share-btn">↗</a>
                </td>
            </tr>"""

    top_bad = sorted(valid, key=lambda r: -r["loss"])[:10]
    bar_labels = json.dumps([r["name"] for r in top_bad])
    bar_values = json.dumps([r["loss"] for r in top_bad])
    labels_json = json.dumps(labels)
    losses_json = json.dumps(losses)
    worst_name = worst["name"] if worst else "—"
    worst_val = f"{worst['loss']:.1f}%" if worst else "—"

    html = f"""
    <!DOCTYPE html>
    <html lang="en" dir="ltr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Packet Loss — LOGIC</title>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            html, body {{ width: 100%; height: 100%; }}
            body {{ font-family: 'Inter', sans-serif; background: #121212; color: #d4d4d4; overflow: hidden; }}
            .app {{ height: 100vh; display: grid; grid-template-rows: auto auto auto 1fr; gap: 1px; background: #1e1e1e; }}
            header {{ display: flex; justify-content: space-between; align-items: center; padding: 0 1.5rem; height: 56px; background: #121212; }}
            header h1 {{ font-size: 0.95rem; font-weight: 600; letter-spacing: 2px; color: #f5f5f5; }}
            header .meta {{ display: flex; gap: 1.5rem; align-items: center; font-size: 0.72rem; color: #525252; font-family: 'JetBrains Mono', monospace; }}
            header .meta .logic {{ color: #737373; letter-spacing: 1px; }}
            .filters {{ display: flex; gap: 0.75rem; align-items: center; padding: 0.75rem 1.5rem; background: #121212; }}
            .filters input, .filters select {{ background: #1a1a1a; border: 1px solid #2a2a2a; color: #d4d4d4; padding: 0.45rem 0.75rem; border-radius: 6px; font-family: 'Inter', sans-serif; font-size: 0.78rem; outline: none; }}
            .filters input:focus, .filters select:focus {{ border-color: #404040; }}
            .filters input {{ flex: 1; max-width: 260px; }}
            .filters .count {{ font-family: 'JetBrains Mono', monospace; font-size: 0.72rem; color: #525252; margin-left: auto; }}
            .stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 1px; background: #1e1e1e; }}
            .stat {{ background: #121212; padding: 1rem 1.5rem; display: flex; flex-direction: column; gap: 0.3rem; }}
            .stat .label {{ font-size: 0.65rem; color: #525252; text-transform: uppercase; letter-spacing: 1.2px; }}
            .stat .value {{ font-size: 1.5rem; font-weight: 600; font-family: 'JetBrains Mono', monospace; color: #f5f5f5; }}
            .stat .value.high {{ color: #ef4444; }} .stat .value.low {{ color: #22c55e; }}
            .stat .sub {{ font-size: 0.7rem; color: #404040; }}
            .main {{ display: grid; grid-template-columns: 1.4fr 1fr 1.3fr; gap: 1px; background: #1e1e1e; min-height: 0; }}
            .panel {{ background: #121212; padding: 1.25rem; display: flex; flex-direction: column; min-height: 0; }}
            .panel h2 {{ font-size: 0.65rem; color: #525252; text-transform: uppercase; letter-spacing: 1.2px; font-weight: 500; margin-bottom: 1rem; display: flex; justify-content: space-between; align-items: center; }}
            .panel h2 .badge {{ font-family: 'JetBrains Mono', monospace; font-size: 0.65rem; color: #737373; }}
            .chart-wrap {{ flex: 1; position: relative; min-height: 0; }}
            .table-wrap {{ flex: 1; overflow-y: auto; min-height: 0; }}
            table {{ width: 100%; border-collapse: collapse; font-size: 0.78rem; }}
            th {{ position: sticky; top: 0; z-index: 2; background: #121212; color: #525252; font-weight: 500; text-align: left; padding: 0.5rem 0.25rem; border-bottom: 1px solid #262626; font-size: 0.62rem; text-transform: uppercase; letter-spacing: 1px; }}
            td {{ padding: 0.55rem 0.25rem; border-bottom: 1px solid #1a1a1a; }}
            .name {{ color: #a3a3a3; font-weight: 400; }}
            .loss {{ font-family: 'JetBrains Mono', monospace; font-weight: 600; }}
            .loss.high {{ color: #ef4444; }} .loss.med {{ color: #eab308; }} .loss.low {{ color: #22c55e; }} .loss.na {{ color: #404040; }}
            .lat {{ font-family: 'JetBrains Mono', monospace; color: #737373; font-size: 0.72rem; }}
            .actions {{ display: flex; gap: 0.3rem; }}
            .mtr-btn, .http-btn {{ background: #1a1a1a; border: 1px solid #2a2a2a; color: #a3a3a3; padding: 0.15rem 0.4rem; border-radius: 4px; font-size: 0.65rem; cursor: pointer; }}
            .mtr-btn:hover:not(:disabled), .http-btn:hover:not(:disabled) {{ border-color: #60a5fa; color: #60a5fa; }}
            .mtr-btn:disabled, .http-btn:disabled {{ opacity: 0.3; cursor: not-allowed; }}
            .share-btn {{ background: none; border: 1px solid #2a2a2a; color: #525252; padding: 0.15rem 0.35rem; border-radius: 4px; font-size: 0.7rem; text-decoration: none; }}
            .share-btn:hover {{ border-color: #60a5fa; color: #60a5fa; }}
            .table-wrap::-webkit-scrollbar {{ width: 4px; }} .table-wrap::-webkit-scrollbar-thumb {{ background: #262626; }}
            .note {{ font-size: 0.65rem; color: #404040; margin-top: 0.75rem; font-family: 'JetBrains Mono', monospace; }}
            .modal-bg {{ display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.7); z-index: 100; justify-content: center; align-items: center; }}
            .modal-bg.active {{ display: flex; }}
            .modal {{ background: #161616; border: 1px solid #2a2a2a; border-radius: 12px; width: 90%; max-width: 800px; max-height: 80vh; display: flex; flex-direction: column; }}
            .modal-header {{ display: flex; justify-content: space-between; align-items: center; padding: 1rem 1.25rem; border-bottom: 1px solid #262626; }}
            .modal-header h3 {{ font-size: 0.85rem; color: #f5f5f5; font-weight: 500; }}
            .modal-header button {{ background: none; border: none; color: #737373; font-size: 1.2rem; cursor: pointer; }}
            .modal-body {{ padding: 1.25rem; overflow-y: auto; flex: 1; }}
            .hop-table {{ width: 100%; border-collapse: collapse; font-size: 0.75rem; }}
            .hop-table th {{ background: #121212; color: #525252; padding: 0.4rem 0.3rem; border-bottom: 1px solid #262626; font-size: 0.62rem; text-transform: uppercase; }}
            .hop-table td {{ padding: 0.4rem 0.3rem; border-bottom: 1px solid #1a1a1a; font-family: 'JetBrains Mono', monospace; color: #a3a3a3; }}
            .hop-table .loss-cell {{ font-weight: 600; }}
            .hop-table .loss-cell.high {{ color: #ef4444; }} .hop-table .loss-cell.med {{ color: #eab308; }} .hop-table .loss-cell.low {{ color: #22c55e; }}
            .hop-table .bottleneck {{ background: rgba(239,68,68,0.1); }}
            .http-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 0.75rem; margin-bottom: 1rem; }}
            .http-card {{ background: #121212; border: 1px solid #262626; border-radius: 8px; padding: 0.75rem 1rem; }}
            .http-card .label {{ font-size: 0.6rem; color: #525252; text-transform: uppercase; letter-spacing: 1px; }}
            .http-card .value {{ font-size: 1.1rem; font-weight: 600; font-family: 'JetBrains Mono', monospace; color: #f5f5f5; margin-top: 0.2rem; }}
            .spinner {{ text-align: center; color: #525252; padding: 2rem; }}
            @media (max-width: 1200px) {{ .main {{ grid-template-columns: 1fr 1fr; }} .panel.radar {{ grid-column: span 2; }} .stats {{ grid-template-columns: repeat(2,1fr); }} }}
            @media (max-width: 800px) {{ body {{ overflow: auto; }} .app {{ height: auto; min-height: 100vh; }} .main {{ grid-template-columns: 1fr; }} .panel.radar {{ grid-column: span 1; min-height: 320px; }} .panel {{ min-height: 280px; }} header {{ flex-direction: column; height: auto; padding: 0.75rem 1rem; gap: 0.5rem; }} .stats {{ grid-template-columns: repeat(2,1fr); }} .filters {{ flex-wrap: wrap; }} }}
        </style>
    </head>
    <body>
        <div class="app">
            <header>
                <h1>PACKET LOSS</h1>
                <div class="meta">
                    <span>TARGET {TARGET}</span>
                    <span>PROBES {len(valid)}/{len(COUNTRIES)}</span>
                    <span class="logic">BUILT BY LOGIC</span>
                </div>
            </header>
            <div class="filters">
                <input type="text" id="searchInput" placeholder="Search country...">
                <select id="thresholdSelect">
                    <option value="all">All</option><option value="low">Healthy (&lt;5%)</option>
                    <option value="med">Medium (5-20%)</option><option value="high">Bad (&gt;20%)</option>
                </select>
                <span class="count" id="visibleCount">{len(COUNTRIES)} shown</span>
            </div>
            <div class="stats">
                <div class="stat"><div class="label">Average Loss</div><div class="value">{avg_loss:.1f}%</div><div class="sub">across active probes</div></div>
                <div class="stat"><div class="label">Worst Country</div><div class="value high">{worst_val}</div><div class="sub">{worst_name}</div></div>
                <div class="stat"><div class="label">Healthy (&lt;5%)</div><div class="value low">{healthy}</div><div class="sub">of {len(valid)} active</div></div>
                <div class="stat"><div class="label">Total Probes</div><div class="value">{len(COUNTRIES)}</div><div class="sub">queried this cycle</div></div>
            </div>
            <div class="main">
                <div class="panel radar"><h2>Loss Distribution <span class="badge">RADAR</span></h2><div class="chart-wrap"><canvas id="radarChart"></canvas></div></div>
                <div class="panel"><h2>Top 10 Worst <span class="badge">BAR</span></h2><div class="chart-wrap"><canvas id="barChart"></canvas></div></div>
                <div class="panel">
                    <h2>Diagnostics <span class="badge" id="tableCount">{len(COUNTRIES)}</span></h2>
                    <div class="table-wrap">
                        <table>
                            <thead><tr><th>Country</th><th>Loss</th><th>Min</th><th>Avg</th><th>Max</th><th>Actions</th></tr></thead>
                            <tbody id="tableBody">{table_rows}</tbody>
                        </table>
                    </div>
                    <p class="note">MTR: hop-by-hop · HTTP: full timing breakdown</p>
                </div>
            </div>
        </div>
        <div class="modal-bg" id="mtrModal">
            <div class="modal">
                <div class="modal-header"><h3 id="modalTitle">MTR</h3><button onclick="closeModal()">✕</button></div>
                <div class="modal-body" id="modalBody"><div class="spinner">Running MTR...</div></div>
            </div>
        </div>
        <div class="modal-bg" id="httpModal">
            <div class="modal">
                <div class="modal-header"><h3 id="httpTitle">HTTP Timings</h3><button onclick="closeHttpModal()">✕</button></div>
                <div class="modal-body" id="httpBody"><div class="spinner">Running HTTP test...</div></div>
            </div>
        </div>
        <script>
            const allData = {json.dumps(rows_data)};
            let radarChart, barChart;
            const lossColor = (v) => v > 20 ? '#ef4444' : v > 5 ? '#eab308' : '#22c55e';

            function buildRadar(labels, data) {{
                if (radarChart) radarChart.destroy();
                radarChart = new Chart(document.getElementById('radarChart').getContext('2d'), {{
                    type: 'radar', data: {{ labels, datasets: [{{ label: 'Loss %', data, backgroundColor: 'rgba(239,68,68,0.08)', borderColor: '#ef4444', borderWidth: 1.5, pointBackgroundColor: '#ef4444', pointBorderColor: '#121212', pointRadius: 2, fill: true }}] }},
                    options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ r: {{ beginAtZero: true, suggestedMax: 100, angleLines: {{ color: '#1e1e1e' }}, grid: {{ color: '#1e1e1e' }}, pointLabels: {{ color: '#525252', font: {{ family: 'Inter', size: 7 }} }}, ticks: {{ color: '#333', backdropColor: 'transparent', stepSize: 25, font: {{ size: 8 }} }} }} }} }}
                }});
            }}
            function buildBar(labels, data) {{
                if (barChart) barChart.destroy();
                barChart = new Chart(document.getElementById('barChart').getContext('2d'), {{
                    type: 'bar', data: {{ labels, datasets: [{{ data, backgroundColor: data.map(v => lossColor(v)), borderWidth: 0, borderRadius: 2 }}] }},
                    options: {{ indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ beginAtZero: true, max: 100, grid: {{ color: '#1e1e1e' }}, ticks: {{ color: '#404040', font: {{ size: 9 }} }} }}, y: {{ grid: {{ display: false }}, ticks: {{ color: '#737373', font: {{ size: 9 }} }} }} }} }}
                }});
            }}
            function applyFilter() {{
                const q = document.getElementById('searchInput').value.toLowerCase().trim();
                const th = document.getElementById('thresholdSelect').value;
                const filtered = allData.filter(r => {{
                    if (q && !r.name.toLowerCase().includes(q)) return false;
                    if (th === 'all') return true;
                    if (r.loss === null) return false;
                    if (th === 'low') return r.loss < 5; if (th === 'med') return r.loss >= 5 && r.loss <= 20; if (th === 'high') return r.loss > 20; return true;
                }});
                const tbody = document.getElementById('tableBody');
                tbody.innerHTML = filtered.map(r => {{
                    if (r.loss === null) return `<tr data-code="${{r.code}}" data-name="${{r.name}}"><td class="name">${{r.name}}</td><td class="loss na">N/A</td><td class="lat">-</td><td class="lat">-</td><td class="lat">-</td><td class="actions"><button class="mtr-btn" disabled>MTR</button><button class="http-btn" disabled>HTTP</button></td></tr>`;
                    const cls = r.loss > 20 ? 'high' : (r.loss > 5 ? 'med' : 'low');
                    const share = r.share_id ? `https://globalping.io?measurement=${{r.share_id}}` : '#';
                    return `<tr data-code="${{r.code}}" data-name="${{r.name}}"><td class="name">${{r.name}}</td><td class="loss ${{cls}}">${{r.loss.toFixed(1)}}%</td><td class="lat">${{r.min !== null ? r.min : '-'}}</td><td class="lat">${{r.avg !== null ? r.avg : '-'}}</td><td class="lat">${{r.max !== null ? r.max : '-'}}</td><td class="actions"><button class="mtr-btn" data-code="${{r.code}}" data-name="${{r.name}}">MTR</button><button class="http-btn" data-code="${{r.code}}" data-name="${{r.name}}">HTTP</button><a href="${{share}}" target="_blank" class="share-btn">↗</a></td></tr>`;
                }}).join('');
                const validFiltered = filtered.filter(r => r.loss !== null);
                buildRadar(validFiltered.map(r => r.name), validFiltered.map(r => r.loss));
                const top = [...validFiltered].sort((a,b) => b.loss - a.loss).slice(0, 10);
                buildBar(top.map(r => r.name), top.map(r => r.loss));
                document.getElementById('visibleCount').textContent = `${{filtered.length}} shown`;
                document.getElementById('tableCount').textContent = filtered.length;
                attachListeners();
            }}
            function attachListeners() {{
                document.querySelectorAll('.mtr-btn:not(:disabled)').forEach(btn => {{
                    btn.onclick = () => runMtr(btn.dataset.code, btn.dataset.name);
                }});
                document.querySelectorAll('.http-btn:not(:disabled)').forEach(btn => {{
                    btn.onclick = () => runHttp(btn.dataset.code, btn.dataset.name);
                }});
            }}
            async function runMtr(code, name) {{
                const modal = document.getElementById('mtrModal');
                const title = document.getElementById('modalTitle');
                const body = document.getElementById('modalBody');
                title.textContent = `MTR from ${{name}} to {TARGET}`;
                body.innerHTML = '<div class="spinner">Running MTR... This may take up to 20s.</div>';
                modal.classList.add('active');
                try {{
                    const resp = await fetch(`/api/mtr?country_code=${{code}}`);
                    const data = await resp.json();
                    if (data.error) {{ body.innerHTML = `<div class="spinner">Error: ${{data.error}}</div>`; return; }}
                    if (!data.hops || data.hops.length === 0) {{ body.innerHTML = '<div class="spinner">No hops returned.</div>'; return; }}
                    let html = '<table class="hop-table"><thead><tr><th>Hop</th><th>Host</th><th>Loss</th><th>Avg</th><th>Status</th></tr></thead><tbody>';
                    let prevLoss = 0;
                    data.hops.forEach(h => {{
                        const loss = h.stats?.loss ?? 0;
                        const cls = loss > 20 ? 'high' : (loss > 5 ? 'med' : 'low');
                        let status = '';
                        if (loss > prevLoss + 10 && loss > 5) {{
                            status = '<span style="color:#ef4444;font-size:0.65rem;">⚠ Bottleneck</span>';
                            html += `<tr class="bottleneck">`;
                        }} else {{
                            html += `<tr>`;
                        }}
                        html += `<td>${{h.hop}}</td><td>${{h.resolvedHost || h.resolvedAddress || '*'}}</td><td class="loss-cell ${{cls}}">${{loss.toFixed(1)}}%</td><td>${{h.stats?.avg?.toFixed(1) || '-'}} ms</td><td>${{status}}</td></tr>`;
                        prevLoss = loss;
                    }});
                    html += '</tbody></table>';
                    html += `<p style="margin-top:0.75rem;font-size:0.65rem;color:#404040;">Measurement ID: ${{data.measurement_id}} · <a href="https://globalping.io?measurement=${{data.measurement_id}}" target="_blank" style="color:#60a5fa;">View on Globalping</a></p>`;
                    body.innerHTML = html;
                }} catch(e) {{ body.innerHTML = `<div class="spinner">Network error: ${{e.message}}</div>`; }}
            }}
            async function runHttp(code, name) {{
                const modal = document.getElementById('httpModal');
                const title = document.getElementById('httpTitle');
                const body = document.getElementById('httpBody');
                title.textContent = `HTTP Timings from ${{name}} to {HTTP_TARGET}`;
                body.innerHTML = '<div class="spinner">Running HTTP test... This may take up to 15s.</div>';
                modal.classList.add('active');
                try {{
                    const resp = await fetch(`/api/http?country_code=${{code}}`);
                    const data = await resp.json();
                    if (data.error) {{ body.innerHTML = `<div class="spinner">Error: ${{data.error}}</div>`; return; }}
                    const t = data.timings || {{}};
                    let html = `<div class="http-grid">`;
                    const metrics = [
                        ['Total', t.total], ['DNS', t.dns], ['TCP', t.tcp],
                        ['TLS', t.tls], ['First Byte', t.firstByte], ['Download', t.download]
                    ];
                    metrics.forEach(([label, val]) => {{
                        html += `<div class="http-card"><div class="label">${{label}}</div><div class="value">${{val !== null && val !== undefined ? val + ' ms' : 'N/A'}}</div></div>`;
                    }});
                    html += `</div>`;
                    html += `<p style="font-size:0.75rem;color:#737373;">HTTP Status: <strong style="color:#f5f5f5;">${{data.statusCode}}</strong></p>`;
                    html += `<p style="margin-top:0.5rem;font-size:0.65rem;color:#404040;">Measurement ID: ${{data.measurement_id}} · <a href="https://globalping.io?measurement=${{data.measurement_id}}" target="_blank" style="color:#60a5fa;">View on Globalping</a></p>`;
                    body.innerHTML = html;
                }} catch(e) {{ body.innerHTML = `<div class="spinner">Network error: ${{e.message}}</div>`; }}
            }}
            function closeModal() {{ document.getElementById('mtrModal').classList.remove('active'); }}
            function closeHttpModal() {{ document.getElementById('httpModal').classList.remove('active'); }}
            document.getElementById('searchInput').addEventListener('input', applyFilter);
            document.getElementById('thresholdSelect').addEventListener('change', applyFilter);
            applyFilter();
        </script>
    </body>
    </html>
    """
    return html

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
