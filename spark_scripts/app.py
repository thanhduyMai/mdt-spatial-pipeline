import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from keplergl import KeplerGl
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import json
import base64

app = FastAPI(title="Viettel SON Optimization Dashboard")

# 配置 Bảng điều khiển kết nối PostGIS
DB_CONFIG = {
    "host": "postgis-dw",
    "port": 5432,
    "database": "mdt_db",
    "user": "postgres",
    "password": "postgres"
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# =========================================================================
# API ENDPOINT 1: TRUY VẤN LỊCH SỬ CHỦ ĐỘNG THEO TỪNG CELL/H3
# =========================================================================
@app.get("/api/cell-history")
def get_cell_history(cell_id: str = Query(..., description="H3 Index hoặc Cell ID"), date: str = Query(..., description="YYYY-MM-DD")):
    query = """
        SELECT 
            hour,
            user_density_count AS user_density,
            avg_rsrp
        FROM mdt_spatial_gold
        WHERE date = %s AND (h3_index = %s OR cell_id = %s)
        ORDER BY hour ASC
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, (date, cell_id, cell_id))
        records = cursor.fetchall()
        cursor.close()
        conn.close()
        return JSONResponse(content={"success": True, "data": records})
    except Exception as e:
        return JSONResponse(content={"success": False, "error": str(e)}, status_code=500)


# =========================================================================
# API ENDPOINT 2: RENDER SIÊU DASHBOARD (FRONTEND HOÀN CHỈNH)
# =========================================================================
@app.get("/", response_class=HTMLResponse)
def render_dashboard(date: str = "2026-06-10"):
    conn = get_db_connection()
    
    # 1. Query dữ liệu không gian ban đầu cho Bản đồ Kepler
    query_spatial = """
        SELECT h3_index, cell_id, cell_lat, cell_lon, h3_center_lat, h3_center_lon, 
               user_density_count AS user_density, avg_rsrp,
               CAST(date || ' ' || hour || ':00:00' AS TIMESTAMP) AS record_time
        FROM mdt_spatial_gold WHERE date = %s ORDER BY hour ASC
    """
    df_spatial = pd.read_sql_query(query_spatial, conn, params=(date,))
    df_spatial['record_time'] = pd.to_datetime(df_spatial['record_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # 2. Query dữ liệu xu hướng chung toàn mạng làm baseline ban đầu cho biểu đồ
    df_spatial['record_time_dt'] = pd.to_datetime(df_spatial['record_time'])
    hourly_trend = df_spatial.groupby(df_spatial['record_time_dt'].dt.hour).agg(
        total_density=('user_density', 'sum'),
        poor_quality_cells=('avg_rsrp', lambda x: int((x < -110).sum()))
    ).reset_index()

    conn.close()

    # Cấu hình ESRI Map Style (Giữ nguyên logic gốc của bạn)
    esri_style = {
        "version": 8, "sources": {
            "esri-satellite": {"type": "raster", "tiles": ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"], "tileSize": 256}
        },
        "layers": [{"id": "satellite-layer", "type": "raster", "source": "esri-satellite"}]
    }
    style_data_uri = f"data:application/json;base64,{base64.b64encode(json.dumps(esri_style).encode('utf-8')).decode('utf-8')}"

    map_config = {
        "version": "v1", "config": {
            "visState": {
                "filters": [{"dataId": ["MDT_Coverage_Live"], "id": "time-filter", "name": ["record_time"], "type": "timeRange", "enlarged": True}],
                "layers": [
                    {"id": "h3-hexagon-layer", "type": "hexagonId", "config": {"dataId": "MDT_Coverage_Live", "label": "Bản đồ H3", "columns": {"hex_id": "h3_index"}, "colorField": {"name": "avg_rsrp", "type": "real"}, "colorScale": "quantize", "sizeField": {"name": "user_density", "type": "integer"}, "isVisible": True}}
                ]
            },
            "mapState": {"latitude": 21.5928, "longitude": 105.8442, "zoom": 11, "pitch": 45, "bearing": 0},
            "mapStyle": {"styleType": "esri_satellite", "mapStyles": {"esri_satellite": {"id": "esri_satellite", "label": "ESRI Satellite", "url": style_data_uri, "custom": True}}}
        }
    }

    m = KeplerGl(height=800, config=map_config)
    m.add_data(data=df_spatial, name="MDT_Coverage_Live")
    html_content = m._repr_html_()

    # Prep dữ liệu mặc định ban đầu ném vào JS
    js_hours = json.dumps([f"{i:02d}:00" for i in range(24)])
    js_default_density = json.dumps(hourly_trend['total_density'].tolist())
    js_default_poor = json.dumps(hourly_trend['poor_quality_cells'].tolist())

    # =====================================================================
    # INJECT ĐỘNG KHỐI FRONTEND GIAO DIỆN CHỈ HUY (CHART CHẠY THEO CLICK)
    # =====================================================================
    dashboard_injection = f"""
    <style>
        html, body {{ width: 100% !important; height: 100vh !important; margin: 0 !important; padding: 0 !important; overflow: hidden !important; }}
        #insight-panel {{ position: absolute; top: 20px; left: 20px; width: 450px; background: #ffffff; border: 1px solid #e0e0e0; border-radius: 8px; box-shadow: 0 8px 24px rgba(0,0,0,0.2); color: #333; z-index: 9999; font-family: Arial, sans-serif; }}
        .panel-header {{ display: flex; justify-content: space-between; align-items: center; padding: 15px 20px; background: #ee0033; color: white; border-radius: 8px 8px 0 0; }}
        .insight-title {{ font-size: 16px; margin: 0; font-weight: bold; }}
        #insight-content {{ padding: 15px 20px; max-height: 75vh; overflow-y: auto; }}
        .alert-box {{ background: #fff3cd; border-left: 4px solid #ffc107; padding: 10px 15px; border-radius: 4px; margin-bottom: 15px; font-size: 13px; }}
        .accordion-btn {{ background-color: #f8f9fa; color: #333; cursor: pointer; padding: 12px 15px; width: 100%; text-align: left; border: 1px solid #dee2e6; outline: none; font-weight: bold; font-size: 13px; display: flex; justify-content: space-between; align-items: center; border-radius: 4px; margin-bottom: 5px; }}
        .accordion-content {{ padding: 10px; background-color: white; display: block; }}
        .chart-container {{ margin-bottom: 15px; position: relative; }}
        #toggle-slicer-btn {{ position: absolute; bottom: 85px; right: 20px; z-index: 9999; background: #ffffff; border: 1px solid #ccc; padding: 8px 12px; border-radius: 4px; cursor: pointer; font-weight: bold; }}
    </style>
    
    <div id="insight-panel">
        <div class="panel-header">
            <h3 class="insight-title">📡 Toàn Mạng (Dữ liệu Tổng hợp)</h3>
        </div>
        <div id="insight-content">
            <div class="alert-box" id="info-status">
                <strong>💡 Hướng dẫn cho Sếp:</strong>
                Bấm vào một ô Lục giác (H3) hoặc Vị trí trạm bất kỳ trên bản đồ để xem phân tích chi tiết 24h của riêng khu vực đó.
            </div>

            <button class="accordion-btn">📈 Biểu đồ Xu hướng Phân tích</button>
            <div class="accordion-content">
                <div style="font-size: 12px; font-weight: bold; color: #555;">Mật độ Người dùng (User Density)</div>
                <div class="chart-container"><canvas id="dynamicDensityChart"></canvas></div>
                
                <div style="font-size: 12px; font-weight: bold; color: #555;">Chỉ số Sóng yếu / Nhiễu (RSRP)</div>
                <div class="chart-container"><canvas id="dynamicPoorChart"></canvas></div>
            </div>
        </div>
    </div>

    <button id="toggle-slicer-btn">👁️ Ẩn/Hiện Thanh Thời Gian</button>

    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script>
        const ctxDensity = document.getElementById('dynamicDensityChart').getContext('2d');
        const ctxPoor = document.getElementById('dynamicPoorChart').getContext('2d');
        const hoursLabels = {js_hours};
        const paramDate = "{date}";

        // Khởi tạo Chart 1: Mật độ User
        const densityChart = new Chart(ctxDensity, {{
            type: 'line',
            data: {{ labels: hoursLabels, datasets: [{{ label: 'User Density', data: {js_default_density}, borderColor: '#0056b3', backgroundColor: 'rgba(0, 86, 179, 0.1)', fill: true, tension: 0.3 }}] }},
            options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }} }}
        }});

        // Khởi tạo Chart 2: Chất lượng RSRP
        const poorChart = new Chart(ctxPoor, {{
            type: 'bar',
            data: {{ labels: hoursLabels, datasets: [{{ label: 'Chỉ số', data: {js_default_poor}, backgroundColor: '#ee0033' }}] }},
            options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }} }}
        }});

        // =====================================================================
        // CORE LOGIC: THAO TÁC EVENT ĐỂ ĐỒNG BỘ HOÁ BIỂU ĐỒ BI (CROSS-FILTERING)
        // =====================================================================
        async function fetchAndUpdateChart(cellId) {{
            document.getElementById('info-status').innerHTML = `⏳ Đang tải dữ liệu cho trạm <b>${{cellId}}</b>...`;
            try {{
                const response = await fetch(`/api/cell-history?cell_id=${{cellId}}&date=${{paramDate}}`);
                const result = await response.json();
                
                if (result.success && result.data.length > 0) {{
                    document.querySelector('.insight-title').innerText = `📡 Chi tiết Trạm: ${{cellId}}`;
                    document.getElementById('info-status').className = "alert-box";
                    document.getElementById('info-status').innerHTML = `<strong>🎯 Đang hiển thị: Cụm ${{cellId}}</strong><br>Dữ liệu đã được bóc tách từ Kho dữ liệu Gold Layer.`;
                    
                    // Map data từ API trả về đúng 24h tương ứng
                    const densityData = new Array(24).fill(0);
                    const rsrpData = new Array(24).fill(0);
                    
                    result.data.forEach(item => {{
                        densityData[item.hour] = item.user_density;
                        rsrpData[item.hour] = item.avg_rsrp;
                    }});

                    // Đẩy dữ liệu mới vào và bắt Chart vẽ lại
                    densityChart.data.datasets[0].data = densityData;
                    densityChart.update();

                    poorChart.data.datasets[0].data = rsrpData;
                    poorChart.data.datasets[0].label = 'RSRP Trung bình (dBm)';
                    poorChart.update();
                }} else {{
                    document.getElementById('info-status').innerHTML = `⚠️ Không tìm thấy bản ghi lịch sử 24h của vùng: <b>${{cellId}}</b>`;
                }}
            }} catch (err) {{
                console.error("Lỗi gọi API:", err);
            }}
        }}

        // Sử dụng MutationObserver lắng nghe Tooltip của Kepler khi Sếp di chuột / Click vào vùng bản đồ
        window.lastCheckedId = null;
        const observer = new MutationObserver((mutations) => {{
            const popoverTable = document.querySelector('.map-popover__table');
            if (popoverTable) {{
                const rows = popoverTable.querySelectorAll('tr');
                let targetId = null;
                rows.forEach(row => {{
                    const label = row.querySelector('.td-label')?.innerText?.toLowerCase();
                    const value = row.querySelector('.td-value')?.innerText;
                    if (label && (label.includes('h3_index') || label.includes('cell_id'))) {{
                        targetId = value.trim();
                    }}
                }});
                
                // Nếu sếp chọn một vùng mới, lập tức kích hoạt API cập nhật biểu đồ
                if (targetId && window.lastCheckedId !== targetId) {{
                    window.lastCheckedId = targetId;
                    fetchAndUpdateChart(targetId);
                }}
            }}
        }});
        observer.observe(document.body, {{ childList: true, subtree: true }});

        // Ẩn/Hiện Time Slicer
        document.getElementById('toggle-slicer-btn').addEventListener('click', () => {{
            const timeWidget = document.querySelector('[class*="bottom-widget"]');
            if (timeWidget) timeWidget.style.display = timeWidget.style.display === 'none' ? '' : 'none';
        }});
    </script>
    """
    
    parts = html_content.rsplit("</body>", 1)
    if len(parts) == 2:
        return f"{parts[0]}{dashboard_injection}</body>{parts[1]}"
    return html_content

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)