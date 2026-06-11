import psycopg2
import pandas as pd
from keplergl import KeplerGl
import argparse
import sys
import json
import base64

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Ngày chạy pipeline (YYYY-MM-DD)")
    args = parser.parse_args()

    print(f"🚀 Đang kéo toàn bộ dữ liệu ngày {args.date} để kết xuất Kepler.gl Dashboard...")
    
    # 1. Kết nối PostGIS
    try:
        conn = psycopg2.connect(
            host="postgis-dw", 
            port=5432,
            database="mdt_db",
            user="postgres",
            password="postgres"
        )
    except Exception as e:
        print(f"❌ Lỗi kết nối PostGIS: {e}")
        sys.exit(1)

    # 2. Query dữ liệu Không gian (Map Data)
    query_spatial = """
        SELECT 
            h3_index,
            cell_id,               
            cell_lat,              
            cell_lon,              
            h3_center_lat,         
            h3_center_lon,         
            user_density_count AS user_density, 
            avg_rsrp,
            CAST(date || ' ' || hour || ':00:00' AS TIMESTAMP) AS record_time
        FROM mdt_spatial_gold
        WHERE date = %s
        ORDER BY hour ASC
    """
    
    # 3. Query dữ liệu Báo cáo chất lượng (Audit Report)
    query_audit = """
        SELECT *
        FROM mdt_audit_report
        WHERE date = %s
        ORDER BY hour ASC
    """
    
    query_heatmap = """
        SELECT 
            hour,
            EXTRACT(ISODOW FROM CAST(date AS DATE)) as dow_num,
            AVG(user_density_count) AS avg_density      
        FROM mdt_spatial_gold
        WHERE CAST(date AS DATE) BETWEEN CAST(%s AS DATE) - INTERVAL '28 days' AND CAST(%s AS DATE)
        GROUP BY hour, dow_num
        ORDER BY dow_num ASC, hour ASC
    """
    try:
        df_spatial = pd.read_sql_query(query_spatial, conn, params=(args.date,))
        df_spatial['record_time'] = pd.to_datetime(df_spatial['record_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            df_audit = pd.read_sql_query(query_audit, conn, params=(args.date,))
        except:
            df_audit = pd.DataFrame() 
            
        try:
            df_hm = pd.read_sql_query(query_heatmap, conn, params=(args.date, args.date))
        except Exception as e:
            print(f"⚠️ Không kéo được data lịch sử cho Heatmap: {e}")
            df_hm = pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Lỗi truy vấn dữ liệu SQL: {e}")
        conn.close()
        sys.exit(1)
        
    conn.close()

    if df_spatial.empty:
        print(f"⚠️ Không có dữ liệu không gian cho ngày {args.date}. Dừng tạo bản đồ.")
        sys.exit(0)

    print(f"📊 Đã tải {len(df_spatial)} bản ghi không gian và {len(df_audit)} bản ghi Audit. Đang dựng siêu bản đồ...")

    # =========================================================================
    # CHUẨN BỊ VÀ TÍNH TOÁN DỮ LIỆU XU HƯỚNG / DỰ BÁO EMA
    # =========================================================================
    df_spatial['record_time_dt'] = pd.to_datetime(df_spatial['record_time'])
    hourly_trend = df_spatial.groupby('record_time_dt').agg(
        total_density=('user_density', 'sum'),
        mean_rsrp=('avg_rsrp', 'mean'),
        poor_quality_cells=('avg_rsrp', lambda x: (x < -110).sum())
    ).reset_index()

    last_time = hourly_trend['record_time_dt'].max()
    future_times = [last_time + pd.Timedelta(hours=i) for i in range(1, 4)]
    future_df = pd.DataFrame({'record_time_dt': future_times})
    
    future_df['total_density'] = hourly_trend['total_density'].ewm(span=4).mean().iloc[-1]
    future_df['mean_rsrp'] = hourly_trend['mean_rsrp'].ewm(span=4).mean().iloc[-1]
    future_df['poor_quality_cells'] = hourly_trend['poor_quality_cells'].ewm(span=4).mean().iloc[-1]
    
    hourly_trend['is_forecast'] = False
    future_df['is_forecast'] = True
    combined_trend = pd.concat([hourly_trend, future_df], ignore_index=True)
    combined_trend['hour_label'] = combined_trend['record_time_dt'].dt.strftime('%H:00')

    js_labels = json.dumps(combined_trend['hour_label'].tolist())
    js_density = json.dumps(combined_trend['total_density'].tolist())
    js_rsrp = json.dumps(combined_trend['mean_rsrp'].round(2).tolist())
    js_poor_cells = json.dumps(combined_trend['poor_quality_cells'].tolist())
    js_is_forecast = json.dumps(combined_trend['is_forecast'].tolist())

    # =========================================================================
    # CHUẨN BỊ DATA VÀ CẤU HÌNH CHO VIETTEL HEATMAP
    # =========================================================================
    dow_map = {1: "Thứ 2", 2: "Thứ 3", 3: "Thứ 4", 4: "Thứ 5", 5: "Thứ 6", 6: "Thứ 7", 7: "Chủ Nhật"}
    heatmap_dict = {f"{h}_{d}": 0.0 for d in range(1, 8) for h in range(24)}

    max_density = 1
    if not df_hm.empty:
        max_density = float(df_hm['avg_density'].max()) if df_hm['avg_density'].max() > 0 else 1
        for _, row in df_hm.iterrows():
            if int(row['dow_num']) in dow_map:
                heatmap_dict[f"{int(row['hour'])}_{int(row['dow_num'])}"] = float(row['avg_density'])

    heatmap_list = [{"x": f"{h:02d}:00", "y": dow_map[d], "v": heatmap_dict[f"{h}_{d}"]} for d in range(1, 8) for h in range(24)]

    js_heatmap_data = json.dumps(heatmap_list)
    js_hours_labels = json.dumps([f"{i:02d}:00" for i in range(24)])
    js_dow_labels = json.dumps(["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ Nhật"])

    # =========================================================================
    # --- BẮT ĐẦU CẤU HÌNH BẢN ĐỒ VÀ WIDGET ---
    # =========================================================================
    esri_style = {
        "version": 8,
        "sources": {
            "esri-satellite": {
                "type": "raster",
                "tiles": ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"],
                "tileSize": 256
            },
            "esri-labels": {
                "type": "raster",
                "tiles": ["https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}"],
                "tileSize": 256
            }
        },
        "layers": [
            {"id": "satellite-layer", "type": "raster", "source": "esri-satellite", "minzoom": 0, "maxzoom": 22},
            {"id": "labels-layer", "type": "raster", "source": "esri-labels", "minzoom": 0, "maxzoom": 22}
        ]
    }
    style_json_str = json.dumps(esri_style)
    b64_style = base64.b64encode(style_json_str.encode("utf-8")).decode("utf-8")
    style_data_uri = f"data:application/json;base64,{b64_style}"

    map_config = {
        "version": "v1",
        "config": {
            "visState": {
                "filters": [
                    {
                        "dataId": ["MDT_Coverage_Live"],
                        "id": "time-filter",
                        "name": ["record_time"],
                        "type": "timeRange",
                        "enlarged": True 
                    }
                ],
                "layers": [
                    {
                        "id": "h3-hexagon-layer",
                        "type": "hexagonId",
                        "config": {
                            "dataId": "MDT_Coverage_Live",
                            "label": "Bản đồ H3",
                            "columns": {"hex_id": "h3_index"},
                            "colorField": {"name": "avg_rsrp", "type": "real"},
                            "colorScale": "quantize",
                            "sizeField": {"name": "user_density", "type": "integer"},
                            "sizeScale": "linear",
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.8,
                                "coverage": 0.95,
                                "enable3d": True,
                                "elevationScale": 20,
                                "sizeRange": [0, 500],
                                "colorRange": {
                                    "name": "ColorBrewer RdYlGn-6",
                                    "type": "diverging",
                                    "category": "ColorBrewer",
                                    "colors": ["#d73027", "#fc8d59", "#fee08b", "#d9ef8b", "#91cf60", "#1a9850"]
                                }
                            }
                        }
                    },
                    {
                        "id": "arc-signal-layer",
                        "type": "arc",
                        "config": {
                            "dataId": "MDT_Coverage_Live",
                            "label": "Tia sóng Trạm - H3",
                            "color": [248, 149, 112],
                            "columns": {"lat0": "cell_lat", "lng0": "cell_lon", "lat1": "h3_center_lat", "lng1": "h3_center_lon"},
                            "isVisible": True,
                            "visConfig": {"opacity": 0.8, "thickness": 2, "targetColor": [255, 203, 153]}
                        }
                    },
                    {
                        "id": "point-label-layer",
                        "type": "point",
                        "config": {
                            "dataId": "MDT_Coverage_Live",
                            "label": "Trạm có số liệu",
                            "columns": {"lat": "cell_lat", "lng": "cell_lon"},
                            "colorField": {"name": "avg_rsrp", "type": "real"},
                            "colorScale": "quantize",
                            "isVisible": True,
                            "visConfig": {
                                "radius": 15,
                                "fixedRadius": False,
                                "opacity": 0.95,
                                "outline": True,
                                "thickness": 2,
                                "strokeColor": [255, 255, 255],
                                "colorRange": {
                                    "name": "ColorBrewer",
                                    "type": "diverging",
                                    "colors": ["#d73027", "#fc8d59", "#fee08b", "#d9ef8b", "#91cf60", "#1a9850"]
                                }
                            },
                            "textLabel": [
                                {
                                    "field": {"name": "user_density", "type": "integer"},
                                    "color": [25, 25, 25],
                                    "size": 13,
                                    "offset": [0, 0],
                                    "anchor": "middle",
                                    "alignment": "center"
                                }
                            ]
                        }
                    }
                ]
            },
            "mapState": {"latitude": 21.5928, "longitude": 105.8442, "zoom": 11, "pitch": 45, "bearing": 0},
            "mapStyle": {
                "styleType": "esri_satellite",
                "mapStyles": {"esri_satellite": {"id": "esri_satellite", "label": "ESRI Satellite", "url": style_data_uri, "custom": True}}
            }
        }
    }

    # 4. Khởi tạo Kepler.gl
    m = KeplerGl(height=800, config=map_config)

    # 5. Bơm Data
    m.add_data(data=df_spatial, name="MDT_Coverage_Live")
    if not df_audit.empty:
        m.add_data(data=df_audit, name="Quality_Audit_Report")

    # 6. Xuất ra file HTML
    output_file = f"/opt/airflow/dags/kepler_dashboard_{args.date}.html"
    try:
        m.save_to_html(file_name=output_file, read_only=False)
        print(f"✅ Đã lưu HTML gốc tại {output_file}")
        
        with open(output_file, "r", encoding="utf-8") as f:
            html_content = f.read()
        
        # =====================================================================
        # BỘ CODE INJECT GIAO DIỆN MỚI (CÓ NÚT THU GỌN VÀ TOGGLE TIME SLICER)
        # =====================================================================
        dashboard_injection = f"""
        <style>
            html, body {{
                width: 100% !important; height: 100vh !important; 
                margin: 0 !important; padding: 0 !important; overflow: hidden !important;
            }}
            /* Giao diện Panel Trắng */
            #insight-panel {{
                position: absolute; top: 20px; left: 20px; width: 440px; 
                background: #ffffff; border: 1px solid #e0e0e0;
                border-radius: 6px; box-shadow: 0 6px 16px rgba(0,0,0,0.15);
                color: #333; z-index: 9999;
                font-family: Arial, sans-serif;
            }}
            .panel-header {{
                display: flex; justify-content: space-between; align-items: center;
                padding: 15px 20px 10px 20px;
                border-bottom: 1px solid transparent;
            }}
            .panel-header.has-border {{ border-bottom: 1px solid #eee; }}
            .insight-title {{ 
                font-size: 18px; margin: 0; color: #cc0000; font-weight: bold; 
            }}
            #toggle-insight-btn {{
                background: none; border: none; font-size: 16px; cursor: pointer; color: #666;
            }}
            #toggle-insight-btn:hover {{ color: #000; }}
            
            #insight-content {{
                padding: 0 20px 20px 20px;
                max-height: 75vh; overflow-y: auto;
            }}
            
            .insight-status {{
                font-size: 13px; color: #555; margin-bottom: 10px; padding-bottom: 10px;
                border-bottom: 1px solid #eee;
            }}
            .filter-row {{
                display: flex; align-items: center; font-size: 13px; margin-bottom: 15px;
            }}
            .filter-row select {{
                margin-left: 10px; padding: 3px 5px; border: 1px solid #ccc; border-radius: 4px; outline: none;
            }}
            .chart-container {{ margin-bottom: 20px; position: relative; }}
            .chart-title {{ font-size: 13px; color: #555; margin-bottom: 5px; font-weight: bold; }}
            
            /* Nút Toggle Time Slicer */
            #toggle-slicer-btn {{
                position: absolute; bottom: 85px; right: 20px; z-index: 9999;
                background: #ffffff; border: 1px solid #ccc; padding: 8px 12px;
                border-radius: 4px; box-shadow: 0 4px 10px rgba(0,0,0,0.2);
                cursor: pointer; font-size: 13px; font-weight: bold; color: #333;
                font-family: Arial, sans-serif;
            }}
            #toggle-slicer-btn:hover {{ background: #f5f5f5; }}

            /* Dải Legend dưới cùng */
            #bottom-legend {{
                position: absolute; bottom: 30px; left: 50%; transform: translateX(-50%);
                display: flex; background: rgba(255,255,255,0.9); border-radius: 4px;
                box-shadow: 0 4px 10px rgba(0,0,0,0.2); z-index: 9998; font-family: Arial, sans-serif;
                overflow: hidden; border: 1px solid #ccc;
            }}
            .legend-item {{
                padding: 8px 20px; text-align: center; color: #000; font-size: 12px; font-weight: bold;
                border-right: 1px solid rgba(0,0,0,0.1); min-width: 70px;
            }}
            .legend-item:last-child {{ border-right: none; color: #fff; }}
        </style>
        
        <div id="insight-panel">
            <div class="panel-header" id="panel-header">
                <h3 class="insight-title">Trạm giám sát: Cụm Trung Tâm</h3>
                <button id="toggle-insight-btn" title="Thu gọn/Mở rộng">➖</button>
            </div>
            
            <div id="insight-content">
                <div class="insight-status">
                    Trạng thái hiện tại: <span style="color: #ff9900; font-weight: bold;">Cảnh báo Tải Mạng</span>
                </div>
                
                <div class="filter-row">
                    Lịch sử quan trắc: 
                    <select><option>24 giờ qua</option><option>7 ngày qua</option></select>
                </div>

                <div class="chart-title">Bản đồ tải định kỳ (Heatmap)</div>
                <div class="chart-container"><canvas id="chartHeatmap" height="150"></canvas></div>

                <div class="chart-title">Số lượng khu vực sóng yếu (< -110 dBm)</div>
                <div class="chart-container"><canvas id="chartPoor"></canvas></div>

                <div class="chart-title">Dự báo Mật độ User</div>
                <div class="chart-container"><canvas id="chartDensity"></canvas></div>
            </div>
        </div>

        <button id="toggle-slicer-btn">👁️ Ẩn/Hiện Time Slicer</button>

        <div id="bottom-legend">
            <div class="legend-item" style="background: #1a9850;">0-50<br>Rất Tốt</div>
            <div class="legend-item" style="background: #91cf60;">51-100<br>Tốt</div>
            <div class="legend-item" style="background: #d9ef8b;">101-150<br>Trung Bình</div>
            <div class="legend-item" style="background: #fee08b;">151-200<br>Kém</div>
            <div class="legend-item" style="background: #fc8d59; color: #fff;">201-300<br>Xấu</div>
            <div class="legend-item" style="background: #d73027; color: #fff;">301-500<br>Nguy Hại</div>
        </div>

        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chartjs-chart-matrix@2.0.1/dist/chartjs-chart-matrix.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.0.0"></script>
        
        <script>
            // JS ĐÓNG MỞ INSIGHT PANEL
            const insightContent = document.getElementById('insight-content');
            const toggleBtn = document.getElementById('toggle-insight-btn');
            const panelHeader = document.getElementById('panel-header');
            let isPanelOpen = true;

            toggleBtn.addEventListener('click', () => {{
                isPanelOpen = !isPanelOpen;
                if (isPanelOpen) {{
                    insightContent.style.display = 'block';
                    toggleBtn.innerText = '➖';
                    panelHeader.classList.add('has-border');
                }} else {{
                    insightContent.style.display = 'none';
                    toggleBtn.innerText = '➕';
                    panelHeader.classList.remove('has-border');
                }}
            }});

            // JS ĐÓNG MỞ TIME SLICER KELPER.GL
            const toggleSlicerBtn = document.getElementById('toggle-slicer-btn');
            toggleSlicerBtn.addEventListener('click', () => {{
                // Kepler tự render div có chứa class bottom-widget
                const timeWidget = document.querySelector('[class*="bottom-widget"]');
                if (timeWidget) {{
                    if (timeWidget.style.display === 'none') {{
                        timeWidget.style.display = '';
                    }} else {{
                        timeWidget.style.display = 'none';
                    }}
                }} else {{
                    alert('Không tìm thấy thanh Time Slicer trên giao diện!');
                }}
            }});

            // Kích hoạt DataLabels
            Chart.register(ChartDataLabels);

            const labels = {js_labels};
            const isForecast = {js_is_forecast};
            const pointStyles = isForecast.map(f => f ? 'triangle' : 'circle');
            const segmentStyles = ctx => ctx.p1DataIndex >= labels.length - 3 ? [5, 5] : undefined;

            const commonOptions = {{
                responsive: true,
                plugins: {{ 
                    legend: {{ display: false }},
                    datalabels: {{
                        display: function(context) {{
                            return context.dataset.data[context.dataIndex] > 0; 
                        }},
                        color: '#444', align: 'top', anchor: 'end',
                        font: {{ weight: 'bold', size: 10 }},
                        formatter: Math.round
                    }}
                }},
                scales: {{ 
                    x: {{ ticks: {{ color: '#666' }}, grid: {{ color: '#f0f0f0' }} }}, 
                    y: {{ ticks: {{ color: '#666' }}, grid: {{ color: '#f0f0f0' }} }} 
                }}
            }};

            // 1. BIỂU ĐỒ VIETTEL HEATMAP
            const heatmapData = {js_heatmap_data};
            const maxDensity = {max_density};
            new Chart(document.getElementById('chartHeatmap'), {{
                type: 'matrix',
                data: {{
                    datasets: [{{
                        label: 'Mật độ',
                        data: heatmapData,
                        backgroundColor: function(context) {{
                            if (!context.dataset.data[context.dataIndex]) return 'transparent';
                            const v = context.dataset.data[context.dataIndex].v;
                            if (v === 0) return '#f9f9f9'; 
                            const pct = v / maxDensity;
                            if (pct >= 0.8) return '#081d58'; 
                            if (pct >= 0.6) return '#225ea8'; 
                            if (pct >= 0.4) return '#41b6c4'; 
                            if (pct >= 0.2) return '#c7e9b4'; 
                            return '#ffffcc'; 
                        }},
                        borderColor: '#fff', borderWidth: 1,
                        width: ({{chart}}) => {{ const w = chart.chartArea ? chart.chartArea.width : 300; return w / 24 - 1; }},
                        height: ({{chart}}) => {{ const h = chart.chartArea ? chart.chartArea.height : 100; return h / 7 - 1; }}
                    }}]
                }},
                options: {{
                    maintainAspectRatio: false,
                    plugins: {{ legend: {{ display: false }}, datalabels: {{ display: false }} }},
                    scales: {{
                        x: {{ type: 'category', labels: {js_hours_labels}, ticks: {{ color: '#666', maxTicksLimit: 8 }} }},
                        y: {{ type: 'category', labels: {js_dow_labels}, ticks: {{ color: '#666' }} }}
                    }}
                }}
            }});

            // 2. BIỂU ĐỒ BAR CHART
            const poorCellsData = {js_poor_cells};
            new Chart(document.getElementById('chartPoor'), {{
                type: 'bar',
                data: {{
                    labels: labels,
                    datasets: [{{
                        data: poorCellsData,
                        backgroundColor: poorCellsData.map((val, idx) => {{
                            if (isForecast[idx]) return 'rgba(200, 200, 200, 0.5)'; 
                            if (val > 50) return '#d73027'; 
                            if (val > 20) return '#fc8d59'; 
                            if (val > 10) return '#fee08b'; 
                            return '#91cf60'; 
                        }})
                    }}]
                }},
                options: commonOptions
            }});

            // 3. BIỂU ĐỒ LINE CHART
            new Chart(document.getElementById('chartDensity'), {{
                type: 'line',
                data: {{
                    labels: labels,
                    datasets: [{{
                        label: 'User Density', data: {js_density},
                        borderColor: '#41b6c4', backgroundColor: 'rgba(65, 182, 196, 0.1)',
                        fill: true, pointStyle: pointStyles, tension: 0.3,
                        segment: {{ borderDash: segmentStyles }}
                    }}]
                }},
                options: commonOptions
            }});
        </script>
        """
        
        parts = html_content.rsplit("</body>", 1)
        if len(parts) == 2:
            html_content = f"{parts[0]}{dashboard_injection}</body>{parts[1]}"
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(html_content)
            print("✅ Đã tiêm thành công giao diện mới (Có Panel Thu Gọn + Tắt/Mở Time Slicer)!")
        else:
            print("❌ Lỗi: Không tìm thấy thẻ </body>.")
            
    except Exception as e:
        print(f"❌ Lỗi xử lý HTML: {str(e)}")

if __name__ == "__main__":
    main()