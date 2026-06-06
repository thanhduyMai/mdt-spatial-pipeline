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
    
    try:
        df_spatial = pd.read_sql_query(query_spatial, conn, params=(args.date,))
        # Chuyển đổi tường minh kiểu datetime để Kepler nhận diện thanh trượt dễ hơn
        df_spatial['record_time'] = pd.to_datetime(df_spatial['record_time'])
        
        # Đọc thêm bảng Audit
        try:
            df_audit = pd.read_sql_query(query_audit, conn, params=(args.date,))
        except:
            df_audit = pd.DataFrame() # Nếu bảng chưa có thì bỏ qua
            
    except Exception as e:
        print(f"❌ Lỗi truy vấn dữ liệu SQL: {e}")
        conn.close()
        sys.exit(1)
        
    conn.close()

    if df_spatial.empty:
        print(f"⚠️ Không có dữ liệu không gian cho ngày {args.date}. Dừng tạo bản đồ.")
        sys.exit(0)

    print(f"📊 Đã tải {len(df_spatial)} bản ghi không gian và {len(df_audit)} bản ghi Audit. Đang dựng siêu bản đồ 3D Kepler...")

    # --- BẮT ĐẦU CẤU HÌNH BẢN ĐỒ VÀ WIDGET ---
    esri_style = {
        "version": 8,
        "sources": {
            "esri-satellite": {
                "type": "raster",
                "tiles": ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"],
                "tileSize": 256
            }
        },
        "layers": [
            {
                "id": "satellite-layer",
                "type": "raster",
                "source": "esri-satellite",
                "minzoom": 0, "maxzoom": 22
            }
        ]
    }
    style_json_str = json.dumps(esri_style)
    b64_style = base64.b64encode(style_json_str.encode("utf-8")).decode("utf-8")
    style_data_uri = f"data:application/json;base64,{b64_style}"

    map_config = {
        "version": "v1",
        "config": {
            # Bật VisState để cấu hình Lớp hiển thị và Thanh trượt thời gian
            "visState": {
                "filters": [
                    {
                        "dataId": ["MDT_Coverage_Live"],
                        "id": "time-filter",
                        "name": ["record_time"],
                        "type": "timeRange",
                        "enlarged": True # Bật thanh trượt bự ở dưới đáy màn hình
                    }
                ],
                "layers": [
                    # Lớp vẽ tia sóng (Arc Layer) từ trạm đến người dùng
                    {
                        "id": "arc-signal-layer",
                        "type": "arc",
                        "config": {
                            "dataId": "MDT_Coverage_Live",
                            "label": "Tia sóng Trạm - H3",
                            "color": [248, 149, 112],
                            "columns": {
                                "lat0": "cell_lat",
                                "lng0": "cell_lon",
                                "lat1": "h3_center_lat",
                                "lng1": "h3_center_lon"
                            },
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.8,
                                "thickness": 2,
                                "colorRange": {
                                    "name": "Global Warming",
                                    "type": "sequential",
                                    "category": "Uber",
                                    "colors": ["#5A1846", "#900C3F", "#C70039", "#E3611C", "#F1920E", "#FFC300"]
                                },
                                "targetColor": [255, 203, 153]
                            }
                        }
                    }
                ]
            },
            "mapState": {
                "latitude": 21.5613,
                "longitude": 105.8760,
                "zoom": 10,
                "pitch": 45,
                "bearing": 0
            },
            "mapStyle": {
                "styleType": "esri_satellite",
                "mapStyles": {
                    "esri_satellite": {
                        "id": "esri_satellite",
                        "label": "ESRI Satellite",
                        "url": style_data_uri,
                        "custom": True,
                        "icon": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/1/0/0"
                    }
                }
            }
        }
    }
    # --- KẾT THÚC CẤU HÌNH ---

    # 4. Khởi tạo Kepler.gl
    m = KeplerGl(height=800, config=map_config)

    # 5. Bơm Data vào bản đồ
    m.add_data(data=df_spatial, name="MDT_Coverage_Live")
    
    # Bơm thêm Quality Audit Dashboard nếu có dữ liệu
    if not df_audit.empty:
        m.add_data(data=df_audit, name="Quality_Audit_Report")

    # 6. Xuất ra file HTML
    output_file = f"/opt/airflow/dags/kepler_dashboard_{args.date}.html"
    try:
        m.save_to_html(file_name=output_file)
        print(f"✅ Đã tạo xong siêu bản đồ 3D + Audit Dashboard! File lưu tại: {output_file}")
    except Exception as e:
        print(f"❌ Lỗi khi lưu file HTML: {e}")

if __name__ == "__main__":
    main()