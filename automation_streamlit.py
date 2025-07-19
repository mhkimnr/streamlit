import streamlit as st
import pandas as pd
from google.cloud import bigquery
from io import BytesIO
from datetime import datetime, date, timedelta

# GCP 인증
client = bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

# 전체 월 라벨 생성
def generate_month_labels(start_year=2024):
    today = datetime.today()
    labels = []
    for year in range(start_year, today.year + 1):
        end_month = 12 if year < today.year else today.month
        for month in range(1, end_month + 1):
            labels.append(f"{year}-{month:02}")
    return labels

# ---------- 사이드바 ----------
st.sidebar.title("🔍 조회 모드 선택")
mode = st.sidebar.radio("조회 유형을 선택하세요", ["월별 조회", "일별 조회"])

# ---------- 공통 제목 ----------
st.title("📊 대학별 AI 서비스 이용 현황")
st.write("기관ID와 조회 기간을 선택 시, AI서비스 이용 현황을 확인 할 수 있습니다.")

# ---------- 공통: B2B 기관 ID 입력 ----------
b2b_id = st.text_input("대학교 B2B_ID 입력 (예: ICST00004103)", placeholder="기관ID를 입력하세요")

# ---------- 월별 조회 ----------
if mode == "월별 조회":
    st.markdown("## 📅 월별 조회")

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    current_month_str = f"{today.year}년 {today.month}월"
    yesterday_str = f"{yesterday.year}년 {yesterday.month}월 {yesterday.day}일"

    st.markdown(f"""
    <div style="font-size: 14px; color: gray;">
    ※ 조회 연도 및 월을 선택하지 않을 경우, 기본 조회 기간은 2024년 1월부터 현재 조회 시점 기준 월({current_month_str})까지입니다.<br>
    ※ 현재 월의 데이터는 조회 시점 전일({yesterday_str})까지 반영되어 조회됩니다.
    </div>
    """, unsafe_allow_html=True)

    month_labels = generate_month_labels()
    year_labels = sorted(set(label.split("-")[0] for label in month_labels))

    selected_years = st.multiselect("[선택사항] 조회 연도 선택", options=year_labels)
    filtered_months = [m for m in month_labels if m.split("-")[0] in selected_years] if selected_years else month_labels
    selected_months = st.multiselect("[선택사항] 조회 월 선택", options=filtered_months)

    if not selected_months:
        selected_months = filtered_months

    search_triggered = st.button("🔍 검색")

    if search_triggered and b2b_id:
        name_query = """
            SELECT b2b_nm FROM `dbpia-project.nurisql.AI_ALL_AGG`
            WHERE b2b_id = @b2b_id LIMIT 1
        """
        job = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("b2b_id", "STRING", b2b_id)])
        name_df = client.query(name_query, job_config=job).to_dataframe()
        b2b_nm = name_df["b2b_nm"].iloc[0] if not name_df.empty else b2b_id

        main_query = """
            SELECT service_type, label AS month_label,
                   SUM(used_sum) AS used,
                   SUM(prev_year_used_sum) AS prev_used,
                   SUM(session_sum) AS session,
                   0 AS prev_session
            FROM `dbpia-project.nurisql.AI_ALL_AGG`
            WHERE agg_unit = '월' AND b2b_id = @b2b_id AND label IN UNNEST(@months)
            GROUP BY service_type, month_label
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("b2b_id", "STRING", b2b_id),
            bigquery.ArrayQueryParameter("months", "STRING", selected_months)
        ])
        df = client.query(main_query, job_config=job_config).to_dataframe()

        if df.empty:
            st.warning("해당 조건에 대한 데이터가 없습니다.")
        else:
            for col in ["used", "prev_used", "session", "prev_session"]:
                df[col] = df[col].fillna(0)

            sorted_months = sorted(selected_months)
            service_order = ["AI IDEA", "AI Viewer", "AI Search"]

            def make_pivot(col_name):
                pivot = df.pivot(index="service_type", columns="month_label", values=col_name).fillna(0)
                pivot = pivot.round(0).astype(int)
                pivot.index.name = "구분"
                return pivot.reindex(service_order).reindex(columns=sorted_months)

            pivot_usage = make_pivot("used")
            pivot_prev = make_pivot("prev_used")
            total = pivot_usage.sum(numeric_only=True).fillna(0).astype(int)
            prev_total = pivot_prev.sum(numeric_only=True).fillna(0).astype(int)
            rate = (total / prev_total.replace(0, pd.NA) - 1) * 100
            rate = rate.apply(lambda x: f"{round(x,1)}%" if pd.notnull(x) else "-")

            pivot_usage.loc["서비스 전체"] = total
            pivot_usage.loc["전년대비"] = rate

            pivot_session = make_pivot("session")
            pivot_session_prev = make_pivot("prev_session")
            total_s = pivot_session.sum(numeric_only=True).fillna(0).astype(int)
            total_prev_s = pivot_session_prev.sum(numeric_only=True).fillna(0).astype(int)
            rate_s = (total_s / total_prev_s.replace(0, pd.NA) - 1) * 100
            rate_s = rate_s.apply(lambda x: f"{round(x,1)}%" if pd.notnull(x) else "-")

            pivot_session.loc["서비스 전체"] = total_s
            pivot_session.loc["전년대비"] = rate_s

            st.subheader(f"📈 {b2b_nm} ({b2b_id}) 이용 현황")
            st.dataframe(pivot_usage.style.set_properties(**{"text-align": "center"}), use_container_width=True)

            st.markdown("---")
            st.subheader(f"🧭 {b2b_nm} ({b2b_id}) 세션 수")
            st.dataframe(pivot_session.style.set_properties(**{"text-align": "center"}), use_container_width=True)

            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                pivot_usage.to_excel(writer, sheet_name="AI 이용 현황")
                pivot_session.to_excel(writer, sheet_name="AI 세션 수")
            output.seek(0)

            file_name = f"{b2b_nm}_{b2b_id}_AI월별이용현황_{date.today().strftime('%Y%m%d')}.xlsx"
            st.download_button("📥 엑셀 다운로드", data=output, file_name=file_name,
                               mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                               key="download_button")

# ---------- 일별 조회 ----------
elif mode == "일별 조회":
    st.markdown("## 📅 일별 조회")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("시작 날짜", value=date.today())
    with col2:
        end_date = st.date_input("종료 날짜(조회시점 전일까지 검색 가능)", value=date.today())

    if st.button("🔍 검색") and b2b_id:
        name_query = """
            SELECT b2b_nm FROM `dbpia-project.nurisql.AI_ALL_DYNAMIC`
            WHERE b2b_id = @b2b_id LIMIT 1
        """
        name_job = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("b2b_id", "STRING", b2b_id)
        ])
        name_df = client.query(name_query, job_config=name_job).to_dataframe()
        b2b_nm = name_df["b2b_nm"].iloc[0] if not name_df.empty else b2b_id

        start_label = start_date.strftime("%Y-%m-%d")
        end_label = end_date.strftime("%Y-%m-%d")

        query = """
            SELECT service_type, DATE AS date,
                   SUM(used_sum) AS used,
                   SUM(SESSION_CNT) AS session
            FROM `dbpia-project.nurisql.AI_ALL_DYNAMIC`
            WHERE B2B_ID = @b2b_id AND DATE BETWEEN @start AND @end
            GROUP BY service_type, DATE
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("b2b_id", "STRING", b2b_id),
            bigquery.ScalarQueryParameter("start", "STRING", start_label),
            bigquery.ScalarQueryParameter("end", "STRING", end_label)
        ])
        df = client.query(query, job_config=job_config).to_dataframe()

        if df.empty:
            st.warning("선택한 기간에 대한 데이터가 없습니다.")
        else:
            df.rename(columns={"DATE": "date"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

            df_used = df.pivot(index="service_type", columns="date", values="used").fillna(0).round(0).astype(int)
            df_session = df.pivot(index="service_type", columns="date", values="session").fillna(0).round(0).astype(int)

            rename_map = {
                "AI IDEA": "AI IDEA",
                "AI Viewer": "AI Viewer",
                "AI Search": "AI Search"
            }
            df_used.index = df_used.index.map(rename_map.get)
            df_session.index = df_session.index.map(rename_map.get)

            service_order = ["AI IDEA", "AI Viewer", "AI Search"]
            df_used = df_used.reindex(service_order).fillna(0)
            df_session = df_session.reindex(service_order).fillna(0)

            df_used.loc["서비스 전체"] = df_used.sum(numeric_only=True).fillna(0).astype(int)
            df_session.loc["서비스 전체"] = df_session.sum(numeric_only=True).fillna(0).astype(int)

            df_used.index.name = "구분"
            df_session.index.name = "구분"

            st.subheader(f"📊 {b2b_nm} ({b2b_id}) {start_label} ~ {end_label} 일자 이용 수")
            st.dataframe(df_used.style.set_properties(**{"text-align": "center"}), use_container_width=True)

            st.subheader(f"🧭 {b2b_nm} ({b2b_id}) {start_label} ~ {end_label} 일자 세션 수")
            st.dataframe(df_session.style.set_properties(**{"text-align": "center"}), use_container_width=True)

            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df_used.to_excel(writer, sheet_name="이용 수")
                df_session.to_excel(writer, sheet_name="세션 수")
            output.seek(0)

            file_name = f"{b2b_nm}_{b2b_id}_AI일별이용현황_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"
            st.download_button("📥 엑셀 다운로드", data=output, file_name=file_name,
                               mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
