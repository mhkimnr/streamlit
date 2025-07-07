import os
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from io import BytesIO
from datetime import datetime

# ✅ GCP 인증
if os.environ.get("STREAMLIT_CLOUD") == "1":
    client = bigquery.Client.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
else:
    client = bigquery.Client.from_service_account_json(
        "C:/gcp_auth/dbpia-project-9c4650c54077.json",
        project="dbpia-project"
    )

# ✅ 제목
st.title("📊 대학별 AI 서비스 이용 현황")
st.write("B2B 기관 ID와 조회 기간을 선택하면 월별 서비스 이용 현황 및 전년대비 결과를 보여줍니다.")

# ✅ B2B 기관 입력 및 버튼
b2b_id = st.text_input("B2B 기관 ID 입력 (예: ICST00004103)", placeholder="기관 ID를 입력하세요")
search_button = st.button("🔍 검색")

# ✅ 전체 label 생성: 2024-01 ~ 현재월
def generate_month_labels(start_year=2024):
    today = datetime.today()
    labels = []
    for year in range(start_year, today.year + 1):
        end_month = 12 if year < today.year else today.month
        for month in range(1, end_month + 1):
            labels.append(f"{year}-{month:02}")
    return labels

month_labels = generate_month_labels()
year_labels = sorted(set(label.split("-")[0] for label in month_labels))

# ✅ 연도 필터
selected_years = st.multiselect("조회할 연도 선택 (선택하지 않으면 전체)", options=year_labels)

# ✅ 월 필터 (연도 선택에 따라 필터링)
filtered_months = [m for m in month_labels if m.split("-")[0] in selected_years] if selected_years else month_labels
selected_months = st.multiselect("조회할 월 선택 (선택하지 않으면 전체)", options=filtered_months)

# ✅ 선택값이 없으면 전체 사용
if not selected_months:
    selected_months = filtered_months

# ✅ 실행 조건
if b2b_id and search_button:
    query = """
        SELECT
          service_type,
          label AS month_label,
          SUM(used_sum) AS used,
          SUM(prev_year_used_sum) AS prev_used
        FROM `dbpia-project.nurisql.AI_ALL_AGG`
        WHERE agg_unit = '월'
          AND b2b_id = @b2b_id
          AND label IN UNNEST(@months)
        GROUP BY service_type, month_label
        ORDER BY service_type, month_label
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("b2b_id", "STRING", b2b_id),
            bigquery.ArrayQueryParameter("months", "STRING", selected_months)
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()

    if df.empty:
        st.warning("해당 조건에 대한 데이터가 없습니다.")
    else:
        df["prev_used"] = df["prev_used"].fillna(0)

        # Pivot
        pivot_used = df.pivot(index="service_type", columns="month_label", values="used").fillna(0).astype(int)
        pivot_prev = df.pivot(index="service_type", columns="month_label", values="prev_used").fillna(0).astype(int)

        # ✅ 정렬: month_label 오름차순
        sorted_months = sorted(selected_months)

        # 전체 합계 및 전년대비
        total_used = pivot_used.sum(axis=0)
        total_prev = pivot_prev.sum(axis=0)
        change_rate = (total_used / total_prev.replace(0, pd.NA) - 1) * 100
        change_rate = change_rate.apply(lambda x: f"{round(x, 1)}%" if pd.notnull(x) else "-")

        # 행 추가 및 정렬
        pivot_used.loc["서비스 전체"] = total_used
        pivot_used.loc["전년대비"] = change_rate

        # 컬럼명 변경
        pivot_used.index.name = "서비스 구분"
        pivot_used = pivot_used.reindex(columns=sorted_months)

        # ✅ 결과 출력: 넓은 너비 제공
        st.subheader(f"📈 기관 ID: `{b2b_id}` 이용 현황")
        st.dataframe(
            pivot_used.style.set_properties(**{"text-align": "center"}),
            use_container_width=True
        )

        # ✅ 엑셀 다운로드
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pivot_used.to_excel(writer, sheet_name="AI 이용 현황")
        output.seek(0)

        st.download_button(
            label="📥 엑셀 다운로드",
            data=output,
            file_name=f"{b2b_id}_AI_이용현황.xlsx",
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
