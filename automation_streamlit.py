import streamlit as st
import pandas as pd
from google.cloud import bigquery
from io import BytesIO
from datetime import datetime, date

# ✅ GCP 인증: secrets.toml에 저장된 정보 사용 (Cloud + 로컬 동일)
client = bigquery.Client.from_service_account_info(
    st.secrets["gcp_service_account"]
)

# ✅ 제목
st.title("📊 대학별 AI 서비스 이용 현황")
st.write("기관ID와 조회 기간을 선택 시, 월별 AI서비스 이용 현황 및 전년대비 데이터 확인 가능합니다.")

# ✅ B2B 기관 입력
b2b_id = st.text_input("B2B 기관ID 입력 (예: ICST00004103)", placeholder="기관ID를 입력하세요")

# ✅ 전체 월 라벨 생성: 2024-01 ~ 현재월
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

# ✅ ID와 기간 선택 구분선
st.markdown("---")

# ✅ 날짜 선택 안내 문구 추가
st.caption("📅 날짜 선택은 선택사항입니다. 미선택 시 **2024년 1월부터 조회 시점 달(당월)까지 전체 조회**됩니다.(단, 당월은 전일자까지 반영)")


# ✅ 연도/월 필터
selected_years = st.multiselect("[선택사항] 조회할 연도 선택", options=year_labels)
filtered_months = [m for m in month_labels if m.split("-")[0] in selected_years] if selected_years else month_labels
selected_months = st.multiselect("[선택사항] 조회할 월 선택", options=filtered_months)

# ✅ 검색 버튼
search_button = st.button("🔍 검색")

# ✅ 선택값이 없으면 전체 사용
if not selected_months:
    selected_months = filtered_months

# ✅ 실행 조건
if b2b_id and search_button:
    # (1) 기관명 조회
    b2b_name_query = """
        SELECT b2b_nm
        FROM `dbpia-project.nurisql.AI_ALL_AGG`
        WHERE b2b_id = @b2b_id
        LIMIT 1
    """
    b2b_name_job = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("b2b_id", "STRING", b2b_id)]
    )
    b2b_name_df = client.query(b2b_name_query, job_config=b2b_name_job).to_dataframe()
    b2b_nm = b2b_name_df["b2b_nm"].iloc[0] if not b2b_name_df.empty else b2b_id

    # (2) 메인 쿼리 실행
    query = """
        SELECT
          service_type,
          label AS month_label,
          SUM(used_sum) AS used,
          SUM(prev_year_used_sum) AS prev_used,
          SUM(session_sum) AS session,
          0 AS prev_session
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
        # ✅ 널 값 처리
        df["used"] = df["used"].fillna(0)
        df["prev_used"] = df["prev_used"].fillna(0)
        df["session"] = df["session"].fillna(0)
        df["prev_session"] = df["prev_session"].fillna(0)

        # ✅ 월 정렬
        sorted_months = sorted(selected_months)

        # ✅ [1] 이용 수 처리
        pivot_usage = df.pivot(index="service_type", columns="month_label", values="used").fillna(0).astype(int)
        pivot_usage_prev = df.pivot(index="service_type", columns="month_label", values="prev_used").fillna(0).astype(int)

        total_usage = pivot_usage.sum(axis=0)
        total_usage_prev = pivot_usage_prev.sum(axis=0)
        usage_change_rate = (total_usage / total_usage_prev.replace(0, pd.NA) - 1) * 100
        usage_change_rate = usage_change_rate.apply(lambda x: f"{round(x, 1)}%" if pd.notnull(x) else "-")

        pivot_usage.loc["서비스 전체"] = total_usage
        pivot_usage.loc["전년대비"] = usage_change_rate
        pivot_usage.index.name = "서비스 구분"
        pivot_usage = pivot_usage.reindex(columns=sorted_months)

        # ✅ [2] 세션 수 처리
        pivot_session = df.pivot(index="service_type", columns="month_label", values="session").fillna(0).astype(int)
        pivot_session_prev = df.pivot(index="service_type", columns="month_label", values="prev_session").fillna(0).astype(int)

        total_session = pivot_session.sum(axis=0)
        total_session_prev = pivot_session_prev.sum(axis=0)
        session_change_rate = (total_session / total_session_prev.replace(0, pd.NA) - 1) * 100
        session_change_rate = session_change_rate.apply(lambda x: f"{round(x, 1)}%" if pd.notnull(x) else "-")


        pivot_session.loc["서비스 전체"] = total_session
        pivot_session.loc["전년대비"] = session_change_rate
        pivot_session.index.name = "서비스 구분"
        pivot_session = pivot_session.reindex(columns=sorted_months)

        # ✅ [3] 결과 출력 - 이용 수
        st.subheader(f"📈 {b2b_nm} ({b2b_id}) 이용 현황")
        st.dataframe(pivot_usage.style.set_properties(**{"text-align": "center"}), use_container_width=True)

        # ✅ [4] 결과 출력 - 세션 수
        st.markdown("---")
        st.subheader(f"🧭 {b2b_nm} ({b2b_id}) 세션 수")
        st.dataframe(pivot_session.style.set_properties(**{"text-align": "center"}), use_container_width=True)

        # ✅ [5] 엑셀 다운로드
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pivot_usage.to_excel(writer, sheet_name="AI 이용 현황")
            pivot_session.to_excel(writer, sheet_name="AI 세션 수")
        output.seek(0)

        today_str = date.today().strftime("%Y%m%d")
        file_name = f"{b2b_nm}_{b2b_id}_AI이용현황_{today_str}.xlsx"

        st.download_button(
            label="📥 엑셀 다운로드",
            data=output,
            file_name=file_name,
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
