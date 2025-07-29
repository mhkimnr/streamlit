import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# GCP 인증
client = bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

# ---------- 쿼리 파라미터 ----------
query_params = st.query_params
is_print_mode = query_params.get("print_mode", "0") == "1"
b2b_id_param = query_params.get("b2b_id", None)

# ---------- CSS ----------
st.markdown("""
<style>
    @media print {
        header, footer, #search-wrapper, #pdf-tip, #print-button {
            display: none !important;
        }
        body * {
            visibility: hidden !important;
        }
        #report-section, #report-section * {
            visibility: visible !important;
        }
        #report-section {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
        }
    }
</style>
""", unsafe_allow_html=True)

# ---------- 타이틀 ----------
st.markdown("## 🎓 대학 리포트 조회")

# ---------- 검색 입력 (인쇄 모드에서는 숨김) ----------
b2b_id = ""
search_clicked = False
if not is_print_mode:
    st.markdown('<div id="search-wrapper">', unsafe_allow_html=True)
    col1, col2 = st.columns([4, 1])
    with col1:
        b2b_id = st.text_input("대학교 B2B_ID 입력 (예: ICST00004103)", placeholder="기관ID를 입력하세요", label_visibility="collapsed")
    with col2:
        search_clicked = st.button("🔍 검색", key="search_btn")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown(":arrow_down: 아래에서 리포트가 생성됩니다. 페이지를 내려 확인하세요.")
else:
    b2b_id = b2b_id_param  # URL에서 받은 B2B_ID 사용

# ---------- 리포트 출력 조건 ----------
if (search_clicked and b2b_id) or (is_print_mode and b2b_id):
    st.markdown("<div id='report-section'>", unsafe_allow_html=True)

    # 기관명 조회
    name_query = """
        SELECT b2b_nm FROM `dbpia-project.nurisql.AI_ALL_AGG`
        WHERE b2b_id = @b2b_id LIMIT 1
    """
    name_job = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("b2b_id", "STRING", b2b_id)
    ])
    name_df = client.query(name_query, job_config=name_job).to_dataframe()
    b2b_nm = name_df["b2b_nm"].iloc[0] if not name_df.empty else b2b_id

    st.title(f"📊 {b2b_nm} ({b2b_id}) 리포트")

    # ---------- 누적 이용 수 ----------
    st.header("🖌 누적 이용 수 (2025.01.01 ~ 현재)")
    month_labels = [f"{y}-{m:02}" for y in range(2025, datetime.today().year + 1)
                    for m in range(1, 13)
                    if not (y == datetime.today().year and m > datetime.today().month)]

    cumulative_query = """
        SELECT service_type, SUM(used_sum) AS total_used
        FROM `dbpia-project.nurisql.AI_ALL_AGG`
        WHERE agg_unit = '월' AND b2b_id = @b2b_id AND label IN UNNEST(@months)
        GROUP BY service_type
    """
    cumulative_job = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("b2b_id", "STRING", b2b_id),
        bigquery.ArrayQueryParameter("months", "STRING", month_labels)
    ])
    cumulative_df = client.query(cumulative_query, job_config=cumulative_job).to_dataframe()

    if cumulative_df.empty:
        st.warning("누적 이용 수 데이터가 없습니다.")
    else:
        cumulative_df = cumulative_df.set_index("service_type").reindex(["AI IDEA", "AI Viewer", "AI Search"])
        idea, viewer, search = cumulative_df["total_used"].fillna(0).astype(int).tolist()

        c1, c2, c3 = st.columns(3)
        c1.metric("🧐 AI IDEA", f"{idea:,}")
        c2.metric("👀 AI Viewer", f"{viewer:,}")
        c3.metric("🔍 AI Search", f"{search:,}")

    # ---------- 월별 이용 수 ----------
    st.header("🗓 월별 이용 수 (2025년 기준)")
    monthly_query = """
        SELECT service_type, label AS month, SUM(used_sum) AS used
        FROM `dbpia-project.nurisql.AI_ALL_AGG`
        WHERE agg_unit = '월' AND b2b_id = @b2b_id AND label IN UNNEST(@months)
        GROUP BY service_type, month
    """
    monthly_job = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("b2b_id", "STRING", b2b_id),
        bigquery.ArrayQueryParameter("months", "STRING", month_labels)
    ])
    monthly_df = client.query(monthly_query, job_config=monthly_job).to_dataframe()
    if monthly_df.empty:
        st.warning("월별 이용 수 데이터가 없습니다.")
    else:
        pivot_df = monthly_df.pivot(index="service_type", columns="month", values="used").fillna(0)
        pivot_df = pivot_df.round(0).astype(int)
        pivot_df = pivot_df.reindex(["AI IDEA", "AI Viewer", "AI Search"])
        st.dataframe(pivot_df.style.set_properties(**{"text-align": "center"}), use_container_width=True)

    # ---------- 워드클라우드 ----------
    st.header("💬 Search 질문 워드클라우드 (예시)")
    dummy_questions = [
        "논문 요약 알려줘", "연구 목적이 무엇이야?", "참고문헌 자동으로 생성돼?",
        "자연어처리 최신 연구 알려줘", "논문 구조 설명해줘", "요약 정리해줘",
        "AI 활용 사례는?", "결론은 무엇인가요?", "논문 키워드 추천해줘",
        "서론 요약", "학술적 의의는?", "연구 방법론 설명해줘",
        "검색어 관련 논문 있어?", "연구 배경 요약해줘", "요약해줘"
    ]
    tokens = []
    for q in dummy_questions:
        tokens.extend(q.split())

    counter = Counter(tokens)
    wc = WordCloud(width=800, height=400, background_color='white', font_path="/System/Library/Fonts/AppleGothic.ttf")
    wc.generate_from_frequencies(counter)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wc, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig)

    st.markdown("</div>", unsafe_allow_html=True)

    # 프린트 안내 또는 링크 생성 버튼
    if is_print_mode:
        st.info("📄 이 화면에서 Ctrl + P 또는 Cmd + P를 눌러 PDF로 저장하세요.")
    elif b2b_id:
        base_url = "https://dbpia-report.streamlit.app"
        print_url = f"{base_url}?print_mode=1&b2b_id={b2b_id}"

        st.markdown("<div id='pdf-tip'>", unsafe_allow_html=True)
        st.success("📄 리포트를 PDF로 저장하려면 아래 버튼을 클릭하세요.")
        st.markdown(f"""
            <a href="{print_url}" target="_blank">
                <button id="print-button" style="padding:10px 20px;font-size:16px;background-color:#ff4b4b;color:white;border:none;border-radius:8px;cursor:pointer;">
                    🖨️ 인쇄용 화면 열기
                </button>
            </a>
        """, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
