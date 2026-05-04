import streamlit as st

st.set_page_config(
    page_title="CarMax Chat Analysis Dashboard",
    page_icon=":material/person:",
    layout="wide",
)

nonstarters_page = st.Page("pages/1_Nonstarters.py", title="Nonstarters", icon=":material/monitoring:")
budget_technique_page = st.Page("pages/2_Budget_Technique.py", title="Budget Techqnique", icon=":material/payments:")

pg = st.navigation([nonstarters_page, budget_technique_page])
pg.run()
