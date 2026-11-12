import os
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Stack Overflow - tendances", layout="wide")
OPTS = {
    "key": os.getenv("MINIO_USER", "minioadmin"),
    "secret": os.getenv("MINIO_PASSWORD", "minioadmin"),
    "client_kwargs": {"endpoint_url": os.getenv("MINIO_ENDPOINT", "http://minio:9000")},
}


@st.cache_data(ttl=30)
def load(name):
    try:
        return pd.read_parquet(f"s3://gold/{name}/", storage_options=OPTS)
    except Exception:
        return None


st.title("Analyse des questions Stack Overflow")
top, trends, metrics = load("top_tags"), load("tag_trends"), load("model_metrics")

if top is None or trends is None:
    st.warning("Pas encore de donnees Gold. Lancez le DAG Airflow 'stackoverflow_monthly' puis rechargez.")
    st.stop()

c1, c2, c3 = st.columns(3)
c1.metric("Questions (tag x occurrences)", int(top["nb_questions"].sum()))
c2.metric("Tags distincts", len(top))
c3.metric("Mois couverts", trends["year_month"].nunique())

st.subheader("Top 20 des tags")
st.bar_chart(top.sort_values("nb_questions", ascending=False).head(20).set_index("tag")["nb_questions"])

st.subheader("Volume par tag et par mois")
default = list(top.sort_values("nb_questions", ascending=False).head(5)["tag"])
sel = st.multiselect("Tags", sorted(top["tag"]), default=default)
if sel:
    pv = (trends[trends["tag"].isin(sel)]
          .pivot_table(index="year_month", columns="tag", values="nb_questions", aggfunc="sum")
          .fillna(0).sort_index())
    st.line_chart(pv)

st.subheader("Qualite du modele de prediction des tags")
if metrics is None:
    st.info("Metriques du modele pas encore disponibles.")
else:
    st.dataframe(metrics.sort_values("auc", ascending=False), use_container_width=True)
