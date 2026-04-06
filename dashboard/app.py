"""
CrimeNet — CDR Forensics Dashboard
====================================
Streamlit interactive dashboard for CDR analysis.

Run with:
    streamlit run dashboard/app.py
"""

import sys
import os
import tempfile

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyvis.network import Network

# Ensure project root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from analysis.ingest import load_cdr, get_summary
from analysis.network import build_graph, compute_centrality, detect_communities, simulate_dismantling, get_graph_stats
from analysis.geo import get_tower_locations, get_movement_trajectory, detect_colocation, build_map
from analysis.temporal import hourly_activity, weekday_heatmap, night_call_ratio, detect_bursts, per_number_profile
from analysis.anomaly import build_features, score_anomalies, detect_burner_phones, one_way_communication


# ──────────────────────────────────────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="CrimeNet — CDR Forensics",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ──────────────────────────────────────────────────────────────────────────────
# Sidebar — Data Upload
# ──────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## CrimeNet")
    st.markdown("**CDR Forensics Platform**")
    # st.markdown("*C3iHub, IIT Kanpur*")
    st.divider()

    uploaded = st.file_uploader("Upload CDR CSV", type=["csv"])

    use_sample = False
    sample_path = os.path.join(os.path.dirname(__file__), "..", "data", "sample")
    sample_files = []
    if os.path.exists(sample_path):
        sample_files = [f for f in os.listdir(sample_path) if f.endswith(".csv")]

    if sample_files:
        selected_sample = st.selectbox("Or use a sample dataset", ["— Select —"] + sample_files)
        if selected_sample != "— Select —":
            use_sample = True

    colocation_window = st.slider("Co-location window (minutes)", 10, 120, 30, 5)
    contamination = st.slider("Anomaly sensitivity", 0.05, 0.40, 0.15, 0.05,
                              help="Fraction of records expected to be anomalous")

    st.divider()
    st.markdown("**Generate synthetic data:**")
    st.code("python utils/cdr_generator.py\n  --scenario gang\n  --records 500\n  --state KA", language="bash")


# ──────────────────────────────────────────────────────────────────────────────
# Load data
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner="Loading CDR data...")
def load_data(source):
    return load_cdr(source)


df = None
if uploaded:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(uploaded.read())
        tmp_path = tmp.name
    df = load_data(tmp_path)
elif use_sample and selected_sample != "— Select —":
    df = load_data(os.path.join(sample_path, selected_sample))


# ──────────────────────────────────────────────────────────────────────────────
# No data state
# ──────────────────────────────────────────────────────────────────────────────

if df is None:
    st.title("CrimeNet — CDR Forensics Platform")
    st.markdown(
        """
        Upload a CDR CSV file or select a sample dataset from the sidebar to begin analysis.

        **Expected CSV columns:**
        `Calling_Number, Called_Number, Date, Time, Duration_sec, Call_Type`

        Optional: `Caller_IMEI, Caller_IMSI, Tower_ID, Tower_Location, Tower_Latitude, Tower_Longitude`

        ---
        Generate synthetic data to explore:
        ```bash
        python utils/cdr_generator.py --scenario gang --records 500 --state KA \\
          --output data/sample/gang_demo.csv
        ```
        """
    )
    st.stop()


# ──────────────────────────────────────────────────────────────────────────────
# Pre-compute analysis (cached)
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner="Building network graph...")
def cached_network(data_hash):
    G = build_graph(df)
    centrality = compute_centrality(G)
    communities = detect_communities(G)
    stats = get_graph_stats(G)
    dismantling = simulate_dismantling(G, top_n=5)
    return G, centrality, communities, stats, dismantling

@st.cache_data(show_spinner="Running anomaly detection...")
def cached_anomaly(data_hash, cont):
    features = build_features(df)
    scored = score_anomalies(features, contamination=cont)
    burners = detect_burner_phones(df)
    one_way = one_way_communication(df)
    return scored, burners, one_way

@st.cache_data(show_spinner="Analyzing geospatial data...")
def cached_geo(data_hash, window):
    colocation = detect_colocation(df, window_minutes=window)
    return colocation

data_hash = str(len(df)) + str(df["Datetime"].max())

summary = get_summary(df)
G, centrality_df, communities, graph_stats, dismantling = cached_network(data_hash)
anomaly_df, burner_df, one_way_df = cached_anomaly(data_hash, contamination)
colocation_df = cached_geo(data_hash, colocation_window)

# Attach community labels to centrality
if communities:
    centrality_df["community"] = centrality_df["phone_number"].map(communities)


# ──────────────────────────────────────────────────────────────────────────────
# Tabs
# ──────────────────────────────────────────────────────────────────────────────

tab_overview, tab_network, tab_geo, tab_behavioral, tab_report = st.tabs([
    "Overview", "Network Analysis", "Geographic Map", "Behavioral Analysis", "Investigation Report"
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════

with tab_overview:
    st.header("Dataset Overview")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Records", f"{summary['total_records']:,}")
    col2.metric("Unique Numbers", summary["unique_numbers"])
    col3.metric("Cell Towers", summary.get("unique_towers", "N/A"))
    col4.metric("Date Range", f"{summary['date_range_start']} – {summary['date_range_end']}")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Network Nodes", graph_stats["nodes"])
    col6.metric("Network Edges", graph_stats["edges"])
    col7.metric("Anomalies Flagged", int(anomaly_df["is_anomaly"].sum()) if anomaly_df is not None else 0)
    col8.metric("Burner Suspects", len(burner_df) if burner_df is not None and not burner_df.empty else 0)

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Hourly Call Activity")
        hourly = hourly_activity(df)
        fig = px.bar(
            x=hourly.index, y=hourly.values,
            labels={"x": "Hour of Day", "y": "Call Count"},
            color=hourly.values, color_continuous_scale="Blues",
        )
        fig.update_layout(coloraxis_showscale=False, margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Call Type Distribution")
        call_types = summary.get("call_types", {})
        if call_types:
            fig = px.pie(
                names=list(call_types.keys()),
                values=list(call_types.values()),
                color_discrete_sequence=px.colors.qualitative.Set2,
                hole=0.4,
            )
            fig.update_layout(margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Weekday × Hour Heatmap")
    heatmap_df = weekday_heatmap(df)
    fig = px.imshow(
        heatmap_df.values,
        x=[str(h) for h in range(24)],
        y=heatmap_df.index.tolist(),
        color_continuous_scale="YlOrRd",
        labels={"x": "Hour", "y": "Day", "color": "Calls"},
    )
    fig.update_layout(margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: NETWORK ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

with tab_network:
    st.header("Network / Link Analysis")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Nodes", graph_stats["nodes"])
    c2.metric("Edges", graph_stats["edges"])
    c3.metric("Density", graph_stats["density"])
    c4.metric("Communities", max(communities.values()) + 1 if communities else "N/A")

    st.divider()

    col_graph, col_table = st.columns([3, 2])

    with col_graph:
        st.subheader("Interactive Communication Network")
        net = Network(height="500px", width="100%", bgcolor="#0f172a", font_color="white", directed=True)
        net.barnes_hut()

        community_colors = [
            "#3b82f6", "#ef4444", "#22c55e", "#f59e0b", "#a855f7",
            "#06b6d4", "#f97316", "#84cc16", "#ec4899", "#14b8a6",
        ]

        for node in G.nodes():
            community_id = communities.get(node, 0)
            color = community_colors[community_id % len(community_colors)]
            # Size node by total call weight
            out_w = sum(d["weight"] for _, _, d in G.out_edges(node, data=True))
            net.add_node(node, label=node[-4:], title=node, color=color, size=8 + out_w * 0.5)

        for src, dst, data in G.edges(data=True):
            net.add_edge(src, dst, value=data["weight"], title=f"Calls: {data['weight']}")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".html", mode="w") as tmp_html:
            net.save_graph(tmp_html.name)
            html_content = open(tmp_html.name).read()

        st.components.v1.html(html_content, height=520, scrolling=False)

    with col_table:
        st.subheader("Centrality Rankings")
        display_cols = ["phone_number", "betweenness_centrality", "pagerank", "total_calls"]
        if "community" in centrality_df.columns:
            display_cols.append("community")
        st.dataframe(
            centrality_df[display_cols].head(15),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()
    st.subheader("Network Dismantling Simulation")
    st.markdown(
        "Simulates which arrests would most effectively fragment the criminal network. "
        "Each step removes the highest-betweenness node and shows the impact on connectivity."
    )
    if dismantling:
        dis_df = pd.DataFrame(dismantling)
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            fig = px.line(
                dis_df, x="step", y="network_efficiency",
                markers=True, title="Network Efficiency After Each Arrest",
                labels={"step": "Arrest Step", "network_efficiency": "Network Efficiency"},
            )
            fig.update_traces(line_color="#ef4444")
            st.plotly_chart(fig, use_container_width=True)
        with col_d2:
            st.dataframe(
                dis_df[["step", "removed_node", "remaining_nodes", "largest_component_size", "network_efficiency"]],
                use_container_width=True, hide_index=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3: GEOGRAPHIC MAP
# ══════════════════════════════════════════════════════════════════════════════

with tab_geo:
    st.header("Geographic Communication Mapping")

    if "Tower_Latitude" not in df.columns:
        st.warning("Tower GPS data not available in this CDR dataset.")
    else:
        all_numbers = sorted(pd.concat([df["Calling_Number"], df["Called_Number"]]).unique())
        track_number = st.selectbox("Track movement for number:", ["— None —"] + list(all_numbers))

        traj_df = None
        if track_number != "— None —":
            traj_df = get_movement_trajectory(df, track_number)

        folium_map = build_map(
            df,
            colocation_df=colocation_df if not colocation_df.empty else None,
            track_number=track_number if track_number != "— None —" else None,
            trajectory_df=traj_df,
        )

        import streamlit.components.v1 as components
        with tempfile.NamedTemporaryFile(delete=False, suffix=".html", mode="w") as tmp_map:
            folium_map.save(tmp_map.name)
            map_html = open(tmp_map.name).read()
        components.html(map_html, height=520)

        if not colocation_df.empty:
            st.subheader(f"Co-location Events (window: {colocation_window} min)")
            st.dataframe(colocation_df.head(20), use_container_width=True, hide_index=True)
        else:
            st.info("No co-location events detected within the configured time window.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4: BEHAVIORAL ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

with tab_behavioral:
    st.header("Behavioral Analysis & Anomaly Detection")

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Anomaly Scores (Top 20)")
        if anomaly_df is not None and len(anomaly_df) > 0:
            fig = px.bar(
                anomaly_df.head(20),
                x="anomaly_rank",
                y="phone_number",
                orientation="h",
                color="is_anomaly",
                color_discrete_map={True: "#ef4444", False: "#3b82f6"},
                labels={"anomaly_rank": "Anomaly Score (1=most)", "phone_number": ""},
            )
            fig.update_layout(margin=dict(t=10), showlegend=True)
            st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Night Call Ratio")
        night_df = night_call_ratio(df).head(15)
        fig = px.bar(
            night_df,
            x="night_ratio",
            y="phone_number",
            orientation="h",
            color="night_ratio",
            color_continuous_scale="Reds",
            labels={"night_ratio": "Night Call Fraction", "phone_number": ""},
        )
        fig.update_layout(coloraxis_showscale=False, margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    col_c, col_d = st.columns(2)

    with col_c:
        st.subheader("Burner Phone Suspects")
        if burner_df is not None and not burner_df.empty:
            st.dataframe(burner_df, use_container_width=True, hide_index=True)
        else:
            st.success("No burner phone indicators detected.")

    with col_d:
        st.subheader("One-Way Communication")
        st.markdown("Numbers that always initiate but never receive a response — possible command-and-control.")
        if not one_way_df.empty:
            st.dataframe(one_way_df.head(15), use_container_width=True, hide_index=True)
        else:
            st.info("No strongly one-directional pairs detected.")

    st.divider()
    st.subheader("Full Anomaly Score Table")
    if anomaly_df is not None:
        st.dataframe(
            anomaly_df[["phone_number", "anomaly_rank", "is_anomaly", "call_count_out", "call_count_in",
                         "night_ratio", "short_call_ratio", "unique_contacts"]],
            use_container_width=True,
            hide_index=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5: INVESTIGATION REPORT
# ══════════════════════════════════════════════════════════════════════════════

with tab_report:
    st.header("Investigation Report")
    st.markdown(
        "Generate a structured PDF report with all findings — suitable for investigator review."
    )

    if st.button("Generate PDF Report", type="primary"):
        try:
            from reports.report_generator import generate_report
            pdf_bytes = generate_report(
                summary=summary,
                centrality_df=centrality_df,
                anomaly_df=anomaly_df,
                burner_df=burner_df if not burner_df.empty else None,
                colocation_df=colocation_df if not colocation_df.empty else None,
                dismantling=dismantling,
            )
            st.download_button(
                label="Download PDF",
                data=pdf_bytes,
                file_name=f"crimenet_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                mime="application/pdf",
            )
            st.success("Report generated successfully.")
        except ImportError:
            st.error("reportlab not installed. Run: pip install reportlab")

    st.divider()
    st.subheader("Summary Table")
    summary_display = {k: str(v) for k, v in summary.items()}
    st.json(summary_display)
