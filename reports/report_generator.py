"""
PDF Investigation Report Generator
=====================================
Generates a structured, court-friendly PDF investigation summary
from CrimeNet analysis results.
"""

import io
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
)


# ──────────────────────────────────────────────────────────────────────────────
# Style constants
# ──────────────────────────────────────────────────────────────────────────────

BRAND_BLUE = colors.HexColor("#1e3a5f")
ACCENT_RED = colors.HexColor("#c0392b")
LIGHT_GRAY = colors.HexColor("#f2f2f2")
MID_GRAY = colors.HexColor("#7f8c8d")
WHITE = colors.white


def _styles():
    base = getSampleStyleSheet()
    custom = {
        "title": ParagraphStyle(
            "title",
            parent=base["Title"],
            fontSize=20,
            textColor=BRAND_BLUE,
            spaceAfter=4,
        ),
        "subtitle": ParagraphStyle(
            "subtitle",
            parent=base["Normal"],
            fontSize=11,
            textColor=MID_GRAY,
            spaceAfter=12,
        ),
        "section": ParagraphStyle(
            "section",
            parent=base["Heading2"],
            fontSize=13,
            textColor=BRAND_BLUE,
            spaceBefore=16,
            spaceAfter=6,
            borderPad=2,
        ),
        "body": ParagraphStyle(
            "body",
            parent=base["Normal"],
            fontSize=9,
            leading=14,
        ),
        "flag": ParagraphStyle(
            "flag",
            parent=base["Normal"],
            fontSize=9,
            textColor=ACCENT_RED,
            leading=13,
        ),
    }
    return custom


def _table_style(header_color=BRAND_BLUE):
    return TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), header_color),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GRAY]),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cccccc")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ])


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def generate_report(
    summary: dict,
    centrality_df,
    anomaly_df,
    burner_df,
    colocation_df,
    dismantling: list,
    output_path: str = None,
) -> bytes:
    """
    Generate a PDF investigation report.

    Parameters
    ----------
    summary : dict
        Output of analysis.ingest.get_summary().
    centrality_df : pd.DataFrame
        Output of analysis.network.compute_centrality().
    anomaly_df : pd.DataFrame
        Output of analysis.anomaly.score_anomalies().
    burner_df : pd.DataFrame
        Output of analysis.anomaly.detect_burner_phones().
    colocation_df : pd.DataFrame
        Output of analysis.geo.detect_colocation().
    dismantling : list of dict
        Output of analysis.network.simulate_dismantling().
    output_path : str, optional
        If provided, also writes the PDF to this file path.

    Returns
    -------
    bytes
        Raw PDF bytes (suitable for Streamlit download button).
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )

    S = _styles()
    story = []
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ── Header ────────────────────────────────────────────────────────────────
    story.append(Paragraph("CrimeNet — CDR Investigation Report", S["title"]))
    story.append(Paragraph(f"Generated: {generated_at} | Digital Forensics Research", S["subtitle"]))
    story.append(HRFlowable(width="100%", thickness=1.5, color=BRAND_BLUE, spaceAfter=8))

    # ── Dataset Overview ──────────────────────────────────────────────────────
    story.append(Paragraph("1. Dataset Overview", S["section"]))

    overview_data = [
        ["Metric", "Value"],
        ["Total CDR Records", str(summary.get("total_records", "N/A"))],
        ["Unique Phone Numbers", str(summary.get("unique_numbers", "N/A"))],
        ["Date Range", f"{summary.get('date_range_start', '?')} to {summary.get('date_range_end', '?')}"],
        ["Unique Cell Towers", str(summary.get("unique_towers", "N/A"))],
        ["Total Duration (min)", str(summary.get("total_duration_min", "N/A"))],
        ["Avg Call Duration (sec)", str(summary.get("avg_duration_sec", "N/A"))],
    ]
    t = Table(overview_data, colWidths=[7 * cm, 9 * cm])
    t.setStyle(_table_style())
    story.append(t)
    story.append(Spacer(1, 0.3 * cm))

    # ── Network Analysis ──────────────────────────────────────────────────────
    story.append(Paragraph("2. Key Suspects — Network Centrality", S["section"]))
    story.append(Paragraph(
        "Nodes ranked by betweenness centrality — numbers that act as brokers "
        "between sub-groups are the most investigatively significant.",
        S["body"]
    ))
    story.append(Spacer(1, 0.2 * cm))

    if centrality_df is not None and len(centrality_df) > 0:
        top = centrality_df.head(10)
        cent_data = [["Phone Number", "Betweenness", "PageRank", "Out Calls", "In Calls"]]
        for _, row in top.iterrows():
            cent_data.append([
                row["phone_number"],
                str(row["betweenness_centrality"]),
                str(row["pagerank"]),
                str(row["call_count_out"]),
                str(row["call_count_in"]),
            ])
        t = Table(cent_data, colWidths=[4.5 * cm, 3 * cm, 3 * cm, 2.5 * cm, 2.5 * cm])
        t.setStyle(_table_style())
        story.append(t)
    else:
        story.append(Paragraph("No centrality data available.", S["body"]))

    story.append(Spacer(1, 0.3 * cm))

    # ── Anomaly Detection ─────────────────────────────────────────────────────
    story.append(Paragraph("3. Anomalous Behavior Flags", S["section"]))
    story.append(Paragraph(
        "Numbers flagged by Isolation Forest as statistically anomalous "
        "based on call frequency, timing, duration, and contact patterns.",
        S["body"]
    ))
    story.append(Spacer(1, 0.2 * cm))

    if anomaly_df is not None and len(anomaly_df) > 0:
        flagged = anomaly_df[anomaly_df["is_anomaly"]].head(8)
        if len(flagged) > 0:
            anom_data = [["Phone Number", "Anomaly Rank", "Night Ratio", "Short Call Ratio", "Calls/Day"]]
            for _, row in flagged.iterrows():
                anom_data.append([
                    row["phone_number"],
                    str(row["anomaly_rank"]),
                    str(row.get("night_ratio", "N/A")),
                    str(row.get("short_call_ratio", "N/A")),
                    str(row.get("calls_per_day", "N/A")),
                ])
            t = Table(anom_data, colWidths=[4.5 * cm, 3 * cm, 3 * cm, 3 * cm, 2 * cm])
            t.setStyle(_table_style(ACCENT_RED))
            story.append(t)
        else:
            story.append(Paragraph("No anomalies detected at current threshold.", S["body"]))

    story.append(Spacer(1, 0.3 * cm))

    # ── Burner Phone Detection ────────────────────────────────────────────────
    story.append(Paragraph("4. Burner Phone Suspects", S["section"]))

    if burner_df is not None and len(burner_df) > 0:
        burner_data = [["Phone Number", "IMEI", "Detection Signal", "Detail"]]
        for _, row in burner_df.head(10).iterrows():
            burner_data.append([
                row["phone_number"],
                str(row.get("imei", "N/A"))[:15],
                row["signal"][:60],
                row["detail"][:60],
            ])
        t = Table(burner_data, colWidths=[3.5 * cm, 3.5 * cm, 5 * cm, 5 * cm])
        t.setStyle(_table_style(ACCENT_RED))
        story.append(t)
    else:
        story.append(Paragraph("No burner phone indicators detected.", S["body"]))

    story.append(Spacer(1, 0.3 * cm))

    # ── Co-location Events ────────────────────────────────────────────────────
    story.append(Paragraph("5. Physical Meeting Detections (Co-location)", S["section"]))
    story.append(Paragraph(
        "Two numbers at the same cell tower within a 30-minute window — "
        "potential physical meetings even without direct calls.",
        S["body"]
    ))
    story.append(Spacer(1, 0.2 * cm))

    if colocation_df is not None and len(colocation_df) > 0:
        coloc_data = [["Number A", "Number B", "Tower", "Time A", "Gap (min)"]]
        for _, row in colocation_df.head(10).iterrows():
            coloc_data.append([
                row["phone_a"],
                row["phone_b"],
                row.get("tower_location", row["tower_id"])[:25],
                str(row["time_a"])[:16],
                str(row["gap_minutes"]),
            ])
        t = Table(coloc_data, colWidths=[3.5 * cm, 3.5 * cm, 4 * cm, 4 * cm, 2 * cm])
        t.setStyle(_table_style())
        story.append(t)
    else:
        story.append(Paragraph("No co-location events detected.", S["body"]))

    story.append(Spacer(1, 0.3 * cm))

    # ── Network Dismantling ───────────────────────────────────────────────────
    story.append(Paragraph("6. Arrest Priority — Network Dismantling Simulation", S["section"]))
    story.append(Paragraph(
        "Simulates which arrests would most effectively disrupt the communication network. "
        "Targets are selected by betweenness centrality at each step.",
        S["body"]
    ))
    story.append(Spacer(1, 0.2 * cm))

    if dismantling:
        dis_data = [["Step", "Remove", "Remaining Nodes", "Largest Component", "Network Efficiency"]]
        for step in dismantling:
            dis_data.append([
                str(step["step"]),
                step["removed_node"],
                str(step["remaining_nodes"]),
                str(step["largest_component_size"]),
                str(step["network_efficiency"]),
            ])
        t = Table(dis_data, colWidths=[1.5 * cm, 4 * cm, 3.5 * cm, 3.5 * cm, 4 * cm])
        t.setStyle(_table_style())
        story.append(t)
    else:
        story.append(Paragraph("Dismantling simulation data not available.", S["body"]))

    # ── Footer ────────────────────────────────────────────────────────────────
    story.append(Spacer(1, 0.5 * cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=MID_GRAY, spaceAfter=6))
    story.append(Paragraph(
        "This report is generated for research and investigative review purposes. "
        "All findings reference raw CDR records and should be verified before court submission. "
        "CrimeNet — Digital Forensics Research.",
        ParagraphStyle("footer", parent=getSampleStyleSheet()["Normal"], fontSize=7, textColor=MID_GRAY),
    ))

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()

    if output_path:
        with open(output_path, "wb") as f:
            f.write(pdf_bytes)

    return pdf_bytes
