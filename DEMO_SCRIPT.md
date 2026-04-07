# CrimeNet — 5-Minute Demo Script
### Digital Forensics & Cybercrime Investigation

---

## The Setup (30 seconds — say this before touching the screen)

> "What you're about to see is a real investigative scenario — a SIM card fraud ring
> operating across Bengaluru. Law enforcement has obtained 30 days of CDR data covering
> 17 phone numbers. The challenge: who are the masterminds, how do they operate,
> and who do we arrest first to dismantle the entire network?
> That's exactly what CrimeNet answers — in under 5 minutes."

**Then open the browser to `http://localhost:8501` and load `demo_fraud_ring.csv`.**

---

## Tab 1 — Overview (45 seconds)

Point to the top metrics row:

> "330 CDR records, 17 unique numbers, 7 cell towers, 30 days.
> Raw data — meaningless on its own. But look at this hourly activity chart."

Point to the spike between 10pm–2am:

> "This ring operates at night. That's not random — that's coordinated evasion of
> normal business surveillance. And this weekday heatmap confirms it:
> activity clusters on Tuesday and Thursday nights — consistent pre-planned operation windows."

---

## Tab 2 — Network Analysis (90 seconds)

Switch to the Network tab. Let the graph render.

> "This is the communication network — every node is a phone number, every edge is a call.
> Watch how the layout forms. Three nodes immediately pull to the centre."

Point to the three large central nodes `9900001111`, `9900002222`, `9900003333`:

> "These are your ringleaders. Not because they made the most calls —
> because they have the highest *betweenness centrality*.
> Every information flow in this network passes through them.
> They're the brokers — remove them and the network fragments."

Scroll to the centrality table:

> "The table confirms it. These three hold betweenness scores 10x higher than anyone else.
> PageRank backs this up — the network's 8 intermediaries all depend on them."

Point to the coloured communities:

> "Community detection has automatically identified two sub-groups —
> the blue cluster is the inner ring, the green cluster is the execution layer.
> This maps directly to how organized crime cells are structured."

Scroll to the Dismantling Simulation chart:

> "And here's the research contribution I'm most proud of.
> If you arrest just these 3 nodes, network efficiency drops by over 80%.
> This gives investigators a mathematically grounded arrest priority — not intuition."

---

## Tab 3 — Geographic Map (60 seconds)

Switch to the Geographic Map tab.

> "CDRs contain cell tower IDs. We've mapped those to GPS coordinates."

Point to the heatmap:

> "This heatmap shows where communication is concentrated — central Bengaluru.
> But the real finding is here."

Point to the red exclamation markers:

> "These red markers are co-location events — two phones at the *same tower*
> within a 30-minute window. With no direct call between them.
> This is how we prove a physical meeting without any direct communication record."

Click on the Koramangala marker:

> "March 15th, 10am. All three ringleaders and two senior intermediaries
> converge at Koramangala. Six hours later, the first wave of fraud calls hits the victims.
> That's not coincidence — that's a planning meeting captured in telecom metadata."

Select a ringleader number in the trajectory dropdown:

> "And this trajectory view shows exactly where that number moved over 30 days.
> This is alibi verification — or disproval."

---

## Tab 4 — Behavioral Analysis (60 seconds)

Switch to the Behavioral Analysis tab.

> "Every number has a behavioral fingerprint. Our Isolation Forest model
> scores each number on 9 features — call frequency, timing, duration patterns,
> one-way communication ratio, and more."

Point to the red bars in the anomaly chart:

> "The flagged numbers in red are statistical outliers. Look at who tops the list —
> the burner phones."

Point to the Burner Phone Suspects table:

> "6600001111 and 6600002222. Three simultaneous red flags:
> active for only 7 days, called only 2 unique targets, and — this is the key one —
> the same IMEI hardware ID was used with a *different* SIM number after March 15th.
> That's a SIM swap. That's deliberate evasion. And CrimeNet caught it automatically."

Point to the night call ratio chart:

> "The ringleaders also have a night call ratio above 60%.
> Normal users average around 10%. This behavioral signature alone would flag them
> for further investigation."

---

## Tab 5 — Investigation Report (30 seconds)

Switch to the Investigation Report tab. Click Generate PDF.

> "Finally — one click generates a structured PDF investigation report.
> Every finding links back to raw CDR rows. Every flag has a traceable,
> auditable reason. This is what makes it court-admissible and explainable —
> not a black box score, but evidence."

Download and briefly show the PDF.

---

## Closing — Research Pitch (30 seconds)

> "What you've seen in 5 minutes would take an experienced analyst 2–3 days manually.
>
> The research opportunities here go deeper: network dismantling theory for
> investigative optimization, adversarial CDR evasion detection, multi-source fusion
> with IPDR and financial records, and federated analysis across telecom operators
> without sharing raw data.
>
> CrimeNet is a foundation for that research — open, explainable, and built
> specifically for the Indian telecom forensics context.
>
> I'd love to explore how this platform can support active investigative priorities."

---

## Key Numbers to Remember

| What to say | Number |
|---|---|
| Ringleaders | `9900001111`, `9900002222`, `9900003333` |
| Betweenness drop after arrest | ~80% network efficiency loss |
| Burner IMEI swap date | March 15, 2024 |
| Fraud Wave 1 meeting location | Koramangala |
| Fraud Wave 2 meeting location | Whitefield |
| Co-location window | 30 minutes |
| Total records in demo | 330 |

---

## If They Ask Questions

**"How is this different from commercial tools like Maltego or i2?"**
> "Commercial tools are black boxes — you get a graph but no mathematical basis for prioritization.
> CrimeNet is explainable by design: every ranking has a traceable formula,
> every anomaly links to a specific CDR row. That matters in court."

**"What data do you need to run this?"**
> "Just a standard CDR CSV from any telecom operator. No proprietary format — the same
> data law enforcement already obtains through court orders."

**"Can this scale to millions of records?"**
> "The current implementation handles tens of thousands comfortably.
> For larger datasets, the NetworkX layer can be replaced with graph databases like Neo4j,
> and the anomaly detection can run on distributed frameworks. That's a natural research extension."

**"What about encrypted communications — WhatsApp etc.?"**
> "CDRs only capture metadata — not content, and not OTT apps directly.
> But IPDR analysis (internet packet data records) can complement CDR analysis for that.
> Correlating CDR silence gaps with IPDR data spikes is an active research area."

---

*CrimeNet — Digital Forensics Research Demo*
