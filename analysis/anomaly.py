"""
Anomaly Detection
==================
Isolation Forest anomaly scoring, burner phone detection (IMEI-SIM correlation),
one-way communication detection, and short-burst coded signalling patterns.
"""

import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a per-number feature matrix for anomaly detection.

    Features
    --------
    - call_count_out       : total outbound calls
    - call_count_in        : total inbound calls
    - unique_contacts      : number of unique numbers contacted
    - avg_duration         : average call duration (voice only)
    - night_ratio          : fraction of calls at night (22:00–05:00)
    - one_way_ratio        : fraction of contacts who never call back
    - short_call_ratio     : fraction of calls under 10 seconds
    - active_days          : number of distinct active days
    - calls_per_day        : average calls per active day

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned CDR DataFrame.

    Returns
    -------
    pd.DataFrame
        One row per unique calling number with computed features.
    """
    df = df.copy()
    df["hour"] = df["Datetime"].dt.hour
    df["date"] = df["Datetime"].dt.date
    df["is_night"] = (df["hour"] >= 22) | (df["hour"] < 5)
    df["is_short"] = (df["Duration_sec"] > 0) & (df["Duration_sec"] < 10)

    out_stats = (
        df.groupby("Calling_Number")
        .agg(
            call_count_out=("Calling_Number", "count"),
            unique_contacts=("Called_Number", "nunique"),
            avg_duration=("Duration_sec", lambda x: x[x > 0].mean() if (x > 0).any() else 0),
            night_calls=("is_night", "sum"),
            short_calls=("is_short", "sum"),
            active_days=("date", "nunique"),
        )
        .reset_index()
        .rename(columns={"Calling_Number": "phone_number"})
    )

    in_counts = df.groupby("Called_Number").size().reset_index(name="call_count_in")
    in_counts.rename(columns={"Called_Number": "phone_number"}, inplace=True)

    # One-way: contacts that never call this number back
    callers = set(df["Calling_Number"].unique())

    def one_way_ratio(number):
        contacts = df[df["Calling_Number"] == number]["Called_Number"].unique()
        if len(contacts) == 0:
            return 0.0
        one_way = sum(1 for c in contacts if c not in callers)
        return round(one_way / len(contacts), 3)

    features = out_stats.merge(in_counts, on="phone_number", how="left")
    features["call_count_in"] = features["call_count_in"].fillna(0).astype(int)
    features["night_ratio"] = (features["night_calls"] / features["call_count_out"]).round(3)
    features["short_call_ratio"] = (features["short_calls"] / features["call_count_out"]).round(3)
    features["calls_per_day"] = (features["call_count_out"] / features["active_days"]).round(2)
    features["one_way_ratio"] = features["phone_number"].apply(one_way_ratio)

    features.drop(columns=["night_calls", "short_calls"], inplace=True)
    features["avg_duration"] = features["avg_duration"].fillna(0).round(1)

    return features.reset_index(drop=True)


def score_anomalies(features_df: pd.DataFrame, contamination: float = 0.15) -> pd.DataFrame:
    """
    Run Isolation Forest on the feature matrix and add anomaly scores.

    Parameters
    ----------
    features_df : pd.DataFrame
        Output of build_features().
    contamination : float
        Expected fraction of anomalies in the dataset (default 15%).

    Returns
    -------
    pd.DataFrame
        Original features plus:
        - anomaly_score : raw Isolation Forest score (lower = more anomalous)
        - anomaly_rank  : normalized 0–1 score (1 = most anomalous)
        - is_anomaly    : bool flag
    """
    numeric_cols = [
        "call_count_out", "call_count_in", "unique_contacts",
        "avg_duration", "night_ratio", "one_way_ratio",
        "short_call_ratio", "active_days", "calls_per_day",
    ]

    available = [c for c in numeric_cols if c in features_df.columns]
    X = features_df[available].fillna(0).values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
    )
    model.fit(X_scaled)

    raw_scores = model.score_samples(X_scaled)
    predictions = model.predict(X_scaled)

    result = features_df.copy()
    result["anomaly_score"] = raw_scores
    # Normalize to 0–1 where 1 = most anomalous
    result["anomaly_rank"] = 1 - (raw_scores - raw_scores.min()) / (raw_scores.max() - raw_scores.min() + 1e-9)
    result["anomaly_rank"] = result["anomaly_rank"].round(3)
    result["is_anomaly"] = predictions == -1

    return result.sort_values("anomaly_rank", ascending=False).reset_index(drop=True)


def detect_burner_phones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect potential burner phones using IMEI-SIM correlation analysis.

    Detection signals:
    1. Same IMEI used with multiple SIM numbers (SIM swap — evasion tactic)
    2. Number active only in a narrow time window (< 14 days)
    3. Number only called 1–2 unique targets
    4. Number never received incoming calls

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned CDR DataFrame. Requires Caller_IMEI column.

    Returns
    -------
    pd.DataFrame
        Suspected burner phones with detection signals and evidence counts.
    """
    if "Caller_IMEI" not in df.columns:
        return pd.DataFrame()

    results = []

    # Signal 1: IMEI shared across multiple phone numbers
    imei_to_phones = df.groupby("Caller_IMEI")["Calling_Number"].nunique()
    shared_imeis = imei_to_phones[imei_to_phones > 1].index

    for imei in shared_imeis:
        phones = df[df["Caller_IMEI"] == imei]["Calling_Number"].unique()
        for phone in phones:
            results.append({
                "phone_number": phone,
                "imei": imei,
                "signal": "IMEI shared across multiple SIMs",
                "detail": f"IMEI {imei} used by {len(phones)} numbers: {list(phones)}",
            })

    all_callees = set(df["Called_Number"].unique())

    per_number = df.groupby("Calling_Number").agg(
        first_seen=("Datetime", "min"),
        last_seen=("Datetime", "max"),
        unique_targets=("Called_Number", "nunique"),
        total_calls=("Calling_Number", "count"),
    ).reset_index().rename(columns={"Calling_Number": "phone_number"})

    for _, row in per_number.iterrows():
        phone = row["phone_number"]
        active_days = (row["last_seen"] - row["first_seen"]).days + 1
        received_calls = phone in all_callees

        signals = []

        # Signal 2: narrow active window
        if active_days <= 14:
            signals.append(f"Active only {active_days} day(s)")

        # Signal 3: minimal unique targets
        if row["unique_targets"] <= 2:
            signals.append(f"Only {int(row['unique_targets'])} unique target(s)")

        # Signal 4: never received calls
        if not received_calls:
            signals.append("Never received incoming calls")

        if len(signals) >= 2:
            results.append({
                "phone_number": phone,
                "imei": df[df["Calling_Number"] == phone]["Caller_IMEI"].iloc[0]
                if "Caller_IMEI" in df.columns else "N/A",
                "signal": " | ".join(signals),
                "detail": f"Active {active_days} day(s), {int(row['unique_targets'])} target(s), "
                          f"{int(row['total_calls'])} total calls",
            })

    if not results:
        return pd.DataFrame()

    burner_df = pd.DataFrame(results).drop_duplicates(subset=["phone_number"]).reset_index(drop=True)
    return burner_df


def one_way_communication(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify strongly one-directional communication pairs (A always calls B, B never responds).

    These patterns may indicate:
    - Command-and-control relationships
    - Victim–perpetrator pairs
    - Coded check-in signals

    Returns
    -------
    pd.DataFrame
        Columns: caller, callee, calls_from_caller, calls_from_callee, ratio
        Sorted by ratio descending.
    """
    pair_counts = (
        df.groupby(["Calling_Number", "Called_Number"])
        .size()
        .reset_index(name="count")
    )

    # For each (A→B), look up (B→A)
    reverse = pair_counts.rename(
        columns={"Calling_Number": "Called_Number", "Called_Number": "Calling_Number", "count": "reverse_count"}
    )
    merged = pair_counts.merge(reverse, on=["Calling_Number", "Called_Number"], how="left")
    merged["reverse_count"] = merged["reverse_count"].fillna(0).astype(int)

    # Keep only pairs where A→B significantly dominates B→A
    merged["ratio"] = merged["count"] / (merged["count"] + merged["reverse_count"])
    one_way = merged[merged["ratio"] >= 0.9].copy()
    one_way.columns = ["caller", "callee", "calls_from_caller", "calls_from_callee", "ratio"]

    return one_way.sort_values("ratio", ascending=False).reset_index(drop=True)
