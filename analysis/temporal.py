"""
Temporal Pattern Analysis
==========================
Analyze call patterns over time: hourly activity, daily heatmaps,
communication burst detection, and silence gap analysis.
"""

import pandas as pd
import numpy as np


def hourly_activity(df: pd.DataFrame) -> pd.Series:
    """
    Compute call volume by hour of day (0–23).

    Returns
    -------
    pd.Series
        Index = hour (0–23), values = call count.
    """
    return df["Datetime"].dt.hour.value_counts().sort_index()


def daily_activity(df: pd.DataFrame) -> pd.Series:
    """
    Compute call volume by calendar date.

    Returns
    -------
    pd.Series
        Index = date, values = call count.
    """
    return df["Datetime"].dt.date.value_counts().sort_index()


def weekday_heatmap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a heatmap grid of call volume: day-of-week × hour-of-day.

    Returns
    -------
    pd.DataFrame
        7 rows (Mon–Sun) × 24 columns (0–23 hours).
    """
    df = df.copy()
    df["weekday"] = df["Datetime"].dt.day_name()
    df["hour"] = df["Datetime"].dt.hour

    order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot = (
        df.groupby(["weekday", "hour"])
        .size()
        .unstack(fill_value=0)
        .reindex(order, fill_value=0)
    )
    # Ensure all 24 hours are present
    pivot = pivot.reindex(columns=range(24), fill_value=0)
    return pivot


def night_call_ratio(df: pd.DataFrame, night_start: int = 22, night_end: int = 5) -> pd.DataFrame:
    """
    Compute the fraction of calls made during night hours per phone number.

    Night hours span night_start (inclusive) to night_end (exclusive, next day).
    Default: 22:00 – 05:00.

    Parameters
    ----------
    df : pd.DataFrame
    night_start : int   Hour when night begins (default 22)
    night_end   : int   Hour when night ends (default 5, i.e. 05:00)

    Returns
    -------
    pd.DataFrame
        Columns: phone_number, total_calls, night_calls, night_ratio
        Sorted by night_ratio descending.
    """
    df = df.copy()
    df["hour"] = df["Datetime"].dt.hour

    if night_start > night_end:
        df["is_night"] = (df["hour"] >= night_start) | (df["hour"] < night_end)
    else:
        df["is_night"] = (df["hour"] >= night_start) & (df["hour"] < night_end)

    result = (
        df.groupby("Calling_Number")
        .agg(
            total_calls=("Calling_Number", "count"),
            night_calls=("is_night", "sum"),
        )
        .reset_index()
        .rename(columns={"Calling_Number": "phone_number"})
    )
    result["night_ratio"] = (result["night_calls"] / result["total_calls"]).round(3)
    return result.sort_values("night_ratio", ascending=False).reset_index(drop=True)


def detect_bursts(df: pd.DataFrame, window_hours: int = 2, threshold: int = 10) -> pd.DataFrame:
    """
    Detect burst periods: time windows where call volume spikes above a threshold.

    Parameters
    ----------
    df : pd.DataFrame
    window_hours : int   Rolling window size in hours.
    threshold : int      Minimum calls in the window to flag as a burst.

    Returns
    -------
    pd.DataFrame
        Burst windows with start time, end time, and call count.
    """
    df_sorted = df.sort_values("Datetime").copy()
    df_sorted.set_index("Datetime", inplace=True)

    # Resample to hourly counts
    hourly = df_sorted.resample("h").size()
    rolling = hourly.rolling(window=window_hours, min_periods=1).sum()

    burst_times = rolling[rolling >= threshold]
    if burst_times.empty:
        return pd.DataFrame(columns=["window_start", "window_end", "call_count"])

    results = []
    for ts, count in burst_times.items():
        results.append({
            "window_start": ts - pd.Timedelta(hours=window_hours - 1),
            "window_end": ts,
            "call_count": int(count),
        })

    return pd.DataFrame(results).drop_duplicates().reset_index(drop=True)


def silence_gaps(df: pd.DataFrame, phone_number: str, min_gap_hours: int = 24) -> pd.DataFrame:
    """
    Find significant gaps in a number's communication history.

    A silence gap may indicate: device off, SIM swap, evasion, or arrest.

    Parameters
    ----------
    df : pd.DataFrame
    phone_number : str
    min_gap_hours : int   Minimum gap duration to report (default 24 hours).

    Returns
    -------
    pd.DataFrame
        Each row: gap_start, gap_end, gap_hours.
    """
    mask = (df["Calling_Number"] == phone_number) | (df["Called_Number"] == phone_number)
    subset = df[mask].sort_values("Datetime")

    if len(subset) < 2:
        return pd.DataFrame(columns=["gap_start", "gap_end", "gap_hours"])

    times = subset["Datetime"].values
    gaps = []
    for i in range(1, len(times)):
        gap_seconds = (times[i] - times[i - 1]) / np.timedelta64(1, "s")
        gap_hours = gap_seconds / 3600
        if gap_hours >= min_gap_hours:
            gaps.append({
                "gap_start": pd.Timestamp(times[i - 1]),
                "gap_end": pd.Timestamp(times[i]),
                "gap_hours": round(gap_hours, 1),
            })

    return pd.DataFrame(gaps).reset_index(drop=True)


def per_number_profile(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a per-number behavioral profile from temporal features.

    Columns: phone_number, first_seen, last_seen, active_days,
             total_calls_out, avg_hour, std_hour (regularity indicator)

    Returns
    -------
    pd.DataFrame
        One row per unique calling number.
    """
    df = df.copy()
    df["hour"] = df["Datetime"].dt.hour
    df["date"] = df["Datetime"].dt.date

    profiles = (
        df.groupby("Calling_Number")
        .agg(
            first_seen=("Datetime", "min"),
            last_seen=("Datetime", "max"),
            active_days=("date", "nunique"),
            total_calls_out=("Calling_Number", "count"),
            avg_hour=("hour", "mean"),
            std_hour=("hour", "std"),
        )
        .reset_index()
        .rename(columns={"Calling_Number": "phone_number"})
    )
    profiles["avg_hour"] = profiles["avg_hour"].round(1)
    profiles["std_hour"] = profiles["std_hour"].round(2)
    return profiles.sort_values("total_calls_out", ascending=False).reset_index(drop=True)
