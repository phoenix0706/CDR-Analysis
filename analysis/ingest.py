"""
CDR Ingestion & Validation
===========================
Load, validate, normalize, and clean Call Detail Record CSV files.
"""

import pandas as pd
from pathlib import Path


REQUIRED_COLUMNS = {
    "Calling_Number",
    "Called_Number",
    "Date",
    "Time",
    "Duration_sec",
    "Call_Type",
}

OPTIONAL_COLUMNS = {
    "Caller_IMEI",
    "Caller_IMSI",
    "Caller_Operator",
    "Tower_ID",
    "Tower_Location",
    "Tower_Latitude",
    "Tower_Longitude",
    "SR",
}


def load_cdr(filepath: str) -> pd.DataFrame:
    """
    Load a CDR CSV file, validate schema, normalize fields, and return a clean DataFrame.

    Parameters
    ----------
    filepath : str
        Path to the CDR CSV file.

    Returns
    -------
    pd.DataFrame
        Cleaned CDR data with a unified datetime column.

    Raises
    ------
    ValueError
        If required columns are missing.
    FileNotFoundError
        If the file does not exist.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"CDR file not found: {filepath}")

    df = pd.read_csv(filepath, dtype=str)

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Validate required columns
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required CDR columns: {missing}")

    df = _normalize(df)
    return df


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all normalization steps to the raw DataFrame."""
    df = df.copy()

    # Drop completely empty rows
    df.dropna(how="all", inplace=True)

    # Normalize phone numbers — strip non-digit chars, keep 10 digits
    for col in ["Calling_Number", "Called_Number"]:
        df[col] = df[col].str.replace(r"\D", "", regex=True).str[-10:]

    # Build unified datetime
    df["Datetime"] = pd.to_datetime(
        df["Date"].str.strip() + " " + df["Time"].str.strip(),
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )

    # Numeric fields
    df["Duration_sec"] = pd.to_numeric(df["Duration_sec"], errors="coerce").fillna(0).astype(int)

    if "Tower_Latitude" in df.columns:
        df["Tower_Latitude"] = pd.to_numeric(df["Tower_Latitude"], errors="coerce")
    if "Tower_Longitude" in df.columns:
        df["Tower_Longitude"] = pd.to_numeric(df["Tower_Longitude"], errors="coerce")

    # Remove self-calls (same caller and callee)
    df = df[df["Calling_Number"] != df["Called_Number"]].reset_index(drop=True)

    # Remove rows with no valid datetime
    invalid_dt = df["Datetime"].isna().sum()
    if invalid_dt > 0:
        df = df[df["Datetime"].notna()].reset_index(drop=True)

    # Sort chronologically
    df.sort_values("Datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def get_summary(df: pd.DataFrame) -> dict:
    """
    Return a summary dict of key CDR statistics.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned CDR DataFrame from load_cdr().

    Returns
    -------
    dict
        Summary statistics for display in the dashboard.
    """
    all_numbers = pd.concat([df["Calling_Number"], df["Called_Number"]]).unique()

    summary = {
        "total_records": len(df),
        "unique_numbers": len(all_numbers),
        "date_range_start": df["Datetime"].min().strftime("%Y-%m-%d"),
        "date_range_end": df["Datetime"].max().strftime("%Y-%m-%d"),
        "call_types": df["Call_Type"].value_counts().to_dict(),
        "top_callers": df["Calling_Number"].value_counts().head(5).to_dict(),
        "top_called": df["Called_Number"].value_counts().head(5).to_dict(),
        "avg_duration_sec": round(df[df["Duration_sec"] > 0]["Duration_sec"].mean(), 1),
        "total_duration_min": round(df["Duration_sec"].sum() / 60, 1),
    }

    if "Tower_ID" in df.columns:
        summary["unique_towers"] = df["Tower_ID"].nunique()

    if "Caller_Operator" in df.columns:
        summary["operators"] = df["Caller_Operator"].value_counts().to_dict()

    return summary
