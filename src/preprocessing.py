"""Preprocessing helpers for PySpark pipeline (placeholders)."""

def select_features(df, features):
    """Return df with only selected features (placeholder)."""
    return df.select(*features)
