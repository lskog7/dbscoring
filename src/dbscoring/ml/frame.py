"""Data-frame conversion utilities shared by the ML package."""

from __future__ import annotations

from typing import Any

import pandas as pd
import polars as pl


def to_polars(frame: pl.DataFrame | pd.DataFrame) -> pl.DataFrame:
    """Convert a pandas or polars frame to a Polars DataFrame."""

    if isinstance(frame, pl.DataFrame):
        return frame
    if isinstance(frame, pd.DataFrame):
        return pl.from_pandas(frame)
    raise TypeError(f"Unsupported frame type: {type(frame)!r}")


def to_pandas(frame: pl.DataFrame | pd.DataFrame) -> pd.DataFrame:
    """Convert a pandas or polars frame to a pandas DataFrame."""

    if isinstance(frame, pd.DataFrame):
        return frame.copy()
    if isinstance(frame, pl.DataFrame):
        return frame.to_pandas()
    raise TypeError(f"Unsupported frame type: {type(frame)!r}")


def normalize_feature_values(value: Any) -> float | str | None:
    """Normalize object values before they enter CatBoost."""

    if value is None:
        return None
    text = str(value)
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return text
