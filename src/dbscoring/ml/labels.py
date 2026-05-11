"""Label-provider contract and deterministic demo labels."""

from __future__ import annotations

import polars as pl

from dbscoring.ml.frame import to_polars


def validate_label_frame(labels: pl.DataFrame) -> pl.DataFrame:
    """Validate external labels for supervised credit-scoring training."""

    required = {"client_id", "target"}
    missing = required - set(labels.columns)
    if missing:
        raise ValueError(f"Label frame is missing columns: {sorted(missing)}")
    normalized = labels.select(
        pl.col("client_id").cast(pl.Utf8),
        pl.col("target").cast(pl.Int8),
        *([pl.col("target_dt").cast(pl.Utf8)] if "target_dt" in labels.columns else []),
    )
    invalid_targets = normalized.filter(~pl.col("target").is_in([0, 1]))
    if not invalid_targets.is_empty():
        raise ValueError("Label target must be binary: 0 or 1")
    duplicated = normalized.group_by("client_id").len().filter(pl.col("len") > 1)
    if not duplicated.is_empty():
        raise ValueError("Label frame contains duplicated client_id values")
    return normalized


def make_synthetic_labels(features: pl.DataFrame) -> pl.DataFrame:
    """Create deterministic demo labels from real feature values.

    The output is intentionally suitable for tests and demos only. Production
    training must pass a real external label dataset through `validate_label_frame`.
    """

    frame = to_polars(features)
    if "client_id" not in frame.columns:
        raise ValueError("Feature frame must contain client_id")
    feature_columns = [column for column in frame.columns if column != "client_id"]
    if not feature_columns:
        raise ValueError("Feature frame must contain at least one feature column")
    score = pl.lit(0.0)
    for column in feature_columns[:8]:
        score = score + pl.col(column).cast(pl.Float64, strict=False).fill_null(0.0)
    return (
        frame.select("client_id", score.alias("_score"))
        .with_columns(
            (pl.col("_score").rank(method="average") > (pl.len() / 2))
            .cast(pl.Int8)
            .alias("target")
        )
        .select("client_id", "target")
    )
