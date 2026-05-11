from __future__ import annotations

from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from dbscoring.etl import build_feature_frame, build_warehouse
from dbscoring.ml import (
    build_training_frame,
    load_model,
    make_synthetic_labels,
    predict,
    save_model,
    train_catboost,
    tune_catboost,
    validate_label_frame,
)
from dbscoring.paths import ProjectPaths


def test_synthetic_labels_are_reproducible(fixture_paths: ProjectPaths) -> None:
    build_warehouse(fixture_paths, reset=True)
    features = build_feature_frame(fixture_paths)

    first = make_synthetic_labels(features)
    second = make_synthetic_labels(features)

    assert first.equals(second)
    assert set(first.get_column("target").to_list()) <= {0, 1}


def test_label_frame_validation_rejects_invalid_targets() -> None:
    labels = pl.DataFrame({"client_id": ["1"], "target": [2]})

    with pytest.raises(ValueError, match="binary"):
        validate_label_frame(labels)


def test_training_frame_accepts_polars_and_pandas(fixture_paths: ProjectPaths) -> None:
    build_warehouse(fixture_paths, reset=True)
    features = build_feature_frame(fixture_paths)
    labels = make_synthetic_labels(features)

    frame_from_polars = build_training_frame(features, labels)
    frame_from_pandas = build_training_frame(features.to_pandas(), labels.to_pandas())

    assert isinstance(frame_from_polars, pd.DataFrame)
    assert frame_from_polars.shape == frame_from_pandas.shape


def test_catboost_train_predict_save_load_smoke(
    fixture_paths: ProjectPaths, tmp_path: Path
) -> None:
    build_warehouse(fixture_paths, reset=True)
    features = build_feature_frame(fixture_paths)
    labels = make_synthetic_labels(features)

    model = train_catboost(features, labels, iterations=4, depth=2, learning_rate=0.1)
    predictions = predict(model, features)
    model_path = tmp_path / "catboost.cbm"
    save_model(model, model_path)
    loaded = load_model(model_path)
    loaded_predictions = predict(loaded, features)

    assert predictions.height == features.height
    assert loaded_predictions.height == predictions.height
    assert set(model.metrics) == {"logloss", "pr_auc", "roc_auc"}


def test_optuna_tune_mini_run(fixture_paths: ProjectPaths) -> None:
    build_warehouse(fixture_paths, reset=True)
    features = build_feature_frame(fixture_paths)
    labels = make_synthetic_labels(features)

    result = tune_catboost(features, labels, trials=1)

    assert "best_params" in result
    assert "best_value" in result
