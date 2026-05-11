"""CatBoost training, tuning, persistence and inference."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
from sklearn.metrics import average_precision_score, log_loss, roc_auc_score
from sklearn.model_selection import train_test_split

from dbscoring.ml.frame import normalize_feature_values, to_pandas, to_polars
from dbscoring.ml.labels import validate_label_frame


@dataclass(slots=True)
class ScoringModel:
    """Trained scoring model plus reproducible metadata."""

    model: Any
    feature_columns: list[str]
    cat_features: list[str]
    metrics: dict[str, float]


def build_training_frame(
    features: pl.DataFrame | pd.DataFrame,
    labels: pl.DataFrame | pd.DataFrame,
) -> pd.DataFrame:
    """Join features and labels and normalize values for CatBoost."""

    feature_frame = to_polars(features)
    label_frame = validate_label_frame(to_polars(labels))
    joined = feature_frame.join(
        label_frame.select("client_id", "target"), on="client_id", how="inner"
    )
    if joined.is_empty():
        raise ValueError("No clients overlap between features and labels")
    pandas_frame = joined.to_pandas()
    for column in pandas_frame.columns:
        if column not in {"client_id", "target"}:
            pandas_frame[column] = pandas_frame[column].map(normalize_feature_values)
    return pandas_frame


def _split_features(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series, list[str], list[str]]:
    feature_columns = [
        column for column in frame.columns if column not in {"client_id", "target"}
    ]
    if not feature_columns:
        raise ValueError("Training frame contains no feature columns")
    x_frame = frame[feature_columns].copy()
    y_series = frame["target"].astype(int)
    cat_features = [
        column for column in feature_columns if x_frame[column].dtype == "object"
    ]
    for column in feature_columns:
        if column not in cat_features:
            x_frame[column] = pd.to_numeric(x_frame[column], errors="coerce")
    return x_frame, y_series, feature_columns, cat_features


def _score_binary_model(
    model: Any, x_valid: pd.DataFrame, y_valid: pd.Series
) -> dict[str, float]:
    probabilities = model.predict_proba(x_valid)[:, 1]
    metrics = {
        "logloss": float(log_loss(y_valid, probabilities, labels=[0, 1])),
        "pr_auc": float(average_precision_score(y_valid, probabilities)),
    }
    if len(np.unique(y_valid)) > 1:
        metrics["roc_auc"] = float(roc_auc_score(y_valid, probabilities))
    else:
        metrics["roc_auc"] = float("nan")
    return metrics


def train_catboost(
    features: pl.DataFrame | pd.DataFrame,
    labels: pl.DataFrame | pd.DataFrame,
    *,
    iterations: int = 50,
    depth: int = 4,
    learning_rate: float = 0.08,
    random_seed: int = 42,
) -> ScoringModel:
    """Train a CatBoost binary classifier."""

    from catboost import CatBoostClassifier

    training_frame = build_training_frame(features, labels)
    x_frame, y_series, feature_columns, cat_features = _split_features(training_frame)
    stratify = (
        y_series
        if y_series.nunique() > 1 and y_series.value_counts().min() > 1
        else None
    )
    test_size = 0.5 if len(y_series) < 8 else 0.25
    x_train, x_valid, y_train, y_valid = train_test_split(
        x_frame,
        y_series,
        test_size=test_size,
        random_state=random_seed,
        stratify=stratify,
    )
    model = CatBoostClassifier(
        iterations=iterations,
        depth=depth,
        learning_rate=learning_rate,
        loss_function="Logloss",
        random_seed=random_seed,
        allow_writing_files=False,
        verbose=False,
    )
    model.fit(x_train, y_train, cat_features=cat_features)
    return ScoringModel(
        model=model,
        feature_columns=feature_columns,
        cat_features=cat_features,
        metrics=_score_binary_model(model, x_valid, y_valid),
    )


def tune_catboost(
    features: pl.DataFrame | pd.DataFrame,
    labels: pl.DataFrame | pd.DataFrame,
    *,
    trials: int = 10,
    random_seed: int = 42,
) -> dict[str, Any]:
    """Tune CatBoost hyperparameters with Optuna."""

    import optuna
    from catboost import CatBoostClassifier

    training_frame = build_training_frame(features, labels)
    x_frame, y_series, _feature_columns, cat_features = _split_features(training_frame)
    test_size = 0.5 if len(y_series) < 8 else 0.25
    x_train, x_valid, y_train, y_valid = train_test_split(
        x_frame,
        y_series,
        test_size=test_size,
        random_state=random_seed,
    )

    def objective(trial: optuna.Trial) -> float:
        model = CatBoostClassifier(
            iterations=trial.suggest_int("iterations", 10, 80),
            depth=trial.suggest_int("depth", 2, 6),
            learning_rate=trial.suggest_float("learning_rate", 0.02, 0.2),
            loss_function="Logloss",
            random_seed=random_seed,
            allow_writing_files=False,
            verbose=False,
        )
        model.fit(x_train, y_train, cat_features=cat_features)
        probabilities = model.predict_proba(x_valid)[:, 1]
        return float(log_loss(y_valid, probabilities, labels=[0, 1]))

    sampler = optuna.samplers.TPESampler(seed=random_seed)
    study = optuna.create_study(direction="minimize", sampler=sampler)
    study.optimize(objective, n_trials=trials, show_progress_bar=False)
    return {
        "best_params": study.best_params,
        "best_value": float(study.best_value),
        "cat_features": cat_features,
    }


def predict(model: ScoringModel, features: pl.DataFrame | pd.DataFrame) -> pl.DataFrame:
    """Run model inference and return client-level probabilities."""

    frame = to_pandas(features)
    if "client_id" not in frame.columns:
        raise ValueError("Prediction input must contain client_id")
    x_frame = frame[model.feature_columns].copy()
    for column in model.feature_columns:
        x_frame[column] = x_frame[column].map(normalize_feature_values)
        if column not in model.cat_features:
            x_frame[column] = pd.to_numeric(x_frame[column], errors="coerce")
    probabilities = model.model.predict_proba(x_frame)[:, 1]
    return pl.DataFrame(
        {"client_id": frame["client_id"].astype(str), "score": probabilities}
    )


def save_model(model: ScoringModel, path: Path) -> None:
    """Persist a CatBoost model and lightweight metadata."""

    path.parent.mkdir(parents=True, exist_ok=True)
    model.model.save_model(path)
    metadata = {
        "feature_columns": model.feature_columns,
        "cat_features": model.cat_features,
        "metrics": model.metrics,
    }
    path.with_suffix(".json").write_text(
        json.dumps(metadata, indent=2), encoding="utf-8"
    )


def load_model(path: Path) -> ScoringModel:
    """Load a model saved by `save_model`."""

    from catboost import CatBoostClassifier

    metadata = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))
    model = CatBoostClassifier()
    model.load_model(path)
    return ScoringModel(
        model=model,
        feature_columns=list(metadata["feature_columns"]),
        cat_features=list(metadata["cat_features"]),
        metrics=dict(metadata["metrics"]),
    )
