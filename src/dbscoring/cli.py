"""Rich/Typer command-line interface for dbscoring."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from dbscoring.contracts import SOURCE_REGISTRY
from dbscoring.etl import (
    build_feature_frame,
    build_warehouse,
    summarize_warehouse,
    validate_warehouse,
)
from dbscoring.ml import (
    load_model,
    make_synthetic_labels,
    predict,
    save_model,
    train_catboost,
    tune_catboost,
)
from dbscoring.paths import ProjectPaths

app = typer.Typer(help="Credit-scoring ETL and ML toolkit.")
warehouse_app = typer.Typer(help="Build and validate the warehouse.")
ml_app = typer.Typer(help="Train, tune and run scoring models.")
app.add_typer(warehouse_app, name="warehouse")
app.add_typer(ml_app, name="ml")
console = Console()


def _paths(data_root: Path, warehouse_root: Path) -> ProjectPaths:
    return ProjectPaths(data_root=data_root, warehouse_root=warehouse_root).resolve()


def _render_summary(summary: dict[str, int]) -> None:
    table = Table(title="Warehouse summary")
    table.add_column("Table", style="cyan")
    table.add_column("Rows", justify="right", style="green")
    for table_name, rows in summary.items():
        table.add_row(table_name, f"{rows:,}")
    console.print(table)


@app.command()
def status() -> None:
    """Show source registry and project readiness."""

    table = Table(title="Source registry")
    table.add_column("ID", justify="right")
    table.add_column("Source")
    table.add_column("Frequency")
    table.add_column("Target")
    for source in SOURCE_REGISTRY:
        table.add_row(
            str(source.source_id),
            source.source_name,
            source.update_frequency,
            source.target_table,
        )
    console.print(table)


@warehouse_app.command("build")
def warehouse_build(
    data_root: Path = typer.Option(Path("data/sources"), help="Raw source data root."),
    warehouse_root: Path = typer.Option(
        Path("data/warehouse"), help="Output warehouse root."
    ),
    reset: bool = typer.Option(True, help="Rebuild warehouse from scratch."),
) -> None:
    """Build the Polars warehouse from source parquet data."""

    paths = _paths(data_root, warehouse_root)
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        progress.add_task("Building warehouse with Polars", total=None)
        summary = build_warehouse(paths, reset=reset)
    _render_summary(summary.as_dict())


@warehouse_app.command("validate")
def warehouse_validate(
    data_root: Path = typer.Option(Path("data/sources"), help="Raw source data root."),
    warehouse_root: Path = typer.Option(Path("data/warehouse"), help="Warehouse root."),
) -> None:
    """Validate warehouse schemas, keys and load-log statuses."""

    summary = validate_warehouse(_paths(data_root, warehouse_root))
    _render_summary(summary.as_dict())


@warehouse_app.command("report")
def warehouse_report(
    warehouse_root: Path = typer.Option(Path("data/warehouse"), help="Warehouse root."),
) -> None:
    """Render row counts for all warehouse tables."""

    summary = summarize_warehouse(ProjectPaths(warehouse_root=warehouse_root).resolve())
    _render_summary(summary.as_dict())


@ml_app.command("make-labels")
def ml_make_labels(
    warehouse_root: Path = typer.Option(Path("data/warehouse"), help="Warehouse root."),
    output: Path = typer.Option(
        Path("data/ml/labels.parquet"), help="Output label parquet path."
    ),
) -> None:
    """Create deterministic demo labels from real warehouse features."""

    features = build_feature_frame(
        ProjectPaths(warehouse_root=warehouse_root).resolve()
    )
    labels = make_synthetic_labels(features)
    output.parent.mkdir(parents=True, exist_ok=True)
    labels.write_parquet(output)
    console.print(f"[green]Wrote labels:[/] {output} ({labels.height:,} rows)")


@ml_app.command("make-features")
def ml_make_features(
    warehouse_root: Path = typer.Option(Path("data/warehouse"), help="Warehouse root."),
    output: Path = typer.Option(
        Path("data/ml/features.parquet"),
        help="Output feature parquet path.",
    ),
) -> None:
    """Export the source-agnostic wide feature frame."""

    features = build_feature_frame(
        ProjectPaths(warehouse_root=warehouse_root).resolve()
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    features.write_parquet(output)
    console.print(f"[green]Wrote features:[/] {output} ({features.height:,} rows)")


@ml_app.command("train")
def ml_train(
    warehouse_root: Path = typer.Option(Path("data/warehouse"), help="Warehouse root."),
    labels: Path = typer.Option(
        Path("data/ml/labels.parquet"), help="Label parquet path."
    ),
    model_out: Path = typer.Option(
        Path("models/catboost.cbm"), help="Model output path."
    ),
    iterations: int = typer.Option(50, min=1, help="CatBoost iterations."),
) -> None:
    """Train and save a CatBoost scoring model."""

    features = build_feature_frame(
        ProjectPaths(warehouse_root=warehouse_root).resolve()
    )
    label_frame = pl.read_parquet(labels)
    model = train_catboost(features, label_frame, iterations=iterations)
    save_model(model, model_out)
    console.print(f"[green]Model saved:[/] {model_out}")
    console.print(model.metrics)


@ml_app.command("tune")
def ml_tune(
    warehouse_root: Path = typer.Option(Path("data/warehouse"), help="Warehouse root."),
    labels: Path = typer.Option(
        Path("data/ml/labels.parquet"), help="Label parquet path."
    ),
    trials: int = typer.Option(10, min=1, help="Optuna trial count."),
) -> None:
    """Tune CatBoost hyperparameters with Optuna."""

    features = build_feature_frame(
        ProjectPaths(warehouse_root=warehouse_root).resolve()
    )
    result = tune_catboost(features, pl.read_parquet(labels), trials=trials)
    console.print(result)


@ml_app.command("predict")
def ml_predict(
    model: Path = typer.Option(
        Path("models/catboost.cbm"), help="Saved CatBoost model."
    ),
    input_path: Path = typer.Option(
        Path("data/ml/features.parquet"), help="Input feature parquet path."
    ),
    output: Path = typer.Option(
        Path("data/ml/predictions.parquet"), help="Prediction output path."
    ),
) -> None:
    """Run inference on a feature parquet file."""

    scoring_model = load_model(model)
    predictions = predict(scoring_model, pl.read_parquet(input_path))
    output.parent.mkdir(parents=True, exist_ok=True)
    predictions.write_parquet(output)
    console.print(
        f"[green]Wrote predictions:[/] {output} ({predictions.height:,} rows)"
    )
