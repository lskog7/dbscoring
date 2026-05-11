from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from dbscoring.cli import app
from dbscoring.testing import create_source_fixtures


def test_cli_status_renders_source_registry() -> None:
    result = CliRunner().invoke(app, ["status"])

    assert result.exit_code == 0
    assert "client_cards_daily" in result.output


def test_cli_warehouse_build_validate_and_report(tmp_path: Path) -> None:
    data_root = tmp_path / "sources"
    warehouse_root = tmp_path / "warehouse"
    create_source_fixtures(data_root)
    runner = CliRunner()

    build = runner.invoke(
        app,
        [
            "warehouse",
            "build",
            "--data-root",
            str(data_root),
            "--warehouse-root",
            str(warehouse_root),
        ],
    )
    validate = runner.invoke(
        app,
        [
            "warehouse",
            "validate",
            "--data-root",
            str(data_root),
            "--warehouse-root",
            str(warehouse_root),
        ],
    )
    report = runner.invoke(
        app,
        ["warehouse", "report", "--warehouse-root", str(warehouse_root)],
    )

    assert build.exit_code == 0
    assert validate.exit_code == 0
    assert report.exit_code == 0
    assert "client_monthly_attrs_scd1" in report.output


def test_cli_ml_feature_and_label_generation(tmp_path: Path) -> None:
    data_root = tmp_path / "sources"
    warehouse_root = tmp_path / "warehouse"
    features_path = tmp_path / "features.parquet"
    labels_path = tmp_path / "labels.parquet"
    create_source_fixtures(data_root)
    runner = CliRunner()

    build = runner.invoke(
        app,
        [
            "warehouse",
            "build",
            "--data-root",
            str(data_root),
            "--warehouse-root",
            str(warehouse_root),
        ],
    )
    features = runner.invoke(
        app,
        [
            "ml",
            "make-features",
            "--warehouse-root",
            str(warehouse_root),
            "--output",
            str(features_path),
        ],
    )
    labels = runner.invoke(
        app,
        [
            "ml",
            "make-labels",
            "--warehouse-root",
            str(warehouse_root),
            "--output",
            str(labels_path),
        ],
    )

    assert build.exit_code == 0
    assert features.exit_code == 0
    assert labels.exit_code == 0
    assert features_path.exists()
    assert labels_path.exists()
