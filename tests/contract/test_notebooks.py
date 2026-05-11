from __future__ import annotations

import json
from pathlib import Path


def _notebook_text(path: Path) -> str:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    return "\n".join("".join(cell.get("source", [])) for cell in notebook["cells"])


def test_polars_notebook_contains_user_interaction_examples() -> None:
    text = _notebook_text(Path("notebooks/polars_lab.ipynb"))

    assert "build_warehouse" in text
    assert "validate_warehouse" in text
    assert "make_synthetic_labels" in text
    assert "train_catboost" in text


def test_spark_notebook_is_colab_only_and_mirrors_sections() -> None:
    text = _notebook_text(Path("notebooks/spark_lab.ipynb"))

    assert "Colab" in text
    assert "SparkSession" in text
    assert "build_warehouse_spark" in text
    assert "manifest" in text.lower()


def test_old_src_notebooks_are_not_the_canonical_entrypoint() -> None:
    assert Path("notebooks/polars_lab.ipynb").exists()
    assert Path("notebooks/spark_lab.ipynb").exists()
