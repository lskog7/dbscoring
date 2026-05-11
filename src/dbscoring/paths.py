"""Path configuration for source data, warehouse, models and reports."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ProjectPaths:
    """Resolved filesystem locations used by the package."""

    data_root: Path = Path("data/sources")
    warehouse_root: Path = Path("data/warehouse")
    model_root: Path = Path("models")
    report_root: Path = Path("reports")

    def table_path(self, table_name: str) -> Path:
        """Return the parquet file path for a warehouse table."""

        return self.warehouse_root / f"{table_name}.parquet"

    def resolve(self) -> ProjectPaths:
        """Resolve all configured paths without creating directories."""

        return ProjectPaths(
            data_root=self.data_root.resolve(),
            warehouse_root=self.warehouse_root.resolve(),
            model_root=self.model_root.resolve(),
            report_root=self.report_root.resolve(),
        )
