# dbscoring
Credit scoring with Spark. University project for MEPhI "Data and process mining" course.

## Installation
Install dependencies with `uv` (preferred) or `pip` (fallback).

### 1) Install `uv` (preferred path)
- On macOS/Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

- If `curl` is unavailable, install via pip:

```bash
python3 -m pip install uv
```

- Verify:

```bash
uv --version
```

### 2) Project bootstrap with `uv` (recommended)

```bash
uv venv --python 3.13
uv sync
```

- Run tooling and tests:

```bash
uv run lint
uv run typecheck
uv run test
```

### 3) Bootstrap with `pip` (fallback)

```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install numpy pandas polars pytest ruff ty
```

- If you need project package install:

```bash
pip install -e .
```
