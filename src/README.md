# Code

Package-first and notebook-documented project layout:

- `dbscoring/` — tested production package used by CLI and notebooks.
- `../notebooks/polars_lab.ipynb` — main local implementation walkthrough on `polars`.
- `../notebooks/spark_lab.ipynb` — mirror implementation walkthrough for `pyspark` / Colab.

Notebook logic should call package functions where possible. Business contracts must stay aligned across Polars, Spark and tests.
