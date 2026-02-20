# marimo-playground

A [marimo](https://marimo.io) notebook playground for exploring Unity Catalog.

## Setup

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
# Install dependencies
uv sync
```

## Running Notebooks

Notebooks live in the `notebooks/` directory. Use `uv run` so marimo picks up the project's virtual environment.

```bash
# Open an existing notebook
uv run marimo edit notebooks/<notebook-name>.py

# Create a new notebook
uv run marimo edit notebooks/my_new_notebook.py

# Run a notebook as a read-only app
uv run marimo run notebooks/<notebook-name>.py
```

## Dependencies

Installed with `marimo[recommended]`, which includes:

| Package | Feature |
|---|---|
| `duckdb` | SQL cells |
| `altair` | Plotting in datasource viewer |
| `polars` | SQL output back in Python |
| `sqlglot` | SQL cell parsing |
| `openai` | AI features |
| `ruff` | Formatting |
| `nbformat` | Export as `.ipynb` |
| `vegafusion` | Performant charting |
