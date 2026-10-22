← [README](../README.md)

# Code Quality — Ruff, mypy & tox

Linting, formatting, type checking and tests are driven by **[Ruff](https://docs.astral.sh/ruff/)**, **[mypy](https://mypy.readthedocs.io/)** and **[tox](https://tox.wiki/)** — the same tooling as [GEO_Download](../../GEO_Download) and [GEO_E2E](../../GEO_E2E). All three are installed from the `pip:` section of `environment.yml`:

```bash
conda env update -f environment.yml   # only needed once if the env already existed
```

| Tool | Role | Where it is configured |
|------|------|------------------------|
| **Ruff** | Linter + formatter + import sorter (replaces flake8 / isort / black) | `pyproject.toml` → `[tool.ruff]`, `[tool.ruff.lint]`, `[tool.ruff.format]` |
| **mypy** | Static type checking of the source packages and entry-point scripts | `pyproject.toml` → `[tool.mypy]`, `tox.ini` → `[testenv:typecheck]` |
| **tox** | Runs every check in a throwaway virtualenv with its own dependencies | `tox.ini` |
| **pytest** | Test suite | `pyproject.toml` → `[tool.pytest.ini_options]` — see [Testing](testing.md) |

## Ruff — run it directly

Fast, in-loop commands while editing:

```bash
ruff check .                            # lint everything, writes nothing
ruff check --fix .                      # apply safe autofixes (imports, modernization, unused code)
ruff check --output-format=concise .    # one line per finding — good for CI logs
ruff format --check .                   # list files that are not canonically formatted
ruff format .                           # rewrite files in style (line length 120)
```

### Rules

Configured in `pyproject.toml`:

- `line-length = 120`, `target-version = "py311"`; `logs/`, `__pycache__` and `.idea` are excluded.
- Enabled families: `E,W` (pycodestyle), `F` (pyflakes), `I` (import sorting), `B` (bugbear), `UP` (pyupgrade), `C4` (comprehensions), `SIM` (simplify), `RUF` (ruff-specific).
- Exceptions: `SIM108` ignored (ternary rewrites), `F401` ignored in `__init__.py`, `B011` ignored in `test/**`.
- `E402` (import not at top of file) is ignored for `run_*.py`, `utils/upload_utils.py` and `utils/raw2plain_utils.py` — `findspark.init()` **must** run before any `pyspark` import.

Current state: `ruff check .` → **0 findings**.

### Format

`ruff format .` has been applied repo-wide: **173 files already formatted** (157 Python files + `README.md` + 15 files under `docs/`).

> `ruff format` also walks `README.md` and `docs/*.md`, but only rewrites code inside fenced blocks tagged `python` — prose, tables and `bash`/`json`/`text` blocks are left untouched. `ruff check` (lint) only looks at `.py` files.

## mypy

```bash
mypy                 # targets come from [tool.mypy] files in pyproject.toml
tox -e typecheck     # same, in an isolated environment
```

Checked packages: `model`, `utils`, `factory`, `jobs`, `ingestion` (`upload/` + `raw2plain/`), `single_etl`, `views` and the six `run_*.py` entry points — **108 source files**, tests excluded.

`pyspark`, `findspark` and `py4j` ship no type stubs, so they are listed under `[[tool.mypy.overrides]]` with `ignore_missing_imports = true`.

Current state: `mypy` → **0 errors**.

## Coverage — pytest-cov

Coverage is configured in `pyproject.toml` and reported by `pytest-cov` (already installed in the `geo_etl_311` env; it is **not** listed in `environment.yml`/`tox.ini`, so re-add it with `pip install pytest-cov` if you rebuild the env). `addopts` does not include it, so pass it explicitly:

```bash
python -m pytest test/ -q -o addopts= --cov=. --cov-report=term-missing
```

- `[tool.coverage.run]` omits the two manual Spark entry points, `run_pyspark_csv_test.py` and `run_pyspark_supabase_test.py` (hand-run against the cluster, not by pytest).
- `[tool.coverage.report]` excludes `pragma: no cover` and `if __name__ == .__main__.:` lines.

Current state: **100 %** of the executed lines across the 1269-test suite.

## tox environments

`tox` with no arguments runs the default `envlist` from `tox.ini` (`lint, py311`):

| Command | What runs | Status |
|---------|-----------|--------|
| `tox` | default gate = `lint` + `py311` | ✅ |
| `tox -e lint` | `ruff check --output-format=concise .` | ✅ 0 findings |
| `tox -e py311` | full pytest suite (1269 tests) | ✅ |
| `tox -e format` | `ruff format --check .` | ✅ 176 files already formatted |
| `tox -e format-fix` | `ruff format .` — writes the changes | – |
| `tox -e typecheck` | `mypy` (102 files) | ✅ 0 errors |
| `tox -e warnings` | `pytest -W error::DeprecationWarning` | ✅ |
| `tox -e lint,py311,typecheck` | any list of environments in one invocation | – |

Arguments after `--` are passed to pytest (the `py311` env uses `{posargs}`):

```bash
tox -e py311 -- -k upload                # only upload tests
tox -e py311 -- test/jobs -v
```

Notes:

- tox creates its environments under `.tox/` (git-ignored) and installs each env's `deps` from `tox.ini`; your shell only needs `tox` itself — no conda activation is required.
- `skip_install = true` everywhere — the project is not installed as a package, tests import it from the working directory.
- The `py311`, `typecheck` and `warnings` envs install `pyspark==3.5.5`, so their first run downloads the PySpark distribution.
- To add a new check, add a `[testenv:xxx]` block; append `xxx` to `envlist` to make it part of the default `tox` run.
