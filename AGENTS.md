# AGENTS.md — file-namer

This repository is currently a small, single-file Python GUI app built with Tkinter and optionally packaged via PyInstaller.

## Repo layout

- `newapp3.py`: Main Tkinter application and all business logic.
- `newapp3.spec`: PyInstaller spec used to build a windowed executable.
- `build/`, `dist/`: PyInstaller output directories (note: `build/` and `dist/` are currently tracked by git even though `.gitignore` ignores them).

## Commands (build / lint / test)

### Run the app locally

- Run GUI app:
  - `python newapp3.py`

### Build a distributable executable (PyInstaller)

PyInstaller is not vendored here; install it in your environment if you need builds.

- Build using the existing spec:
  - `pyinstaller newapp3.spec`

Common variants (only if needed):
- Clean build:
  - `pyinstaller --clean newapp3.spec`
- One-off build directly from script (bypasses spec):
  - `pyinstaller --noconsole --name newapp3 newapp3.py`

### Lint / format

No linter/formatter configuration is present in this repo (no `pyproject.toml`, `ruff.toml`, `setup.cfg`, etc.).

Minimal sanity checks you can run without adding tooling:
- Syntax check:
  - `python -m py_compile newapp3.py`
- Bytecode compile (also catches syntax errors):
  - `python -m compileall newapp3.py`

If you introduce lint/format tooling in the future, prefer:
- Formatting: Black
- Linting/imports: Ruff
- Types: mypy or pyright

Do not reformat the entire file as part of unrelated changes; keep diffs focused.

### Tests

No test suite is currently present (no `tests/` directory and no test runner config).

If/when you add tests, prefer `pytest`.
- Run all tests:
  - `pytest`
- Run a single test file:
  - `pytest tests/test_something.py`
- Run a single test function:
  - `pytest tests/test_something.py::test_case_name`
- Run by keyword (fast iteration):
  - `pytest -k keyword`

## Editor/agent rules (Cursor/Copilot)

- No Cursor rules found (`.cursor/rules/`, `.cursorrules` not present).
- No Copilot instructions found (`.github/copilot-instructions.md` not present).

If such rules are added later, they take precedence over general guidance below.

## Coding guidelines (match existing code)

This codebase is currently “single-module / pragmatic”. Preserve behavior and avoid refactors while fixing bugs.

### Imports

- Keep imports grouped as:
  1. Python stdlib
  2. Third-party
  3. Local
- Within a group, prefer alphabetical ordering.
- Avoid unused imports; this file is small enough that unused imports are noise.

### Formatting

- Use 4-space indentation.
- Keep line length reasonable (~88–100 chars) but do not do sweeping wrap changes.
- Prefer explicit blocks over dense one-liners (the file has one-liners today; new code should be more readable).

### Types

- Prefer real types over `Any`.
- When adding new functions/classes, annotate parameters and return types.
- Use `pathlib.Path` for filesystem paths.
- Prefer `Optional[T]` only when `None` is a valid value.

### Naming

- Functions/variables: `snake_case`.
- Classes: `PascalCase`.
- Constants and compiled regexes: `UPPER_SNAKE_CASE`.
- Regex patterns should be compiled once at module import time (as done today).

### Error handling

- Avoid empty `except` blocks and `except Exception: pass`.
- Catch specific exceptions when feasible (`FileNotFoundError`, `OSError`, etc.).
- When catching broadly (rare), log enough context to diagnose failures (path, operation, exception message).
- Do not silently change behavior under errors; prefer skipping with a clear log entry.

### Filesystem safety

- Preserve the existing “safe copy” behavior:
  - `safe_copy2_atomic()` writes to `*.part` and replaces atomically.
  - `VERIFY_SIZE`, `FSYNC_AFTER_COPY`, retry/backoff logic are intentional.
- Use `ensure_unique_path_fast()` for collision handling.
- When creating directories, use `mkdir(parents=True, exist_ok=True)`.

### Concurrency and UI

- Tkinter widgets must only be mutated on the main thread.
- The current pattern is:
  - Worker thread does filesystem work.
  - Worker uses `queue.Queue()` to send log messages.
  - UI uses `after()` (`_ui_tick`) to drain the queue and update widgets.
- Keep that pattern: do not call tkinter methods from the worker thread.

### Structure and refactoring

- Prefer minimal changes in `newapp3.py`.
- If the module grows significantly, split by responsibility:
  - `fs_ops.py` (copy/move, retries)
  - `parsing.py` (regex parsing, matching)
  - `ui.py` (Tkinter widgets and event handlers)
  - Keep `main()` small and import the App.

### Localization

- UI strings are currently Korean; keep language consistent.
- If introducing new labels/messages, follow the existing tone.

## Git hygiene (pragmatic)

- Do not commit secrets.
- `build/` and `dist/` are generated artifacts. If you make a repo hygiene change, consider untracking them and relying on `.gitignore` (but do not do this as part of unrelated work).
- Avoid committing changes inside `build/`/`dist/` unless explicitly requested.

## Platform notes

- The default `A` root path in the UI is Windows-style (`Y:/...`). When running on macOS/Linux, change it to a valid local path.
- The PyInstaller spec uses `console=False` (windowed app). Enable a console only if you explicitly need stdout/stderr for debugging.
