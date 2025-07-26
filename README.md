# SFTP Service Demo

Python project managed by `uv`.

## Installation

Install go-task:
```bash
brew install go-task/tap/go-task
```

## Usage

Run the SFTP service:
```bash
uv run src/main.py
```

Available tasks:
- `task unit-test` - Run unit tests
- `task lint` - Run linter with auto-fix
- `task format` - Format code
- `task typecheck` - Run type checks
- `task check-all` - Run all checks (lint, format, typecheck, test)

## Quick Start

Run all checks:
```bash
task check-all
```
