# AGENTS.md

## Goal

Build and maintain a reproducible [DVC (Data Version Control)](https://dvc.org/) pipeline that turns source data and Python code into a nationwide [Ribasim](https://github.com/Deltares/Ribasim) schematization.

## Working agreements

- Use Pixi for commands and dependencies.
- Define reproducible stages in `dvc.yaml`, with complete inputs and outputs.
- Run only the smallest relevant DVC stage while developing. Never run `pixi run repro`; complete reproduction runs are submitted to SLURM manually.
- Run `pixi run check` before finishing; it includes Ruff linting and formatting, and ty type checking.
- Add tests for new behavior where practical.
- Add type hints and concise docstrings to new Python code.
- Use defensive programming: validate inputs and invariants, and assert assumptions close to where they are made.
- Add reusable model-wide checks to `ribasim_nl.validation.validate_model`, with regression tests.
- Strive for DRY (don't repeat yourself) code without introducing unnecessary abstractions.
- Keep changes focused and maintainable; reuse existing repository patterns.
- This repository is public, but the code is not used outside this repository. Backward compatibility is not required, so prefer clear improvements over preserving obsolete APIs.
