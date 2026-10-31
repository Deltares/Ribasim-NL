# AGENTS.md

## Goal

Build and maintain a reproducible [DVC (Data Version Control)](https://dvc.org/) pipeline that turns source data and Python code into a nationwide [Ribasim](https://github.com/Deltares/Ribasim) schematization.

## Working agreements

- Use Pixi for commands and dependencies.
- Only support the Python and dependency versions in the lockfile; do not use compatibility features such as `from __future__ import annotations`.
- In a new git worktree, run `pixi run setup-worktree` before running stages or model runs. It shares the `.env` credentials and DVC cache of the main checkout and installs the Ribasim core, without pulling data. Do not run `pixi run install` in a worktree: it points the git hooks, which all worktrees share, to that worktree.
- Define reproducible stages in `dvc.yaml`, with complete inputs and outputs. Stages depend on their scripts, and on the `src/ribasim_nl/ribasim_nl` package as a whole, since every stage imports most of it.
- Run only the smallest relevant DVC stage while developing. Do not run complete reproductions unless asked; they are started manually, locally with `pixi run repro-local` or on SLURM with `repro.sh`.
- Follow `docs/dev/pull-requests.qmd` for the full runs of pull requests. In short: `dvc.lock` on main is the latest reproduced state and may lag the code. Push the outputs of every committed run. When main's `dvc.lock` changed since the base of a run, keep main's `dvc.lock` and `data/Rijkswaterstaat/metrics/lhm_coupled.json` together, without rerunning, also if git merges them cleanly; never combine two runs by hand. Catch-up runs on main, `pixi run repro-main`, record the merged pull requests. To start one right away, like the nightly timer does, run `systemctl --user start --no-block ribasim-nl-repro-main`; it opens the pull request, publishes the model viewer preview and describes the run. To redo a run of main with all stages, see `docs/dev/pull-requests.qmd`. Describe the effect of a run with the `describe-run` skill.
- Pull requests are squash merged. In a stack, once the pull request below is merged, replay only the commits of the next one on main, with `git rebase --onto origin/main <head commit of the merged pull request>` and `git push --force-with-lease`; do not merge main into it. If that conflicts in `dvc.lock`, keep main's, as above.
- Run `pixi run check` before finishing; it includes Ruff linting and formatting, and ty type checking.
- Add tests for new behavior where practical.
- Add type hints and concise docstrings to new Python code.
- Use defensive programming: validate inputs and invariants, and assert assumptions close to where they are made.
- Add reusable model-wide checks to `ribasim_nl.validation.validate_model`, with regression tests.
- Strive for DRY (don't repeat yourself) code without introducing unnecessary abstractions.
- Keep changes focused and maintainable; reuse existing repository patterns.
- This repository is public, but the code is not used outside this repository. Backward compatibility is not required, so prefer clear improvements over preserving obsolete APIs.

## Model conventions

- Node IDs in the coupled LHM model, and in the model viewer, are the `waterbeheercode` of the authority (`ribasim_nl.aquo`) times 100 000 plus the node ID in the model of that authority. For instance, 203892 is node 3892 of Wetterskip Fryslân. Fix an authority's nodes in its own stages where possible, rather than in `koppelen`.
- Scripts and edit files refer to nodes, links and basin areas by ID, so code that numbers them must give the same IDs on every run, with any library version or CPU: sort with `kind="stable"`. See `docs/dev/reproducing-models.qmd`.
- Analyse model behavior with the results of the latest coupled run in `data/Rijkswaterstaat/runs/lhm_coupled/results` before changing the model; a new coupled run takes about 1.5 hours.
