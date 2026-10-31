---
name: describe-run
description: Describe the effect of a full pipeline run on the coupled LHM model, for the description of its pull request. Use after `pixi run repro-local` on a pull request, and for the catch-up runs on main of `pixi run repro-main`.
---

# Describe the effect of a run

Write the section on the effect of a full run of the pipeline on the coupled model, compared with the run it replaces. See `docs/dev/pull-requests.qmd` for how runs of pull requests and of `main` are recorded.

## Gather

The base is the run that this run is compared with:

- For a pull request, the base of its run: `git merge-base HEAD origin/main`, or in a stack the branch of the pull request below it.
- For a catch-up run on `main`, the commit of `main` it reproduced.

When you can run commands, gather:

1. The metrics compared with the base: `pixi run dvc metrics diff --md --all <base>`. They come from `validate_lhm_coupled`, see `ribasim_nl.result_validation`.
2. For a catch-up run, the pull requests it adds: `git log --format='- %s' <previous>..<base>`, where `<previous>` is the last commit that changed `dvc.lock`, `git log -1 --format=%H <base> -- dvc.lock`.
3. For changes that the metrics do not explain, the results in `data/Rijkswaterstaat/runs/lhm_coupled/results`, compared with those of the base. Pull the results of the base in a separate worktree, see `docs/dev/dvc.qmd`.

When you cannot run commands, as in the unattended catch-up runs, the prompt names a file with the metrics and pull requests. Read that, and do not compute anything else.

## Write

Write in the style of the earlier pull requests, such as #927 and #933: short, factual, in plain sentences, with numbers rather than adjectives.

1. For a catch-up run, start with one sentence that it reproduces `main` with the command it ran, such as `pixi run repro-local --incremental`, followed by the pull requests it adds, as a list of their titles with their numbers. Then add the link to the model viewer, `**[View the model of this pull request](https://ribasim.nl/viewer?pr=<number>)**, compared with \`main\`.`, if the number of the pull request is known.
2. Add a section `## Effect on the coupled model` with a table of the base and this run, with the columns `` `main` `` and `this PR` for a pull request, and the previous run and this run for a catch-up run. Include the computation time, right-hand side calls, mean timestep, 5th percentile of the daily mean timestep, rejected timesteps, and the median NSE and KGE of the compared Observations, with their number. Format the values for reading: `2.2 h`, `1.55M`, `260 s`, `7.4%`.
3. Follow it with bullets on what changed most:
   - the key discharge whose KGE changed most, with its old and new value, and the largest change of the others;
   - the key Basin levels whose 1st or 99th percentile changed most, in m, and the largest change of the others;
   - anything that stands out, such as a large change in computation time or rejected timesteps, a metric that got worse, or a change in the number of compared Observations.
   When nothing changed, say so in one sentence instead.
4. Only relate a change to a pull request when the metrics or results show it, and say how. Do not guess causes.

Print only the Markdown of the description, without an introduction or closing remarks. End with:

```
🤖 Generated with [Claude Code](https://claude.com/claude-code)
```
