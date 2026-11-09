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
3. The largest differences in the simulated results: `pixi run compare-runs --base <base>`, see below.

When you cannot run commands, as in the unattended catch-up runs, the prompt names a file with the metrics, pull requests and the comparison of the results. Read that, and do not compute anything else.

## Compare the results

The metrics are aggregates: medians, and a few key locations. They can hide large local changes. In #967 they moved by less than 1%, while one changed coupling dropped a reach of the Zuid-Willemsvaart by 2 m in dry periods and dried out basins in Aa en Maas. So also compare the results themselves, Basin by Basin and link by link, even when the metrics look quiet.

`compare-runs`, see `ribasim_nl.run_comparison`, reads the results of both runs and lists the largest differences. It matches nodes by their type and location, since IDs can shift, and uses the IDs of this run in its tables. Look at:

- **Levels.** The storage Basins (`meta_categorie` is `bergend`) are apart, since their levels follow their profile. Look for groups: Basins that shift together, and a 1st percentile that moves while the 99th stays, which points at supply in dry periods. Look at which months differ, how far Basins are above their target level, and which run dry.
- **Flows.** The same change along consecutive links is one change. Also look at the net exchange between authorities, and the connections between authorities and links that exist in only one run. A link can be in only one run because a Junction was added on it.
- **Water balance.** Precipitation, evaporation, drainage and infiltration per authority, compared with the change in basin area, including the Basins in only one run.
- **Observations.** The bias shows whether a location gets more or less water.
- **Solver.** Whether a change in computation time comes from a few slow days.

When you can run commands, trace each large difference to its cause: follow the flow upstream along the largest change, until you reach a changed connection, parameter or basin area. The results of this run are in `data/Rijkswaterstaat/runs/lhm_coupled`, and `compare-runs` keeps those of the base in `.cache/viewer/revisions/<commit>`. Write further scripts in the scratchpad. Without commands, only use what the comparison shows: the `input` column names what changed at a Basin itself, so it need not be the cause. A change that is merely nearby is not a cause, so confirm that the water actually takes that path. A difference usually has one source with many Basins downstream of it, so report the source, and how many Basins follow from it. If a connection between authorities changed, judge which one is right: look at which watercourse of HyDAMO reaches the boundary, and at `forced_coupling` in `pipeline/09_koppelen_modellen.py`. Note the Basins that are already far above their target level in the base, since these are sensitive to small changes.

## Write

Write in the style of the earlier pull requests, such as #927 and #933: short, factual, in plain sentences, with numbers rather than adjectives.

1. For a catch-up run, start with one sentence that it reproduces `main` with the command it ran, such as `pixi run repro-local --incremental`, followed by the pull requests it adds, as a list of their titles with their numbers. Then add the link to the model viewer, `**[View the model of this pull request](https://ribasim.nl/viewer?pr=<number>)**, compared with \`main\`.`, if the number of the pull request is known.
2. Add a section `## Effect on the coupled model` with a table of the base and this run, with the columns `` `main` `` and `this PR` for a pull request, and the previous run and this run for a catch-up run. Include the computation time, right-hand side calls, mean timestep, 5th percentile of the daily mean timestep, rejected timesteps, and the median NSE and KGE of the compared Observations, with their number. Format the values for reading: `2.2 h`, `1.55M`, `260 s`, `7.4%`.
3. Follow it with bullets on what changed most:
   - the key discharge whose KGE changed most, with its old and new value, and the largest change of the others;
   - the key Basin levels whose 1st or 99th percentile changed most, in m, and the largest change of the others;
   - anything that stands out, such as a large change in computation time or rejected timesteps, a metric that got worse, or a change in the number of compared Observations.
   When nothing changed, say so in one sentence instead.
3. Add a section `## Largest differences in the results`, with what stood out in the comparison above, largest effect first. For each difference give the numbers, where it is, how many Basins follow from it, and its cause if the results show it. Name what you could not explain, and what measurements cannot confirm. When the results hardly differ, say so in one sentence. When the results were not compared, leave out this section, and say so in the section above.
4. If a change looks wrong or needs someone who knows the area, end with **Please check:** and a list.
5. Only relate a change to a pull request when the metrics or results show it, and say how. Do not guess causes.

Print only the Markdown of the description, without an introduction or closing remarks. End with:

```
🤖 Generated with [Claude Code](https://claude.com/claude-code)
```
