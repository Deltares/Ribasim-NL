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
3. The largest differences in the simulated results, see below.

When you cannot run commands, as in the unattended catch-up runs, the prompt names a file with the metrics and pull requests. Read that, do not compute anything else, and say in the description that the results were not compared.

## Compare the results

The metrics are aggregates: medians, and a few key locations. They can hide large local changes. In #967 they moved by less than 1%, while one changed coupling dropped a reach of the Zuid-Willemsvaart by 2 m in dry periods and dried out basins in Aa en Maas. So also compare the results themselves, Basin by Basin and link by link, even when the metrics look quiet.

The results of this run are in `data/Rijkswaterstaat/runs/lhm_coupled`. Those of the base are usually in the local DVC cache, without pulling anything: `git show <base>:dvc.lock` gives the md5 of that directory, and `<dvc cache dir>/files/md5/<first 2 characters>/<rest>.dir` is a JSON list of its files with their md5, stored the same way. Use `results/basin.nc`, `results/flow.nc`, `results/solver_stats.nc` and `input/database.gpkg`. If they are not in the cache, pull them in a separate worktree, see `docs/dev/dvc.qmd`. Write the comparison scripts in the scratchpad, and save intermediate tables, since reading the results takes a while.

First match the two runs. Match Basins by `node_id`, and links by their `from_node_id` and `to_node_id`, since link IDs and the IDs of coupling Junctions can shift. Check that a node ID still has the same type and location. The authority of a node is `node_id // 100000`. Then look at:

- **Levels.** The mean, 1st and 99th percentile of all Basins, with the storage Basins (`meta_categorie` is `bergend`) apart, since their levels follow their profile. Count the changes over 0.1 and 0.5 m per authority and list the largest, also for the Rijkswaterstaat Basins, of which the metrics only cover a few. Look for groups: Basins that shift together, and a 1st percentile that moves while the 99th stays, which points at supply in dry periods. Look at which months differ. Compare with the target level `meta_streefpeil`, and with the bottom of the profile to see Basins that run dry.
- **Flows.** The mean per link, over the whole run and over a dry period such as August to November 2018. Add the net exchange between authorities, and the connections between authorities that exist in only one run, tracing through the coupling Junctions.
- **Water balance.** Precipitation, evaporation, drainage and infiltration per authority, compared with the change in basin area.
- **Inputs.** Which nodes differ in the tables of the model, such as `Basin / profile`, `Basin / area` and the static tables, and the largest relative changes in basin area, including Basins left with almost none.
- **Observations.** The KGE, NSE and bias of each Observation, not only the medians, from `observation_metrics.parquet` of `data/Rijkswaterstaat/webmap/lhm_coupled/results`. The bias shows whether a location gets more or less water.
- **Solver.** The computation time per day, to see whether a change comes from a few slow days.

Trace each large difference to its cause: follow the flow upstream along the largest change, until you reach a changed connection, parameter or basin area. A change that is merely nearby is not a cause, so confirm that the water actually takes that path. A difference usually has one source with many Basins downstream of it, so report the source, and how many Basins follow from it. If a connection between authorities changed, judge which one is right: look at which watercourse of HyDAMO reaches the boundary, and at `forced_coupling` in `pipeline/09_koppelen_modellen.py`. Note the Basins that are already far above their target level in the base, since these are sensitive to small changes.

## Write

Write in the style of the earlier pull requests, such as #927 and #933: short, factual, in plain sentences, with numbers rather than adjectives.

1. For a catch-up run, start with one sentence that it reproduces `main` with the command it ran, such as `pixi run repro-local --incremental`, followed by the pull requests it adds, as a list of their titles with their numbers. Then add the link to the model viewer, `**[View the model of this pull request](https://ribasim.nl/viewer?pr=<number>)**, compared with \`main\`.`, if the number of the pull request is known.
2. Add a section `## Effect on the coupled model` with a table of the base and this run, with the columns `` `main` `` and `this PR` for a pull request, and the previous run and this run for a catch-up run. Include the computation time, right-hand side calls, mean timestep, 5th percentile of the daily mean timestep, rejected timesteps, and the median NSE and KGE of the compared Observations, with their number. Format the values for reading: `2.2 h`, `1.55M`, `260 s`, `7.4%`.
3. Follow it with bullets on what changed most:
   - the key discharge whose KGE changed most, with its old and new value, and the largest change of the others;
   - the key Basin levels whose 1st or 99th percentile changed most, in m, and the largest change of the others;
   - anything that stands out, such as a large change in computation time or rejected timesteps, a metric that got worse, or a change in the number of compared Observations.
   When nothing changed, say so in one sentence instead.
3. Add a section `## Largest differences in the results`, with what stood out in the comparison above, largest effect first. For each difference give the numbers, where it is, how many Basins follow from it, and its cause if the results show it. Name what you could not explain, and what measurements cannot confirm. When the results hardly differ, say so in one sentence.
4. If a change looks wrong or needs someone who knows the area, end with **Please check:** and a list.
5. Only relate a change to a pull request when the metrics or results show it, and say how. Do not guess causes.

Print only the Markdown of the description, without an introduction or closing remarks. End with:

```
🤖 Generated with [Claude Code](https://claude.com/claude-code)
```
