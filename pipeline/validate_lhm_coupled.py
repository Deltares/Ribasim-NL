"""Check the results of `run_lhm_coupled` for large regressions, see `ribasim_nl.result_validation`.

The metrics are written to a DVC metrics file that is tracked by git, so `pixi run dvc metrics diff main` shows how
they changed, and the comparison per Observation node to a CSV file. Both are written before the check, so they can
be inspected when it fails.
"""

import json

from ribasim_nl.result_validation import result_metrics, validate_results
from ribasim_nl.settings import settings

root = settings.ribasim_nl_data_dir / "Rijkswaterstaat"
toml_path = root / "runs/lhm_coupled/lhm_coupled.toml"
metrics_dir = root / "metrics"

metrics, comparison = result_metrics(toml_path)
metrics_dir.mkdir(exist_ok=True)
# Four significant digits keep the diffs readable
rounded = {group: {name: float(f"{value:.4g}") for name, value in values.items()} for group, values in metrics.items()}
(metrics_dir / "lhm_coupled.json").write_text(json.dumps(rounded, indent=2, ensure_ascii=False) + "\n")
comparison.to_csv(metrics_dir / "lhm_coupled_observations.csv")
validate_results(metrics)
