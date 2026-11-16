"""The no-data levels and wrong levels of the delivered target levels of the peilgebieden, and their cleaning.

The levels stay as delivered in the postprocessed data and the crossings, of which they are an input, so they are
cleaned where they are used: for the target levels of the refined aggregation areas, see
`ribasim_nl.crossings.aggregation`, and for the depth of the profiles, see `ribasim_nl.profiles.gen_profiles`.
"""

from collections.abc import Collection, Mapping

import numpy as np
import pandas as pd

# The levels that mean that the target level of a peilgebied is missing, per dataset. 0 m NAP is a real level.
NO_DATA_LEVELS: dict[str, tuple[float, ...]] = {"Scheldestromen": (-99.0, 99.0), "Zuiderzeeland": (999.0,)}
# Wrong target levels and their corrections, per dataset. The Rijnland boezem has the level of 2024, 0.61 m, in the
# crossings, though it is -0.61 m NAP, see `pipeline/rijnland/00_postprocess.py`.
LEVEL_CORRECTIONS: dict[str, dict[float, float]] = {"Rijnland": {0.61: -0.61}}
# The peilgebieden that have a target level are all below this, in m NAP, and above minus this
MAX_TARGET_LEVEL = 50.0


def clean_target_levels(
    level: pd.Series, no_data_levels: Collection[float] = (), corrections: Mapping[float, float] | None = None
) -> pd.Series:
    """Remove the no-data levels and correct the wrong levels of a dataset.

    :param level: the delivered target levels
    :param no_data_levels: levels that mean that the target level is missing in this dataset, see `NO_DATA_LEVELS`
    :param corrections: wrong levels in this dataset and their corrections, see `LEVEL_CORRECTIONS`
    :return: the target levels, NaN where it is missing

    :raises AssertionError: if a target level is out of range, and so probably another no-data level, or a correction
        corrects no level
    """
    level = level.astype(float)
    level = level.where(~level.isin(list(no_data_levels)))
    for wrong, right in (corrections or {}).items():
        is_wrong = np.isclose(level, wrong)
        assert is_wrong.any(), f"No target level is {wrong}, remove its correction"
        level = level.mask(is_wrong, right)
    invalid = level[level.abs() > MAX_TARGET_LEVEL]
    assert invalid.empty, f"Target levels out of range, are these no-data?\n{invalid}"
    return level
