from typing import Literal

import numpy as np
import pandas as pd
from numpy import ndarray
from pandera.typing.pandas import DataFrame
from ribasim import Model
from ribasim.nodes import basin
from ribasim.schemas import BasinConcentrationSchema


def mfms_budget_to_fraction(budget: str, prefix: str) -> str:
    if "_" in budget:
        bdg_part, sys_part = budget.split("_")
        return f"{prefix}_{bdg_part.lstrip('bdg') + sys_part.lstrip('sys')}"
    else:
        return f"{prefix}_{budget.lstrip('bdg').rstrip('m3')}"


def make_budget_sub_fraction_table(
    fractions_df: pd.DataFrame,
    basin_ids: np.ndarray,
    budget: str,
    prefix: str,
    basin_influx: Literal["drainage", "surface_runoff"] = "drainage",
) -> DataFrame[BasinConcentrationSchema]:
    """Build sub-fraction table (sum of concentrations in one basin-influx == 1)

    Parameters
    ----------
    fractions_df : pd.DataFrame
        Fraction table used as concentrations
    basin_ids : np.ndarray
        Array with basin_ids
    budget : str
        Set of MODFLOW-MetaSWAP budget
    prefix : str
        Prefix to add before the budget fraction in 'substance', so <prefix>_<budget>
    basin_influx : Literal["drainage", "surface_runoff", "precipitation"], optional
        Basin influx term to compute the fraction for, by default "drainage"

    Returns
    -------
    DataFrame[BasinConcentrationSchema]
        A validated basin.concentration.df fraction table
    """
    # not all basins are large enough to get a budget. That's not an issue; no flux is no fraction
    missing_basin_ids = list(set(basin_ids) - set(fractions_df.index.get_level_values("node_id").unique()))
    mask = ~np.isin(basin_ids, list(missing_basin_ids))
    df = (
        fractions_df.loc[basin_ids[mask]]
        .reset_index()[["node_id", "time", budget]]
        .rename(columns={budget: basin_influx})
    )

    # set fraction labels as substance
    df["substance"] = mfms_budget_to_fraction(budget, prefix=prefix)

    # set all other concentrations to 0
    for influx in ["surface_runoff", "precipitation", "drainage"]:
        if influx != basin_influx:
            df[influx] = float(0)
    return BasinConcentrationSchema.validate(df)


def compute_budget_fractions(
    fractions_df: pd.DataFrame,
    basin_ids: np.ndarray,
    budgets: set[str],
    prefix: str,
    basin_influx: Literal["drainage", "surface_runoff", "precipitation"] = "drainage",
) -> list[DataFrame[BasinConcentrationSchema]]:
    """Build concentration table from budget fractions.

    Parameters
    ----------
    fractions_df : pd.DataFrame
        Fraction table used as concentrations
    basin_ids : np.ndarray
        Array with basin_ids
    budgets : set[str]
        Set of MODFLOW-MetaSWAP budgets
    prefix : str
        Prefix to add before the budget fraction in 'substance', so <prefix>_<budget>
    basin_influx : Literal["drainage", "surface_runoff", "precipitation"], optional
        Basin influx term to compute the fraction for, by default "drainage"

    Returns
    -------
    list[DataFrame[BasinConcentrationSchema]]
        List with one or more validated basin.concentration.df fraction tables
    """
    # if we only have 1 budget, we don't need to compute fractions
    if len(budgets) == 1:
        time = fractions_df.index.get_level_values("time").min()

        def _get_values(
            column: str, basin_influx: Literal["drainage", "precipitation", "surface_runoff"], basin_ids: ndarray
        ) -> list[int]:
            if column == basin_influx:
                return [1] * len(basin_ids)
            else:
                return [0] * len(basin_ids)

        df = basin.Concentration(
            node_id=basin_ids,
            time=[time] * len(basin_ids),
            substance=[mfms_budget_to_fraction(i, prefix=prefix) for i in budgets] * len(basin_ids),
            drainage=_get_values("drainage", basin_influx, basin_ids),
            precipitation=_get_values("precipitation", basin_influx, basin_ids),
            surface_runoff=_get_values("surface_runoff", basin_influx, basin_ids),
        ).df

        assert df is not None  # ensure not type warning
        return [df]
    else:
        return [make_budget_sub_fraction_table(fractions_df, basin_ids, budget, prefix=prefix) for budget in budgets]


def assign_fractions_from_budgets(
    model: Model,
    budgets_df: pd.DataFrame,
    primary_budgets: set[str],
    secondary_budgets: set[str],
    surface_runoff_budgets: set[str],
    secondary_basin_ids: np.ndarray,
    primary_basin_ids: np.ndarray,
    prefix: str,
) -> None:
    """Assing fractions to the concentration table from MODFLOW-MetaSWAP budgets.

    Note(!) budgets_df is returned by

    Parameters
    ----------
    budgets_df : pd.DataFrame
        MODFLOW-MetaSWAP budgets per basin over time
    primary_budgets : set[str]
        set of budgets that are to be summed to primary drainage/infiltration, e.g. {"bdgriv_sys1", "bdgriv_sys4", "bdgriv_sys5"}
    secondary_budgets : set[str]
        set of budgets that are to be summed to secondary drainage/infiltration, e.g. {"bdgriv_sys2", "bdgriv_sys3", "bdgriv_sys6", "bdgdrn_sys1", "bdgdrn_sys2", "bdgdrn_sys3", "bdgpsswm3"}
    surface_runoff_budgets: set[str]
        set of budgets that are to be summed to secondary surface_runoff, e.g. {"bdgqrunm3"}
    secondary_basin_ids: np.ndarray
        list of basin_ids that belong to secondary basins
    primary_basin_ids: np.ndarray
        list of basin_ids that belong to primary basins
    prefix: str
        prefix to add to a fraction substance


    """
    # fractions primary_drainage
    primary_drainage_bdg_df = budgets_df[list(primary_budgets)].clip(upper=0).abs()
    secondary_drainage_bdg_sum = pd.Series(primary_drainage_bdg_df.sum(axis=1))
    mask = primary_drainage_bdg_df.notna() & (primary_drainage_bdg_df != 0)
    primary_drainage_fractions_df = primary_drainage_bdg_df.div(secondary_drainage_bdg_sum, axis=0).where(mask, 0)

    # fractions secondary_drainage
    secondary_drainage_bdg_df = budgets_df[list(secondary_budgets)].clip(upper=0).abs()
    secondary_drainage_bdg_sum = pd.Series(secondary_drainage_bdg_df.sum(axis=1))
    mask = secondary_drainage_bdg_df.notna() & (secondary_drainage_bdg_df != 0)
    secondary_drainage_fractions_df = secondary_drainage_bdg_df.div(secondary_drainage_bdg_sum, axis=0).where(mask, 0)

    # fractions surface_runoff
    surface_runoff_bdg_df = budgets_df[list(surface_runoff_budgets)].clip(upper=0).abs()
    surface_runoff_bdg_sum = pd.Series(surface_runoff_bdg_df.sum(axis=1))
    mask = surface_runoff_bdg_df.notna() & (surface_runoff_bdg_df != 0)
    surface_runoff_fractions_df = surface_runoff_bdg_df.div(surface_runoff_bdg_sum, axis=0).where(mask, 0)

    # concat all fractions and add to model.basin.concentration.table
    fractions_df = pd.concat(
        compute_budget_fractions(
            fractions_df=primary_drainage_fractions_df,
            basin_ids=primary_basin_ids,
            budgets=primary_budgets,
            basin_influx="drainage",
            prefix=prefix,
        )
        + compute_budget_fractions(
            fractions_df=secondary_drainage_fractions_df,
            basin_ids=secondary_basin_ids,
            budgets=secondary_budgets,
            basin_influx="drainage",
            prefix=prefix,
        )
        + compute_budget_fractions(
            fractions_df=surface_runoff_fractions_df,
            basin_ids=secondary_basin_ids,
            budgets=surface_runoff_budgets,
            basin_influx="surface_runoff",
            prefix=prefix,
        ),
        ignore_index=True,
    )

    fractions_df["meta_lhm_fractions"] = True
    model.basin.concentration.df = BasinConcentrationSchema.validate(fractions_df)
