"""Read the properties of Rijkswaterstaat structures (kunstwerken) from their Excel sheets.

A sheet has `Eigenschap` (property) and `Waarde` (value) columns, with general properties in the first
rows, optionally followed by tables like a Q(h) or QQ relation.
"""

from typing import Any

import pandas as pd
from ribasim.nodes import pump, tabulated_rating_curve

FLOW_KWARGS = {
    "Capaciteit (m3/s)": "flow_rate",
    "Minimale capaciteit (m3/s)": "min_flow_rate",
    "Maximale capaciteit (m3/s)": "max_flow_rate",
    "Streefpeil bovenstrooms (m +NAP)": "min_upstream_level",
    "Streefpeil benedenstrooms (m +NAP)": "max_downstream_level",
}


def read_rating_curve(kwk_df: pd.DataFrame) -> tabulated_rating_curve.Static:
    """Q(h) relation of a structure as TabulatedRatingCurve."""
    qh_df = kwk_df.loc[kwk_df.Eigenschap.to_list().index("Q(h) relatie") + 2 :][["Eigenschap", "Waarde"]].rename(
        columns={"Eigenschap": "level", "Waarde": "flow_rate"}
    )
    qh_df.dropna(inplace=True)
    return tabulated_rating_curve.Static(level=qh_df["level"].to_list(), flow_rate=qh_df["flow_rate"].to_list())


def read_qq_curve(kwk_df: pd.DataFrame) -> pd.DataFrame:
    """QQ relation of a structure: flow rate as function of a condition flow rate."""
    return kwk_df.loc[kwk_df.Eigenschap.to_list().index("QQ relatie") + 2 :][["Eigenschap", "Waarde"]].rename(
        columns={"Eigenschap": "condition_flow_rate", "Waarde": "flow_rate"}
    )


def read_kwk_properties(kwk_df: pd.DataFrame) -> pd.Series:
    """General properties of a structure."""
    properties = kwk_df[0:12][["Eigenschap", "Waarde"]].dropna().set_index("Eigenschap")["Waarde"]

    if "Kunstwerkcode" in properties:
        properties["Kunstwerkcode"] = str(properties["Kunstwerkcode"])
    return properties


def read_flow_kwargs(kwk_properties: pd.Series) -> dict[str, Any]:
    """Flow rates and levels of a structure, as keyword arguments for Outlet or Pump static tables."""
    kwargs = kwk_properties.rename(FLOW_KWARGS).to_dict()
    if "flow_rate" in kwargs:
        kwargs["max_flow_rate"] = kwargs["flow_rate"]
    return {str(k): [v] for k, v in kwargs.items() if k in FLOW_KWARGS.values()}


def read_pump(kwk_properties: pd.Series) -> pump.Static:
    """Static table of a pump."""
    return pump.Static(**read_flow_kwargs(kwk_properties))
