# %%
from pathlib import Path

import geopandas as gpd

from ribasim_nl.model import Model
from ribasim_nl.parametrization.conversions import round_to_precision
from ribasim_nl.parametrization.empty_table import empty_table_df


def update_manning_resistance_static(
    model: Model,
    profiles_gpkg: Path | None = None,
    profile_slope: float = 1,
    profile_width: float = 25,
    manning_n: float = 0.04,
):
    """Generate a default manning-table.

    Args:
        model (Model): Ribasim Model
        profile_slope (float, optional): Slope of the cross section talud. Defaults to 1.
        profile_width (float, optional): _description_. Defaults to 25.
        manning_n (float, optional): _description_. Defaults to 0.04.

    Returns
    -------
        pd.DataFrame: dataframe for static Manning-table in Ribasim model
    """
    # empty dataframe
    static_df = empty_table_df(model=model, node_type="ManningResistance", table_type="Static")

    # length from length edges
    length = [
        round_to_precision(
            model.edge.df[(model.edge.df.from_node_id == node_id) | (model.edge.df.to_node_id == node_id)].length.sum(),
            precision=10,
        )
        for node_id in static_df.node_id
    ]
    static_df.loc[:, "length"] = length

    # slope and width from profiles geopackage else defaults
    if profiles_gpkg:
        profiles_df = gpd.read_file(profiles_gpkg).set_index("profiel_id")
        profile_ids = [
            model.edge.df.set_index("to_node_id").at[i, "meta_profielid_waterbeheerder"] for i in static_df.node_id
        ]
        static_df.loc[:, "profile_slope"] = profiles_df.loc[profile_ids]["profile_slope"].to_numpy()
        static_df.loc[:, "profile_width"] = profiles_df.loc[profile_ids]["profile_width"].to_numpy()
    else:
        static_df.loc[:, "profile_slope"] = profile_slope
        static_df.loc[:, "profile_width"] = profile_width

    # manning_n
    static_df.loc[:, "manning_n"] = manning_n

    model.manning_resistance.static.df = static_df
