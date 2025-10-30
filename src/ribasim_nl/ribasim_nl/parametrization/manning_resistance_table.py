from pathlib import Path

import geopandas as gpd
from pandas import Timestamp

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
            model.link.df[(model.link.df.from_node_id == node_id) | (model.link.df.to_node_id == node_id)].length.sum(),
            precision=10,
        )
        for node_id in static_df.node_id
    ]
    static_df.loc[:, "length"] = length

    # slope and width from profiles geopackage else defaults
    if profiles_gpkg:
        profiles_df = gpd.read_file(profiles_gpkg).set_index("profiel_id")
        profile_ids = [
            model.link.df.set_index("to_node_id").at[i, "meta_profielid_waterbeheerder"] for i in static_df.node_id
        ]
        static_df.loc[:, "profile_slope"] = profiles_df.loc[profile_ids]["profile_slope"].to_numpy()
        static_df.loc[:, "profile_width"] = profiles_df.loc[profile_ids]["profile_width"].to_numpy()
    else:
        static_df.loc[:, "profile_slope"] = profile_slope
        static_df.loc[:, "profile_width"] = profile_width

    # manning_n
    static_df.loc[:, "manning_n"] = manning_n

    model.manning_resistance.static.df = static_df


def calculate_flow_rate(
    depth: float, profile_width: float, profile_slope: float, manning_n: float, slope: float
) -> float:
    """Calculate expeted discharge

    Args:
        depth (float): depth in profile [m]
        profile_width (float): bottom-width of profile [m]
        profile_slope (float): profile embankment slope [-]
        manning_n (float): manning roughness coefficient [s/m^(1/3)]
        slope (float): longitudinal slope as part of manning formula [-]

    Returns
    -------
        float: discharge [m3/s]
    """
    if depth == 0:
        return 0  # No flow at zero depth
    A = (profile_width + profile_slope * depth) * depth  # Cross-sectional area
    P = profile_width + 2 * depth * (1 + profile_slope**2) ** 0.5  # Wetted perimeter
    R = A / P  # Hydraulic radius
    K = (1 / manning_n) * A * (R ** (2 / 3))  # Conveyance
    Q = K * (slope**0.5)  # Discharge
    return Q


def manning_flow_rate(model: Model, manning_node_id: int, at_timestamp: Timestamp | None = None) -> float:
    """Calculate the expected discharge at a timestamp

    Args:
        model (ribasim_nl.Model): ribasim-model including manning_node_id
        manning_node_id (int): node_id of manning-node
        at_timestamp (Timestamp | None, optional): timestamp, defaults to final timestamp in simulation. Defaults to None.

    Returns
    -------
        float: discharge [m3/s]
    """
    # get at_timestamp if not defined
    if at_timestamp is None:
        at_timestamp = model.basin_results.df.index.max()

    # get upstream and downstream basins
    us_basin_node_id = model.upstream_node_id(manning_node_id)
    ds_basin_node_id = model.downstream_node_id(manning_node_id)

    # get slope
    df = model.basin_results.df.loc[at_timestamp].set_index("node_id")
    delta_h = df.at[us_basin_node_id, "level"] - df.at[ds_basin_node_id, "level"]
    slope = abs(delta_h / model.manning_resistance.static.df.set_index("node_id").at[manning_node_id, "length"])

    # get depth as in https://github.com/Deltares/Ribasim/blob/1773acf71857ab05390a60626fbf96dd4ccae740/core/src/solve.jl#L529-L532
    water_level = (df.at[us_basin_node_id, "level"] + df.at[ds_basin_node_id, "level"]) / 2
    bottom_level = (
        model.basin.profile.df.set_index("node_id").loc[us_basin_node_id, "level"].min()
        + model.basin.profile.df.set_index("node_id").loc[ds_basin_node_id, "level"].min()
    ) / 2
    depth = water_level - bottom_level

    q = calculate_flow_rate(
        depth=depth,
        profile_width=model.manning_resistance.static.df.set_index("node_id").at[manning_node_id, "profile_width"],
        profile_slope=model.manning_resistance.static.df.set_index("node_id").at[manning_node_id, "profile_slope"],
        manning_n=model.manning_resistance.static.df.set_index("node_id").at[manning_node_id, "manning_n"],
        slope=slope,
    )

    return q
