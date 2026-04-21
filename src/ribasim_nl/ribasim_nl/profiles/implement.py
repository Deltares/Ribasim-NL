"""Implement profiles in model generation."""

import logging

import pandas as pd
import shapely

from ribasim_nl import CloudStorage, Model

LOG = logging.getLogger(__name__)


def get_tables(water_authority: str, cloud: CloudStorage = CloudStorage()) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Get profile tables

    :param water_authority: water authority
    :param cloud: the GoodCloud-storage, defaults to CloudStorage()

    :type water_authority: str
    :type cloud: CloudStorage, optional

    :return: profile tables (flowing/'doorgaand' and storing/'bergend')
    :rtype: tuple[pandas.DataFrame, pandas.DataFrame]

    :raises FileNotFoundError: if profile *.csv-file(s) cannot be found
    """
    # verify existence of tables
    wd = cloud.joinpath(water_authority, "verwerkt", "profielen")
    fn_flowing = wd / "profielen_doorgaand.csv"
    fn_storing = wd / "profielen_bergend.csv"
    if not fn_flowing.exists() or not fn_storing.exists():
        LOG.critical(f"{fn_flowing.exists()=}: {fn_flowing=}")
        LOG.critical(f"{fn_storing.exists()=}: {fn_storing=}")
        msg = "Profiles not (yet) preprocessed"
        raise FileNotFoundError(msg)

    # read profile data
    df_flowing = pd.read_csv(wd / fn_flowing)
    df_storing = pd.read_csv(wd / fn_storing)

    # return profile tables
    return df_flowing, df_storing


def minimum_surface_area(table: pd.DataFrame, min_area: float) -> pd.DataFrame:
    """Filter profiles based on a minimum surface area.

    :param table: table with profiles
    :param min_area: minimum surface area [m2]

    :type table: pandas.DataFrame
    :type min_area: float

    :return: filtered table with profiles
    :rtype: pandas.DataFrame
    """
    assert all(c in table.columns for c in ["node_id", "level", "area"])
    surface_area = table.loc[table.groupby("node_id")["level"].idxmax(), ["node_id", "area"]]
    valid_ids = surface_area.loc[surface_area["area"] > min_area, "node_id"].values
    return table.loc[table["node_id"].isin(valid_ids)]


def profile_merging(
    left: pd.DataFrame, right: pd.DataFrame, on: str = "node_id", suffixes: tuple[str, str] = ("_left", "_right")
) -> pd.DataFrame:
    """Merging of profile-tables.

    As profile-tables inherently contain duplicate values in the 'node_id'-column, a dummy column is created on which
    the merge is executed in combination with the 'node_id'-column.

    :param left: left dataframe
    :param right: right dataframe
    :param on: column-name to merge on, defaults to 'node_id'
    :param suffixes: suffixes of duplicate column-names for the left and right dataframes,
        defaults to ('_left', '_right')

    :type left: pandas.DataFrame
    :type right: pandas.DataFrame
    :type on: str, optional
    :type suffixes: tuple[str, str], optional

    :return: merged profile table
    :rtype: pandas.DataFrame
    """
    assert on in left.columns and on in right.columns
    dfs = (
        left.assign(count=lambda df: df.groupby(on).cumcount()),
        right.assign(count=lambda df: df.groupby(on).cumcount()),
    )
    out = pd.merge(*dfs, how="outer", on=(on, "count"), suffixes=suffixes, validate=None).drop(columns="count")
    return out


def single_profile_nodes(
    flowing_table: pd.DataFrame, storing_table: pd.DataFrame, *, min_area: float = 1e-3
) -> tuple[list[int], pd.DataFrame, pd.DataFrame]:
    """Determine which basin node-IDs consist of only flowing profiles, or only storing profiles.

    Basins that have either flowing profiles, or storing profiles, well be schematised as being flowing basins only,
    i.e., no storing basin should be added. To exclude the addition of storing basins to these flowing-only basins,
    their node-IDs are returned to be used as input for `AddStorageBasins(additional_basins_to_exclude=[...])`.

    In addition, this results in changes to the flowing and storing profiles tables. All these modifications will be
    returned (in combination with the list of flowing-only basins).

    :param flowing_table: table of flowing profiles ('doorgaand')
    :param storing_table: table of storing profiles ('bergend')
    :param min_area: minimum area for profile to be considered valid [m2], defaults to 1e-3

    :type flowing_table: pandas.DataFrame
    :type storing_table: pandas.DataFrame
    :type min_area: float, optional

    :return: no-storing basin node-IDs ('doorgaand'-only) and modified profile tables
    :rtype: tuple[list[int], pandas.DataFrame, pandas.DataFrame]
    """
    # remove profiles with too small surface area
    df_flowing = minimum_surface_area(flowing_table, min_area)
    df_storing = minimum_surface_area(storing_table, min_area)

    # merge tables
    df = profile_merging(df_flowing, df_storing, suffixes=("_flowing", "_storing"))

    # flag storing basin
    assert sum(df[["area_flowing", "area_storing"]].isna().all(axis=1)) == 0
    df["storing_basin"] = ~df[["area_flowing", "area_storing"]].isna().any(axis=1)

    # modify flowing profiles table
    df["level"] = df["level_flowing"].combine_first(df["level_storing"])
    df["area"] = df["area_flowing"].combine_first(df["area_storing"])
    out_flowing = df[["node_id", "level", "area"]].copy(deep=True)

    # modify storing profiles table
    out_storing = df.loc[df["storing_basin"], ["node_id", "level_storing", "area_storing"]].reset_index(drop=True)
    out_storing.rename(columns={"level_storing": "level", "area_storing": "area"}, inplace=True)

    # add non-used 'storage'-column to tables
    out_flowing["storage"] = None
    out_storing["storage"] = None

    # list storing basin node-IDs
    storing_ids = list(map(int, df.loc[df["storing_basin"], "node_id"].unique()))

    # return processed profile data
    return storing_ids, out_flowing, out_storing


def set_basin_profiles(ribasim_model: Model, water_authority: str, **kwargs) -> Model:
    """Set basin profiles and add storing basins where applicable.

    Based on the profile generation, trapezoidal profiles are set to the flowing basins. In case there are storing basin
    profiles generated, a storing basin is added to the flowing basin via a ManningResistance-node.

    :param ribasim_model: Ribasim model
    :param water_authority: water authority
    :param kwargs: optional arguments

    :key cloud: the GoodCloud storage, defaults to CloudStorage()
    :key dx: horizontal distance between flowing and storing basins, defaults to 10 [m]
    :key dy: vertical distance between flowing and storing basins, defaults to 0 [m]
    :key min_area: minimum are in profile table to be considered valid (removed otherwise), defaults to 1e-3 [m2]
    """
    # optional arguments
    cloud: CloudStorage = kwargs.get("cloud", CloudStorage())
    dx: float = kwargs.get("dx", 10)
    dy: float = kwargs.get("dy", 0)
    min_area: float = kwargs.get("min_area", 1e-3)

    # get profile data
    tables = get_tables(water_authority, cloud=cloud)
    storing_ids, df_flowing, df_storing = single_profile_nodes(*tables, min_area=min_area)

    if (
        ribasim_model.node.df is None
        or ribasim_model.basin.profile.df is None
        or ribasim_model.basin.node.df is None  # pyrefly: ignore[missing-attribute]
        or ribasim_model.basin.static.df is None
        or ribasim_model.basin.state.df is None
        or ribasim_model.basin.area.df is None
        or ribasim_model.link.df is None
        or ribasim_model.manning_resistance.static.df is None
    ):
        msg = "Required model tables are missing."
        raise ValueError(msg)

    # modify existing basins ('doorgaand')
    ribasim_model.node.df = ribasim_model.node.df.assign(meta_node_id=ribasim_model.node.df.index)
    _basin_profile = ribasim_model.basin.profile.df.copy()
    _profiles = profile_merging(df_flowing, _basin_profile[["node_id"]], suffixes=("", "_"))
    ribasim_model.basin.profile.df = _profiles.sort_values(["node_id", "level"], ignore_index=True).combine_first(  # pyrefly: ignore[bad-assignment]
        _basin_profile.sort_values(["node_id", "level"], ignore_index=True)
    )[_basin_profile.columns]
    del _basin_profile

    # duplicate all basin-tables
    basin_node = ribasim_model.basin.node.df
    basin_static = ribasim_model.basin.static.df
    basin_state = ribasim_model.basin.state.df
    basin_area = ribasim_model.basin.area.df
    basin_profile = df_storing.copy(deep=True)

    # ID-selection: add storing basins
    basin_node = basin_node[basin_node.index.isin(storing_ids)].copy(deep=True)  # pyrefly: ignore[bad-index,missing-attribute]
    basin_static = basin_static[basin_static["node_id"].isin(storing_ids)].copy(deep=True)
    basin_state = basin_state[basin_state["node_id"].isin(storing_ids)].copy(deep=True)
    basin_area = basin_area[basin_area["node_id"].isin(storing_ids)].copy(deep=True)
    assert all(basin_profile["node_id"].isin(storing_ids)), "Storing profiles should only contain storing node IDs"

    # node ID incrementation
    incr_node_id = int(ribasim_model.next_node_id - 1)
    basin_node.index += incr_node_id
    basin_static["node_id"] += incr_node_id
    basin_state["node_id"] += incr_node_id
    basin_area["node_id"] += incr_node_id
    basin_profile["node_id"] += incr_node_id

    # basin state category ('bergend')
    basin_state["meta_categorie"] = "bergend"

    # move storing basins' location and set Manning's node location
    basin_node["flowing_geometry"] = basin_node["geometry"].copy()
    basin_node["manning_geometry"] = basin_node["geometry"].translate(xoff=0.5 * dx, yoff=0.5 * dy)
    basin_node["geometry"] = basin_node["geometry"].translate(xoff=dx, yoff=dy)
    incr_node_id = max(basin_node.index) - min(basin_node.index) + 1
    basin_node["manning_id"] = basin_node.index + incr_node_id

    # create links
    basin_node["link_sm_geometry"] = basin_node.apply(
        lambda row: shapely.LineString((row["geometry"], row["manning_geometry"])), axis=1
    )
    basin_node["link_mf_geometry"] = basin_node.apply(
        lambda row: shapely.LineString((row["manning_geometry"], row["flowing_geometry"])), axis=1
    )

    # create node table: ManningResistance
    manning_node = basin_node[["manning_id", "manning_geometry"]].reset_index(drop=True)
    manning_node = (
        manning_node.rename(columns={"manning_id": "node_id", "manning_geometry": "geometry"})
        .set_index("node_id")
        .assign(node_type="ManningResistance", meta_node_id=manning_node.index)
    )

    # create static table: ManningResistance
    manning_static = manning_node.reset_index(drop=False)[["node_id"]]
    manning_static = manning_static.assign(length=1000, manning_n=0.02, profile_width=2.0, profile_slope=3.0)
    # TODO: Assign `profile_width` based on representative widths of storing basins

    # create links tables
    sm_link = (
        basin_node.reset_index(drop=False)[["node_id", "manning_id", "link_sm_geometry"]]
        .rename(columns={"node_id": "from_node_id", "manning_id": "to_node_id", "link_sm_geometry": "geometry"})
        .assign(
            link_type="flow",
            meta_from_node_type="Basin",
            meta_to_node_type="ManningResistance",
            meta_categorie="bergend",
        )
    )
    mf_link = (
        basin_node.reset_index(drop=False)[["manning_id", "meta_node_id", "link_mf_geometry"]]
        .rename(columns={"manning_id": "from_node_id", "meta_node_id": "to_node_id", "link_mf_geometry": "geometry"})
        .assign(
            link_type="flow",
            meta_from_node_type="ManningResistance",
            meta_to_node_type="Basin",
            meta_categorie="bergend",
        )
    )
    link = pd.concat([sm_link, mf_link], axis=0, ignore_index=True)
    incr_link_id = ribasim_model.link.df.index.max() + 1
    link["link_id"] = link.index + incr_link_id
    link.set_index("link_id", inplace=True)

    # clean up basin node table ('bergend')
    basin_node = basin_node[["node_type", "meta_node_id", "geometry"]]
    basin_node["meta_node_id"] = basin_node.index

    # concatenate all newly generated tables to Ribasim model
    # > Node-table
    ribasim_model.node.df = pd.concat([ribasim_model.node.df, basin_node, manning_node], ignore_index=False)  # pyrefly: ignore[bad-assignment]
    # > Link-table
    ribasim_model.link.df = pd.concat([ribasim_model.link.df, link], ignore_index=False)  # pyrefly: ignore[bad-assignment]
    # > Basin-tables
    ribasim_model.basin.static.df = pd.concat([ribasim_model.basin.static.df, basin_static], ignore_index=True)  # pyrefly: ignore[bad-assignment]
    ribasim_model.basin.state.df = pd.concat([ribasim_model.basin.state.df, basin_state], ignore_index=True)  # pyrefly: ignore[bad-assignment]
    ribasim_model.basin.profile.df = pd.concat([ribasim_model.basin.profile.df, basin_profile], ignore_index=True)  # pyrefly: ignore[bad-assignment]
    ribasim_model.basin.area.df = pd.concat([ribasim_model.basin.area.df, basin_area], ignore_index=True)  # pyrefly: ignore[bad-assignment]
    # > ManningResistance-tables
    ribasim_model.manning_resistance.static.df = pd.concat(  # pyrefly: ignore[bad-assignment]
        [ribasim_model.manning_resistance.static.df, manning_static], ignore_index=True
    )

    # update model IDs
    ribasim_model.node._update_used_ids()  # pyrefly: ignore[not-callable]
    ribasim_model.link._update_used_ids()  # pyrefly: ignore[not-callable]

    # return the updated Ribasim model
    return ribasim_model
