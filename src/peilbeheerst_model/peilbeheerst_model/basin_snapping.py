"""Snapping the Basin-node to the hydro-objects within the basin.

The Basin-node must be located on the hydro-objects to facilitate the snapping of Link-objects to these hydro-objects.
"""

import pathlib
import typing

import geopandas as gpd
from ribasim_nl.case_conversions import snake_to_pascal_case
from ribasim_nl.link_geometries import fix_link_geometries
from shapely.ops import nearest_points, unary_union

from ribasim_nl import Model, Network


def node_to_hydro_object(
    model: Model, hydro_objects: gpd.GeoDataFrame, max_distance: float | None = None, main_route_only: bool = True
) -> Model:
    """Snap Basin-nodes to hydro-objects.

    :param model: Ribasim model
    :param hydro_objects: hydro-objects geo-data
    :param max_distance: maximum distance between original location and snapping location, defaults to None
    :param main_route_only: only include hydro-objects that are flagged as the main route, defaults to False
        This flag requires the hydro-objects to contain a column 'main-route' containing boolean values.

    :return: updated Ribasim model

    :raise AssertionError: if model Node-table is empty
    :raise AssertionError: if model Basin-area-table is empty
    """
    # model initiation check
    assert model.node.df is not None
    assert model.basin.area.df is not None

    # copy datasets
    nodes = typing.cast(gpd.GeoDataFrame, model.node.df.copy(deep=True))
    areas = typing.cast(gpd.GeoDataFrame, model.basin.area.df.copy(deep=True))

    # selection of basin-nodes and -areas
    nodes = nodes[(nodes["node_type"] == "Basin") & (nodes["meta_categorie"] != "bergend")]
    areas = areas[areas["node_id"].isin(nodes.index)]
    areas.set_crs(nodes.crs, inplace=True)  # pyrefly: ignore[no-matching-overload]

    # selection and grouping of hydro-objects (per basin)
    if main_route_only:
        hydro_objects = hydro_objects[hydro_objects["main-route"]]
    ho_basin = gpd.sjoin(hydro_objects, areas[["node_id", "geometry"]], how="inner", predicate="within").reset_index(
        drop=True
    )
    ho_basin = ho_basin[["node_id", "geometry"]]

    # clipping hydro-objects to basin-area
    ho_basin = ho_basin.merge(areas[["node_id", "geometry"]].rename(columns={"geometry": "basin"}), on="node_id")
    ho_basin["geometry"] = ho_basin.apply(lambda r: r["geometry"].intersection(r["basin"]), axis=1)
    ho_basin = ho_basin[~ho_basin["geometry"].is_empty]

    # merge hydro-objects to MultiLineString (per basin)
    ho_merged = ho_basin.groupby("node_id")["geometry"].apply(unary_union).rename("hydro")

    # couple grouped hydro-objects to basin-nodes
    out = typing.cast(gpd.GeoDataFrame, nodes.merge(ho_merged, how="left", left_index=True, right_index=True))
    out.set_crs(nodes.crs, inplace=True)  # pyrefly: ignore[no-matching-overload]

    # basin-node snapping
    valid = out["hydro"].notna()
    original_nodes = out.loc[valid, "geometry"]
    snapped_nodes = original_nodes.combine(out.loc[valid, "hydro"], lambda point, line: nearest_points(point, line)[1])
    distances = original_nodes.distance(snapped_nodes.set_crs(nodes.crs))

    # maximum relocation distance
    if max_distance is not None:
        too_far = distances > max_distance
        snapped_nodes = snapped_nodes.where(~too_far, original_nodes)
        distances = distances.where(~too_far, 0.0)

    # relocate basin nodes
    out.loc[valid, "geometry"] = snapped_nodes
    out.loc[valid, "snap_distance"] = distances

    # update Ribasim model
    tmp = model.node.df.copy()
    tmp.loc[out.index, "geometry"] = out["geometry"]
    model.node.df = tmp.copy()  # pyrefly: ignore[bad-assignment]

    # return updated Ribasim model
    return model


def node_to_hydro_object_from_file(
    model: Model,
    fn_hydro_objects: str | pathlib.Path,
    layer: str | None = None,
    max_distance: float | None = None,
    main_route_only: bool = False,
) -> Model:
    """Snap Basin-nodes to hydro-objects wrapper: Hydro-objects are read from file.

    This function calls the `node_to_hydro_object`-function after reading the hydro-objects geo-data from the provided
    filename (and layer).

    :param model: Ribasim model
    :param fn_hydro_objects: filename with hydro-objects geo-data
    :param layer: layer with hydro-objects geo-data, defaults to None
    :param max_distance: maximum distance between original location and snapping location, defaults to None
    :param main_route_only: only include hydro-objects that are flagged as the main route, defaults to False
        This flag requires the hydro-objects to contain a column 'main-route' containing boolean values.

    :return: updated Ribasim model
    """
    gdf = gpd.read_file(fn_hydro_objects, layer=layer)
    return node_to_hydro_object(model, gdf, max_distance=max_distance, main_route_only=main_route_only)


def link_to_hydro_object(
    model: Model, hydro_objects: Network, node_types: tuple[str, ...] = (), relocate_storing_basins: bool = False
) -> Model:
    """Place the Link-geometries (Flow) to hydro-objects.

    When no `node_types` are provided, all node-types are considered. Note that storing basins are excluded from
    snapping to the hydro-objects. These can be relocated to be placed next to the flowing basin by enabling the
    `relocate_storing_basins`-argument.

    :param model: Ribasim model
    :param hydro_objects: network of hydro-objects
    :param node_types: node-types between which the Link-geometries should be snapped to the hydro-objects,
        defaults to ()
    :param relocate_storing_basins: relocate storing basins to be placed next to "their" flowing basin (again),
        defaults to False

    :return: Ribasim model

    :raise AssertionError: if model Node-table is empty
    :raise NotImplementedError: if `relocate_storing_basins` is enabled
    """
    # model initiation check
    assert model.node.df is not None

    # selection of nodes
    nodes = model.node.df[model.node.df["meta_categorie"] != "bergend"]
    if node_types:
        nodes = nodes[nodes["node_type"].isin([snake_to_pascal_case(nt) for nt in node_types])]

    # snap Link-geometries to hydro-objects
    fix_link_geometries(model, hydro_objects, node_ids=nodes.index.tolist())

    # relocate storing basins
    if relocate_storing_basins:
        msg = (
            f"Relocating the storing basins next to 'their' flowing basin not (yet) implemented: "
            f"Set {relocate_storing_basins=} to False"
        )
        raise NotImplementedError(msg)

    # return Ribasim model
    return model
