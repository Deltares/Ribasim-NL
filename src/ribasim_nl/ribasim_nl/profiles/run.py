"""Definition of profiles per water authority."""

import logging
import pathlib
import typing
import warnings

import geopandas as gpd
import momepy
import pandas as pd
import shapely

from ribasim_nl import CloudStorage
from ribasim_nl.profiles import bgt, cross_section, depth, path_finder, width
from ribasim_nl.profiles import hydrotopes as ht

LOG = logging.getLogger(__name__)


def target_level_polygons(
    fn: pathlib.Path, *, layer_polygons: str = "peilgebied", layer_levels: str = "streefpeil"
) -> gpd.GeoDataFrame:
    """Get polygons with target levels from source-data.

    :param fn: filename
    :param layer_polygons: layer-name with polygon data
    :param layer_levels: layer-name with target level data

    :type fn: str
    :type layer_polygons: str, optional
    :type layer_levels: str, optional

    :return: polygons with target levels
    :rtype: geopandas.GeoDataFrame
    """
    polygons = gpd.read_file(fn, layer=layer_polygons)
    levels = gpd.read_file(fn, layer=layer_levels)
    out = polygons.assign(meta_streefpeil=polygons["globalid"].map(levels.set_index("globalid")["waterhoogte"]))
    return out


@typing.overload
def main(
    basins: gpd.GeoDataFrame,
    hydro_objects: gpd.GeoDataFrame,
    crossings: gpd.GeoDataFrame,
    cross_sections: gpd.GeoDataFrame = ...,
    /,
    *,
    hydrotope_table: ht.HydrotopeTable | None = None,
    col_ho_main_route: None = None,
    **kwargs,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]: ...


@typing.overload
def main(
    basins: gpd.GeoDataFrame,
    hydro_objects: gpd.GeoDataFrame,
    cross_sections: gpd.GeoDataFrame = ...,
    /,
    *,
    hydrotope_table: ht.HydrotopeTable | None = None,
    col_ho_main_route: str,
    **kwargs,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]: ...


_KNOWN_KWARGS: set[str] = {
    "debug",
    "epsg",
    "filter_basins",
    "fn_hydrotopes",  # TODO: Remove
    "target_levels",
    "create_depth_profile_lines",
    "kw_make_depth_profile",
    "simplify_geometries",
    "fn_bgt",
    "bgt_buffer",
    "bgt_full_coverage",
    "patch_network",
    "patch_buffer",
    "split_buffer",
    "val_ho_main_route",
    "selection_buffer",
    "water_bodies",
    "col_wb_depth",
    "create_wd_intermediate_output",
}


def main(
    *data: gpd.GeoDataFrame,
    hydrotope_table: ht.HydrotopeTable | None = None,
    cloud: CloudStorage = CloudStorage(),
    col_ho_main_route: str | None = None,
    export_intermediate_output: bool = False,  # TODO: Integrate with defining `wd_intermediate_output`
    wd_intermediate_output: pathlib.Path | None = None,
    **kwargs,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Full profile-generating workflow.

    The amount of required geospatial datasets (i.e., number of entries for `data`) is somewhat flexible. There are two
    considerations that influence this:
        1.  Is the main route ('doorgaand') flagged in the hydro-objects? If so, `col_ho_main_route` should be provided
            and the `crossings`-data is not used, and thus omitted. Thereby, the required number of geospatial datasets
            reduces with one (2 or 3 datasets required).
        2.  Are there measured cross-sectional profiles to be incorporated? If so, 3 or 4 geospatial datasets should be
            provided: Three (3) datasets if `col_ho_main_route` is defined (i.e., hydro-objects contain main-route
            flagging); or four (4) datasets if the main-routing follows from the shortest path implementation. If this
            "additional dataset" is not provided, the cross-sections are fully based on the hydrotope-table (which
            raises a warning, but does not break the code).

    The order of the geospatial datasets is as follows:
        1.  basins (polygons)
        2.  hydro-objects (lines)
        3.  crossings (points) [optional]
        4.  cross-sections (points | lines) [optional]

    :param data: geospatial datasets
    :param hydrotope_table: table with hydrotope-classes, defaults to None  # TODO: Remove `fn_hydrotopes`
        When no `HydrotopeTable` is provided, a *.csv-file containing such a table must be provided via the keyworded
        argument `fn_hydrotopes`. If both are `None`, a `ValueError` is raised.
    :param cloud: cloud-storage object, used to load the hydrotopes-map, defaults to CloudStorage()
    :param kwargs: optional arguments

    :key debug: flag for debug-mode, defaults to False
    :key epsg: EPSG to which all geospatial data is projected, defaults to 28992
    :key filter_basins: filter basins on 'doorgaand' (i.e., excl. 'bergend'), defaults to True
    :key fn_hydrotopes: *.csv-file containing hydrotope-specifications, defaults to None  # TODO: Remove
        Required if no `HydrotopeTable` is provided (i.e., `hydrotope_table=None`)
    :key create_depth_profile_lines: create depth profile lines of the cross-sections from point-data, defaults to False
    :key kw_make_depth_profile: optional arguments for making depth profile lines, defaults to {}
    :key simplify_geometries: simplify geometries by removing duplicates, defaults to True
    :key fn_bgt: geospatial file with previously downloaded BGT-data, or where to export the newly downloaded BGT-data
        to for future use, defaults to None
    :key bgt_buffer: minimum overlap between BGT-polygon and hydro-object to be coupled, defaults to 0.1 [m]
    :key bgt_full_coverage: consider BGT-data not intersecting any hydro-objects as part of the storing capacity of the
        basin, defaults to True
    :key patch_network: patch the network, defaults to True
        Network patching is computationally expensive, but assures a fully connected network from the hydro-objects
        required for generating a proper graph. This option can be disabled when the hydro-objects are already fully
        connected.
    :key patch_buffer: distance below which endpoints of hydro-objects are considered "connected", defaults to 1 [m]
    :key split_buffer: distance between crossing and hydro-object at which the hydro-object should be split (if not
        already) due to a crossing, defaults to 0.1 [m]
    :key col_ho_main_route: column-name in `hydro_objects` containing flag(s) for main-routing, defaults to None
        When `col_ho_main_route` is defined, the main-routing is based on this column instead of drawing a graph per
        basin and connecting the crossings within the basin using a shortest path algorithm. This means that the
        `crossings`-dataset is no longer required to generate the profiles. This impacts the required geospatial
        datasets provided to `data` (see documentation).
    :key val_ho_main_route: value(s) in `hydro_objects[col_ho_main_route]` flagging main-routing, defaults to True
    :key internal_crossings: include crossings inside the basin (`True`) or limit to crossings at the basin-border
        (`False`), defaults to True
    :key selection_buffer: buffer used when selecting the subset of crossings for a single basin, defaults to 0.1 [m]
        This buffer is applied on the basin-polygon for `internal_crossings=True`, and to the basin-polygon's exterior
        for `internal_crossings=False`.
    :key water_bodies: water bodies with specific, user-defined representative depths used to overwrite the determined
        representative depths per hydro-object, defaults to None
        When `water_bodies` (polygons), hydro-objects within the polygon(s) have their representative depth overwritten
        by the depth value(s) in `water_bodies`.
    :key col_wb_depth: column-name with representative depth values of `water_bodies`, defaults to "depth"
    :key export_intermediate_output: export intermediate output, e.g., for debugging, defaults to False
        If `export_intermediate_output=True`, a working directory must be defined, i.e., `wd_intermediate_output`
    :key wd_intermediate_output: working directory for intermediate output files, defaults to None
    :key create_wd_intermediate: create working directory for intermediate output files (if non-existing), including the
        parents, defaults to True

    :return: basin profiles for flowing/'doorgaand' and storing/'bergend' separately

    :raises TypeError: if unknown keyword arguments are given (possible typos)
    :raises ValueError: if both `hydrotope_table` and `fn_hydrotopes` are undefined (i.e., `None`)  # TODO: Remove
    :raises ValueError: if `wd_intermediate_output` is not defined while `export_intermediate_output=True`  # TODO: Remove
    :raises ValueError: if less than two (2) or more than four (4) geospatial dataframes are provided as `data`
    :raises ValueError: if NaN-values are found in one of the profile tables and debug-mode is not enabled
    """
    # check for typos in `kwargs`
    unknown = kwargs.keys() - _KNOWN_KWARGS
    if unknown:
        msg = f"Unexpected keyword argument(s): {unknown}"
        raise TypeError(msg)

    # optional arguments
    debug: bool = kwargs.get("debug", False)
    epsg: int = kwargs.get("epsg", 28992)
    filter_basins: bool = kwargs.get("filter_basins", True)
    # > hydrotopes  TODO: Remove
    fn_hydrotopes: pathlib.Path | str | None = kwargs.get("fn_hydrotopes")
    # > target levels
    target_levels: gpd.GeoDataFrame = kwargs.get("target_levels", data[0])
    # > generation of depth profile lines
    create_depth_profile_lines: bool = kwargs.get("create_depth_profile_lines", False)
    kw_make_depth_profile: dict[str, str] = kwargs.get("kw_make_depth_profile", {})
    # > simplification of geometries: removing duplicates
    simplify_geometries: bool = kwargs.get("simplify_geometries", True)
    # > BGT-data
    fn_bgt: pathlib.Path | str | None = kwargs.get("fn_bgt")
    bgt_buffer: float = kwargs.get("bgt_buffer", 0.1)
    bgt_full_coverage: bool = kwargs.get("bgt_full_coverage", True)
    # > network patching
    patch_network: bool = kwargs.get("patch_network", True)
    patch_buffer: float = kwargs.get("patch_buffer", 1)
    split_buffer: float = kwargs.get("split_buffer", 0.1)
    # > main-routing from hydro-objects alone
    # col_ho_main_route: str | None = kwargs.get("col_ho_main_route")
    val_ho_main_route: typing.Any = kwargs.get("val_ho_main_route", True)
    # > selection of crossings
    internal_crossings: bool = kwargs.get("internal_crossings", True)
    selection_buffer: float = kwargs.get("selection_buffer", 1)
    # > overwriting of representative depth
    water_bodies: gpd.GeoDataFrame | None = kwargs.get("water_bodies")
    col_wb_depth: str = kwargs.get("col_wb_depth", "depth")
    # > export intermediate output
    # export_intermediate_output: bool = kwargs.get("export_intermediate_output", False)
    # wd_intermediate_output: pathlib.Path | None = kwargs.get("wd_intermediate_output")
    create_wd_intermediate: bool = kwargs.get("create_wd_intermediate", True)
    _fn_int_output: str = "int_output.gpkg"

    # transition: abandon `fn_hydrotopes` as optional argument
    # TODO: Implement deprecation of `fn_hydrotopes`
    if fn_hydrotopes is not None:
        warnings.warn(
            "The semi-optional `fn_hydrotopes`-argument is deprecated and will be removed in the future. Load the "
            "hydrotopes-table explicitly and pass it to `hydrotope_table` instead:\n\n"
            "    from ribasim_nl.profiles import hydrotopes as ht\n"
            "    table = ht.HydrotopeTable.from_csv('path/to/file.csv')\n"
            "    main(..., hydrotope_table=table)\n",
            DeprecationWarning,
            stacklevel=2,
        )

    # validate optional arguments
    # > hydrotope data
    if hydrotope_table is None and fn_hydrotopes is None:
        msg = (
            f"Either a table with hydrotopes must be given ({hydrotope_table=}), "
            f"or a *.csv-file with hydrotopes ({fn_hydrotopes=})"
        )
        raise ValueError(msg)
    if hydrotope_table is None:
        assert fn_hydrotopes is not None
        hydrotope_table = ht.HydrotopeTable.from_csv(fn_hydrotopes)
    elif fn_hydrotopes is not None:
        LOG.warning(f"Hydrotope-table specified; skipped {fn_hydrotopes=}")
    # > intermediate output
    if export_intermediate_output and wd_intermediate_output is None:
        msg = (
            f"When exporting the intermediate output ({export_intermediate_output=}), "
            f"a working directory must be provided: {wd_intermediate_output=}"
        )
        raise ValueError(msg)
    if wd_intermediate_output is not None and create_wd_intermediate:
        wd_intermediate_output.mkdir(parents=True, exist_ok=True)
    # > main routes from hydro-objects
    main_route_from_hydro_objects: bool = col_ho_main_route is not None

    # align all CRS
    for gdf in data:
        gdf.to_crs(epsg=epsg, inplace=True)

    # split dataframes
    basins, hydro_objects, crossings, cross_sections = _unpack_data(
        *data, flagged_main_route=main_route_from_hydro_objects
    )

    # main route data available in hydro-objects [optional]
    if main_route_from_hydro_objects:
        assert col_ho_main_route is not None
        _validate_hydro_objects_main_routing(hydro_objects, col_ho_main_route, val_ho_main_route)

    # filter basins
    if filter_basins:
        basins = basins[basins["node_id"] == basins["meta_node_id"]]

    # creating depth profile-lines
    if create_depth_profile_lines and cross_sections is not None:
        cross_sections = depth.make_depth_profiles(cross_sections, **kw_make_depth_profile)

    # remove duplicate geometries
    if simplify_geometries:
        hydro_objects = path_finder.simplify_geodata(hydro_objects)
        if crossings is not None:
            crossings = path_finder.simplify_geodata(crossings, tolerance=1e-2, col_in_use="in_use")

    # get BGT-data
    bgt_data = _get_bgt_data(fn_bgt, basins)

    # patch network
    if main_route_from_hydro_objects:
        LOG.warning(f"With {main_route_from_hydro_objects=}, no network patching executed ({patch_network=})")
    elif patch_network:
        hydro_objects = path_finder.fully_connected_network(hydro_objects, buffer=patch_buffer)
        if crossings is not None:
            hydro_objects = path_finder.split_hydro_objects(hydro_objects, crossings, buffer=split_buffer)
    else:
        hydro_objects = (
            gpd.GeoDataFrame(geometry=[hydro_objects.union_all()], crs=hydro_objects.crs)
            .drop_duplicates(subset="geometry")
            .explode()
            .reset_index(drop=True)
        )

    if main_route_from_hydro_objects:
        # get all user-defined hydro-objects on the main-route (i.e., 'doorgaand')
        assert col_ho_main_route is not None
        assert val_ho_main_route is not None
        main_route_idx = _label_main_routing_from_flags(hydro_objects, col_ho_main_route, val_ho_main_route)
    else:
        assert crossings is not None
        main_route_idx = _label_main_routing_from_network(
            basins, hydro_objects, crossings, selection_buffer, internal_crossings, wd_intermediate_output
        )

    # label hydro-objects
    hydro_objects["main-route"] = hydro_objects.index.isin(main_route_idx)
    if wd_intermediate_output is not None:
        hydro_objects[hydro_objects["main-route"]].to_file(wd_intermediate_output / _fn_int_output, layer="main-route")
        if not main_route_from_hydro_objects:
            assert crossings is not None
            if internal_crossings:
                _temp = shapely.MultiPolygon(basins.explode().geometry.values).buffer(selection_buffer)
            else:
                _temp = shapely.MultiPolygon(basins.explode().exterior.buffer(selection_buffer))
            crossings[crossings.intersects(_temp)].to_file(wd_intermediate_output / _fn_int_output, layer="endpoints")
            del _temp

    # BGT-coupling
    hydro_objects = width.couple_bgt_to_hydro_objects(hydro_objects, bgt_data, min_overlap=bgt_buffer)
    hydro_objects = width.estimate_width(hydro_objects, bgt_data, drop_na=True)
    if wd_intermediate_output is not None:
        bgt.save_bgt_coupling(hydro_objects, bgt_data, wd_intermediate_output)

    # depth from hydrotopes
    hydrotope_map = ht.get_hydrotopes_map(cloud=cloud)
    hydro_objects = depth.depth_from_hydrotopes(hydro_objects, hydrotope_map, hydrotope_table, drop_na=True)

    # depth from measurements
    if cross_sections is not None:
        cross_sections = depth.normalise_measured_cross_sections(cross_sections, target_levels)
        hydro_objects = depth.depth_from_measurements(hydro_objects, cross_sections)

    # depth from (multi)polygons (user-defined)
    if water_bodies is not None:
        hydro_objects = _overwrite_depth(hydro_objects, water_bodies, col_wb_depth)

    # export depth-data [optional]
    if wd_intermediate_output is not None:
        hydro_objects.to_file(wd_intermediate_output / _fn_int_output, layer="hydro-objects")

    # basin profiles
    main_route = hydro_objects["main-route"].values
    flowing_profiles = cross_section.assign_basin_profiles(basins, hydro_objects[main_route], as_geo_dataframe=True)
    storing_profiles = cross_section.assign_basin_profiles(basins, hydro_objects[~main_route], as_geo_dataframe=True)

    # NaN-valued basin profiles
    if sum(flowing_profiles["area"].isna()) > 0:
        if debug:
            LOG.warning(
                f"NaN-values present in profile-table (flowing/'doorgaand'):"
                f"\n{flowing_profiles[flowing_profiles.isna()]}"
            )
        else:
            msg = f"Abort profile table generation with NaN-values: {sum(flowing_profiles.isna())} NaN-values found"
            raise ValueError(msg)
    if sum(storing_profiles["area"].isna()) > 0:
        if debug:
            LOG.warning(
                f"NaN-values present in profile-table (storing/'bergend')\n{storing_profiles[storing_profiles.isna()]}"
            )
        else:
            msg = f"Abort profile table generation with NaN-values: {sum(storing_profiles.isna())} NaN-values found"
            raise ValueError(msg)

    # fill storing basins with BGT-data
    if bgt_full_coverage:
        storing_profiles = cross_section.full_bgt_coverage(
            flowing_profiles, storing_profiles, basins, bgt_data, as_geo_dataframe=True, min_valid_area=2e-3
        )

    # assure no zero area-values
    assert all(flowing_profiles["area"] > 0), (
        f"Not all profile areas are larger than zero: {flowing_profiles[~flowing_profiles['area'] > 0]}"
    )
    assert all(storing_profiles["area"] > 0), (
        f"Not all profile areas are larger than zero: {storing_profiles[~storing_profiles['area'] > 0]}"
    )

    # export basin profiles
    if wd_intermediate_output is not None:
        flowing_profiles.to_file(wd_intermediate_output / _fn_int_output, layer="basin_profiles_doorgaand")
        storing_profiles.to_file(wd_intermediate_output / _fn_int_output, layer="basin_profiles_bergend")
    return flowing_profiles, storing_profiles


def _unpack_data(
    *data: gpd.GeoDataFrame, flagged_main_route: bool
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame | None, gpd.GeoDataFrame | None]:
    """Unpack geospatial datasets.

    The unpacking order depends on whether the main route is flagged in the hydro-objects or determined via crossings:

    if flagged_main_route:
        basins, hydro_objects[, cross_sections] = data
    else:
        basins, hydro_objects, crossings[, cross_sections] = data

    Expected datasets (with geometry type):
        1.  basins (polygons)
        2.  hydro-objects (lines)
        3.  crossings (points) - omitted when `flagged_main_route=True`
        4.  cross-sections (points | lines) - optional

    When `cross_sections` is omitted, representative depth values are derived entirely from hydrotopes, which may impact
    the Ribasim simulation.

    :param data: geospatial datasets, provided in the order above
    :param flagged_main_route: whether hydro-objects contains a main-route flag

    :return: (basins, hydro_objects, crossings, cross_sections)
        `crossings` and/or `cross_sections` may be `None`

    :raises TypeError: if the number of geospatial datasets is inconsistent with `flagged_main_route`
    :raises ValueError: if the number of geospatial datasets is not 2, 3, or 4
    """
    # unpack datasets
    match len(data):
        case 2:
            if not flagged_main_route:
                msg = (
                    "2 GeoDataFrames (basins, hydro_objects) require `col_ho_main_route` set to a column-name of "
                    "`hydro_objects`, got None"
                )
                raise TypeError(msg)
            basins, hydro_objects = data
            crossings = cross_sections = None
        case 3:
            if flagged_main_route:
                basins, hydro_objects, cross_sections = data
                crossings = None
            else:
                basins, hydro_objects, crossings = data
                cross_sections = None
        case 4:
            if flagged_main_route:
                msg = (
                    "4 GeoDataFrames (basins, hydro_objects, crossings, cross_sections) are incompatible with "
                    "`col_ho_main_route`: set `col_ho_main_route=None` or pass three (3) geospatial arguments"
                )
                raise TypeError(msg)
            basins, hydro_objects, crossings, cross_sections = data
        case _:
            msg = f"Expected 2, 3, or 4 GeoDataFrames, got {len(data)}"
            raise ValueError(msg)

    # warn missing cross-sections (measured)
    if cross_sections is None:
        LOG.warning("No cross-section data provided: Cross-section profiles fully based on hydrotopes")

    # return unpacked datasets
    return basins, hydro_objects, crossings, cross_sections


def _validate_hydro_objects_main_routing(hydro_objects: gpd.GeoDataFrame, col: str, val: typing.Any) -> None:
    if col not in hydro_objects.columns:
        msg = f"{col=} not found in {hydro_objects.columns=}"
        raise IndexError(msg)
    if pd.api.types.is_scalar(val):
        if val not in hydro_objects[col].unique():
            msg = f"{val=} not found in hydro_objects[col_ho_main_route] ({col=})"
            raise ValueError(msg)
    else:
        if not any(v in hydro_objects[col].unique() for v in val):
            msg = f"None of {val=} found in hydro_objects[col_ho_main_route] ({col=})"
            raise ValueError(msg)


def _get_bgt_data(fn_bgt: pathlib.Path | str | None, basins: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    geo_filter = shapely.MultiPolygon(basins.convex_hull.values).convex_hull
    if fn_bgt is None:
        return bgt.download_bgt_water(geo_filter=geo_filter)
    fn_bgt = pathlib.Path(fn_bgt)
    return bgt.get_water_surfaces(fn_bgt.parent, fn=fn_bgt.name, geo_filter=geo_filter, write=True)


def _label_main_routing_from_flags(hydro_objects: gpd.GeoDataFrame, col: str, val: typing.Any) -> set[int]:
    """Label main-routing based on hydro-objects flagging.

    :param hydro_objects: hydro-objects without main-routing labels
    :param col: column-name in `hydro_objects` containing flag(s) for main-routing
    :param val: value(s) in `hydro_objects[col_ho_main_route]` flagging main-routing

    :type hydro_objects: geopandas.GeoDataFrame
    :type col: str
    :type val: any

    :return: hydro-objects indices to label as main route
    :rtype: set[int]
    """
    if pd.api.types.is_scalar(val):
        indices = hydro_objects[hydro_objects[col] == val].index
    else:
        indices = hydro_objects[hydro_objects[col].isin(val)].index
    return set(indices)


def _label_main_routing_from_network(
    basins: gpd.GeoDataFrame,
    hydro_objects: gpd.GeoDataFrame,
    crossings: gpd.GeoDataFrame,
    buffer: float,
    internal: bool,
    wd: pathlib.Path | None,
) -> set[int]:
    """Labelling of main-route based on shortest path(s) between crossings.

    :param basins: basin areas (polygons)
    :param hydro_objects: hydro-objects (lines)
    :param crossings: basin-crossings (points)
    :param buffer: buffer used when selecting the subset of crossings for a single basin
        This buffer is applied on the basin-polygon for `internal_crossings=True`, and to the basin-polygon's exterior
        for `internal_crossings=False`.
    :param internal: include crossings inside the basin (`True`) or limit to crossings at the basin-border (`False`)
    :param wd: working directory for intermediate output files

    :type basins: geopandas.GeoDataFrame
    :type hydro_objects: geopandas.GeoDataFrame
    :type crossings: geopandas.GeoDataFrame
    :type buffer: float
    :type internal: bool
    :type wd: pathlib.Path | None

    :return: hydro-objects indices to label as main route
    :rtype: set[int]
    """
    # collectors
    main_route_idx: set[int] = set()
    point_collector: list[gpd.GeoDataFrame] = []
    line_collector: list[gpd.GeoDataFrame] = []
    error_collector: list[int] = []

    # find main routes per basin
    for node_id, basin in zip(basins["node_id"].values, basins.geometry.values):
        # data selections
        subset_hydro_objects = hydro_objects[hydro_objects.intersects(basin)]
        subset_crossings = path_finder.select_crossings(basin, crossings, buffer=buffer, internal=internal)

        # create network-graph
        if len(subset_hydro_objects) == 0:
            error_collector.append(int(node_id))
            LOG.warning(f"No hydro-objects found for Basin #{node_id}")
            continue
        graph = path_finder.generate_graph(subset_hydro_objects)

        # basin flatness
        use_full_graph = path_finder.full_graph_search(basin, graph, subset_crossings)

        # find flow routes
        flow_edges = path_finder.find_flow_routes(graph, subset_crossings, use_full_graph=use_full_graph)
        indices = path_finder.label_flow_hydro_objects(subset_hydro_objects, graph, flow_edges)
        main_route_idx.update(indices)

        # update collectors
        if wd is not None:
            points, lines = typing.cast(tuple[gpd.GeoDataFrame, gpd.GeoDataFrame], momepy.nx_to_gdf(graph))
            point_collector.append(points)
            line_collector.append(lines)

    # overview of erroneous basins
    if error_collector:
        LOG.critical(f"No hydro-objects found for the following basins ({len(error_collector)}): {error_collector}")

    # concatenate basin-groups of point- and line-data
    if wd is not None:
        points = typing.cast(gpd.GeoDataFrame, pd.concat(point_collector, axis=0))
        lines = typing.cast(gpd.GeoDataFrame, pd.concat(line_collector, axis=0))
        points.to_file(wd / "graph.gpkg", layer="points")
        lines.to_file(wd / "graph.gpkg", layer="lines")

    return main_route_idx


def _overwrite_depth(
    hydro_objects: gpd.GeoDataFrame, water_bodies: gpd.GeoDataFrame, col_depth: str
) -> gpd.GeoDataFrame:
    """Overwrite the representative depth of (large) water bodies based on (Multi)Polygon(s).

    :param hydro_objects: hydro-objects containing representative depths
    :param water_bodies: (Multi)Polygon-data with depth-values to overwrite
    :param col_depth: column-name of `water_bodies` containing depth-values

    :type hydro_objects: geopandas.GeoDataFrame
    :type water_bodies: geopandas.GeoDataFrame
    :type col_depth: str

    :return: hydro-objects with (partially) overwritten representative depths
    :rtype: geopandas.GeoDataFrame

    :raises IndexError: if the hydro-objects have no representative depth assigned (yet)
    :raises IndexError: if `col_depth` not in `water_bodies.columns`
    """
    # validate GeoDataFrames
    if "depth" not in hydro_objects.columns:
        msg = f"The representative depth cannot be overwritten as no depths are defined yet: {hydro_objects.columns=}"
        raise IndexError(msg)
    if col_depth not in water_bodies.columns:
        msg = f"Depth-column not found in the water bodies: {water_bodies.columns=}"
        raise IndexError(msg)

    # overwrite representative depths
    for g, v in water_bodies[["geometry", col_depth]].values:
        hydro_objects.loc[hydro_objects.intersects(g), "depth"] = v
    return hydro_objects
