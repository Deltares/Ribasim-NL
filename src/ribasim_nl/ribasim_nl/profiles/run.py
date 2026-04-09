"""Definition of profiles per water authority."""

import logging
import pathlib

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


def main(
    *data: gpd.GeoDataFrame, hydrotope_table: ht.HydrotopeTable | None = None, **kwargs
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Full profile-generating workflow.

    :param data: geospatial datasets:
        1.  basins (polygons)
        2.  crossings (points)
        3.  hydro-objects (lines)
        4.  cross-sections (points | lines) [optional]
    :param hydrotope_table: table with hydrotope-classes, defaults to None
        When no `HydrotopeTable` is provided, a *.csv-file containing such a table must be provided via the keyworded
        argument `fn_hydrotopes`. If both are `None`, a `ValueError` is raised.
    :param kwargs: optional arguments

    :key cloud: cloud-storage object, used to load the hydrotopes-map, defaults to CloudStorage()
    :key debug: flag for debug-mode, defaults to False
    :key epsg: EPSG to which all geospatial data is projected, defaults to 28992
    :key filter_basins: filter basins on 'doorgaand' (i.e., excl. 'bergend'), defaults to True
    :key fn_hydrotopes: *.csv-file containing hydrotope-specifications, defaults to None
        Required if no `HydrotopeTable` is provided (i.e., `hydrotope_table=None`)
    :key create_depth_profile_lines: create depth profile lines of the cross-sections from point-data, defaults to False
    :key kw_make_depth_profile: optional arguments for making depth profile lines, defaults to {}
    :key simplify_geometries: simplify geometries by removing duplicates, defaults to True
    :key fn_bgt: geospatial file with previously downloaded BGT-data, or where to export the newly downloaded BGT-data
        to for future use, defaults to None
    :key bgt_buffer: minimum overlap between BGT-polygon and hydro-object to be coupled, defaults to 0.1 [m]
    :key patch_network: patch the network, defaults to True
        Network patching is computationally expensive, but assures a fully connected network from the hydro-objects
        required for generating a proper graph. This option can be disabled when the hydro-objects are already fully
        connected.
    :key patch_buffer: distance below which endpoints of hydro-objects are considered "connected", defaults to 1 [m]
    :key split_buffer: distance between crossing and hydro-object at which the hydro-object should be split (if not
        already) due to a crossing, defaults to 0.1 [m]
    :key internal_crossings: include crossings inside the basin (`True`) or limit to crossings at the basin-border
        (`False`), defaults to True
    :key selection_buffer: buffer used when selecting the subset of crossings for a single basin, defaults to 0.1 [m]
        This buffer is applied on the basin-polygon for `internal_crossings=True`, and to the basin-polygon's exterior
        for `internal_crossings=False`.
    :key export_intermediate_output: export intermediate output, e.g., for debugging, defaults to False
        If `export_intermediate_output=True`, a working directory must be defined, i.e., `wd_intermediate_output`
    :key wd_intermediate_output: working directory for intermediate output files, defaults to None
    :key create_wd_intermediate: create working directory for intermediate output files (if non-existing), including the
        parents, defaults to True

    :type data: geopandas.GeoDataFrame
    :type hydrotope_table: hydrotopes.HydrotopeTable

    :return: basin profiles for flowing/'doorgaand' and storing/'bergend' separately
    :rtype: tuple[geopandas.GeoDataFrame, geopandas.GeoDataFrame]

    :raises ValueError: if both `hydrotope_table` and `fn_hydrotopes` are undefined (i.e., `None`)
    :raises ValueError: if `wd_intermediate_output` is not defined while `export_intermediate_output=True`
    :raises ValueError: if less than three (3) or more than four (4) geospatial dataframes are provided as `data`
    :raises ValueError: if NaN-values are found in one of the profile tables and debug-mode is not enabled
    """
    # optional arguments
    cloud: CloudStorage = kwargs.get("cloud", CloudStorage())
    debug: bool = kwargs.get("debug", False)
    epsg: int = kwargs.get("epsg", 28992)
    filter_basins: bool = kwargs.get("filter_basins", True)
    # > hydrotopes
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
    # > selection of crossings
    internal_crossings: bool = kwargs.get("internal_crossings", True)
    selection_buffer: float = kwargs.get("selection_buffer", 1)
    # > export intermediate output
    export_intermediate_output: bool = kwargs.get("export_intermediate_output", False)
    wd_intermediate_output: pathlib.Path | None = kwargs.get("wd_intermediate_output")
    create_wd_intermediate: bool = kwargs.get("create_wd_intermediate", True)
    _fn_graph = "graph.gpkg"
    _fn_int_output = "int_output.gpkg"

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

    # align all CRS
    for gdf in data:
        gdf.to_crs(epsg=epsg, inplace=True)

    # split dataframes
    match len(data):
        case 3:
            basins, crossings, hydro_objects = data
            cross_sections = None
            LOG.warning("No cross-section data provided: Cross-section profiles fully based on hydrotopes")
        case 4:
            basins, crossings, hydro_objects, cross_sections = data
        case _:
            msg = f"There should be 3 or 4 GeoDataFrames provided; {len(data)} given."
            raise ValueError(msg)

    # filter basins
    if filter_basins:
        basins = basins[basins["node_id"] == basins["meta_node_id"]]

    # creating depth profile-lines
    if create_depth_profile_lines and cross_sections is not None:
        cross_sections = depth.make_depth_profiles(cross_sections, **kw_make_depth_profile)

    # remove duplicate geometries
    if simplify_geometries:
        crossings = path_finder.simplify_geodata(crossings, tolerance=1e-2, col_in_use="in_use")
        hydro_objects = path_finder.simplify_geodata(hydro_objects)

    # get BGT-data
    geo_filter = shapely.MultiPolygon(basins.convex_hull.values).convex_hull
    if fn_bgt is None:
        bgt_data = bgt.download_bgt_water(geo_filter=geo_filter)
    else:
        fn_bgt = pathlib.Path(fn_bgt)
        bgt_data = bgt.get_water_surfaces(fn_bgt.parent, fn=fn_bgt.name, geo_filter=geo_filter, write=True)

    # patch network
    if patch_network:
        hydro_objects = path_finder.fully_connected_network(hydro_objects, buffer=patch_buffer)
        hydro_objects = path_finder.split_hydro_objects(hydro_objects, crossings, buffer=split_buffer)
    else:
        hydro_objects = (
            gpd.GeoDataFrame(geometry=[hydro_objects.union_all()], crs=hydro_objects.crs)
            .drop_duplicates(subset="geometry")
            .explode()
            .reset_index(drop=True)
        )

    # collectors
    main_route_idx: set[int] = set()
    point_collector: list[shapely.Point] = []
    line_collector: list[shapely.LineString] = []
    error_collector: list[int] = []

    # find main routes per basin
    for node_id, basin in zip(basins["node_id"].values, basins.geometry.values):
        # data selections
        subset_hydro_objects = hydro_objects[hydro_objects.intersects(basin)]
        subset_crossings = path_finder.select_crossings(
            basin, crossings, buffer=selection_buffer, internal=internal_crossings
        )

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
        if wd_intermediate_output is not None:
            points, lines = momepy.nx_to_gdf(graph)
            point_collector.append(points)
            line_collector.append(lines)

    # overview of erroneous basins
    if error_collector:
        LOG.critical(f"No hydro-objects found for the following basins ({len(error_collector)}): {error_collector}")

    # concatenate basin-groups of point- and line-data
    if wd_intermediate_output is not None:
        points = pd.concat(point_collector, axis=0)
        lines = pd.concat(line_collector, axis=0)
        points.to_file(wd_intermediate_output / _fn_graph, layer="points")
        lines.to_file(wd_intermediate_output / _fn_graph, layer="lines")

    # label hydro-objects
    hydro_objects["main-route"] = hydro_objects.index.isin(main_route_idx)
    if wd_intermediate_output is not None:
        hydro_objects[hydro_objects["main-route"]].to_file(wd_intermediate_output / _fn_int_output, layer="main-route")
        if internal_crossings:
            _temp = shapely.MultiPolygon(basins.explode().geometry.values).buffer(selection_buffer)
        else:
            _temp = shapely.MultiPolygon(basins.explode().exterior.buffer(selection_buffer))
        crossings[crossings.intersects(_temp)].to_file(wd_intermediate_output / _fn_int_output, layer="endpoints")

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
                f"NaN-values present in profile-table (flowing/'doorgaand'):\n{flowing_profiles[flowing_profiles.isna()]}"
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
