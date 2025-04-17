import geopandas as gpd
import numpy as np
import pandas as pd
import ribasim
import shapely


def _transpose_basin_definition_polygons(
    basin_definition_in: gpd.GeoDataFrame, basin_definition_out: gpd.GeoDataFrame
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Retruns basin_difinition_out with index of basin_definition_in that intersect the basin_definition_out polygons

    Args:
        basin_definition_in (gpd.GeoDataFrame): Basin definition with (multi) polygons
        basin_definition_out (gpd.GeoDataFrame): Basin definition with (multi) polygons

    Returns
    -------
        tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]: Basin definition with new index, Basin definition with
        polygons without any intersection
    """
    tree = shapely.STRtree(basin_definition_out["geometry"])
    index_in, index_out = tree.query(basin_definition_in.representative_point(), predicate="intersects")
    index_in = basin_definition_in.index[index_in]
    index_out = basin_definition_out.index[index_out]
    index_undifined = basin_definition_out.index[~np.isin(basin_definition_out.index, index_out)]
    basin_definition_undifined = basin_definition_out.loc[index_undifined]
    basin_definition_out = basin_definition_out.loc[index_out]
    return basin_definition_out.set_index([index_in]), basin_definition_undifined


def _fill_basin_definition_from_points(basin_definition: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Retuns basin_definition filled with index of basins within polygon definition

    Args:
        basin_definition (gpd.GeoDataFrame): Basin definition with (multi) polygons
        nodes (gpd.GeoDataFrame): Ribasim Basin nodes

    Returns
    -------
        gpd.GeoDataFrame: basin_definition with index from underlying Ribasim Basins
    """
    tree = shapely.STRtree(basin_definition["geometry"])
    (
        index_nodes,
        index_basin_definition,
    ) = tree.query(nodes["geometry"], predicate="within")  #'overlaps', 'within'
    index_basin_definition = basin_definition.index[index_basin_definition]
    index_nodes = nodes.index[index_nodes]
    basin_definition = basin_definition.loc[index_basin_definition]
    return basin_definition.set_index(index_nodes)


def _split_basin_definition(
    basin_definition: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Splits basin definition based on 'meta_categorie' in Ribasim Basin nodes

    Args:
        basin_definition (gpd.GeoDataFrame): Basin definition with (multi) polygons
        nodes (gpd.GeoDataFrame): Ribasim Basin nodes

    Returns
    -------
        tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]: Basin definition with (multi) polygons for primary ans secondary Basins
    """
    secondary_nodes = nodes[nodes["meta_categorie"] == "bergend"]
    primary_nodes = nodes[nodes["meta_categorie"] != "bergend"]
    basin_definition = basin_definition.set_index("node_id", drop=True)
    secondary_mask = np.isin(secondary_nodes["node_id"], basin_definition.index)
    primary_mask = np.isin(primary_nodes["node_id"], basin_definition.index)
    if not secondary_mask.all():
        popped = secondary_nodes["node_id"][~secondary_mask]
        f"poped following secondary nodes: {popped}"
    if not primary_mask.all():
        popped = primary_nodes["node_id"][~primary_mask]
        f"poped following primary nodes: {popped}"
    return basin_definition.loc[primary_nodes["node_id"][primary_mask]], basin_definition.loc[
        secondary_nodes["node_id"][secondary_mask]
    ]


def split_basin_definitions(
    basin_definition: gpd.GeoDataFrame, ribasim_model: ribasim.Model
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    nodes = gpd.GeoDataFrame(
        {
            "node_id": ribasim_model.basin.node.df.index,
            "geometry": ribasim_model.basin.node.df["geometry"],
            "meta_categorie": ribasim_model.basin.node.df["meta_categorie"],
        }
    )
    # split based on meta_label in Ribasim model definition
    basin_definition_primair, basin_definition_secondair = _split_basin_definition(basin_definition, nodes)
    # transpose primairy basins to secondary basin definition to get rid of the narrow polygons
    basin_definition_primair_polygon, basin_definition_undifined = _transpose_basin_definition_polygons(
        basin_definition_primair, basin_definition_secondair
    )
    # fill empty basins based on pip for secondary nodes
    basin_definition_primair_points = _fill_basin_definition_from_points(
        basin_definition_undifined, nodes[nodes["meta_categorie"] != "bergend"]
    )
    basin_definition_primair = pd.concat([basin_definition_primair_polygon, basin_definition_primair_points])
    return basin_definition_primair.reset_index(), basin_definition_secondair.reset_index()
