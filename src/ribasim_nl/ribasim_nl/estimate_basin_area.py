import geopandas as gpd
from shapely.geometry import shape

from ribasim_nl import CloudStorage, Model


def estimate_basin_area(
    model: Model, default_width: float = 1, max_distance: float = 100, clip_on_basin_area: bool = False
) -> dict[int, shape]:
    """Estimate basin area using BGT waterdeel

    Args:
        model (Model): Ribasim
        default_width (float, optional): Default width of Basin Links. Defaults to 1.
        max_distance (float, optional): Max distance (left/right-sided) from Basin Links. Defaults to 100.
        clip_on_basin_area (bool, optional): Option to clip result on existing basin area. Defaults to False.

    Returns
    -------
        dict[str, shape]: Dictionary with basin polygon per Basin node_id
    """
    cloud = CloudStorage()

    geoms = {}

    waterdelen_gdf = gpd.read_file(
        cloud.joinpath("Basisgegevens", "BGT", "bgt_waterdeel.gpkg"), bbox=tuple(model.basin.area.df.total_bounds)
    )
    waterdelen_gdf = waterdelen_gdf[waterdelen_gdf["eindRegistratie"].isna()]

    basin_area = model.basin.area.df.set_index("node_id")["geometry"]

    for row in model.basin.node.df.itertuples():
        if row.geometry.within(basin_area[row.Index]):
            # get all lines as a MultiLineString
            basin_lines = model.link.df.loc[
                (model.link.df.from_node_id == row.Index) | (model.link.df.to_node_id == row.Index)
            ].geometry.union_all()

            # Basin Clip Area
            basin_clip_area_poly = basin_lines.buffer(max_distance, cap_style="square")

            # Initial basin_area_poly
            basin_area_poly = basin_lines.buffer(default_width / 2, cap_style="square")

            # Select waterdelen: spatial query
            waterdelen_select_gdf = waterdelen_gdf.iloc[
                waterdelen_gdf.sindex.query(basin_area_poly, predicate="intersects")
            ]

            # Keep intersecting basin_lines only if found any, otherwise we'll keep rough selection
            mask = waterdelen_select_gdf.intersects(basin_lines)
            if mask.any():
                waterdelen_select_gdf = waterdelen_select_gdf[mask]

            # Keep if intersecting clip
            mask = waterdelen_select_gdf.within(basin_clip_area_poly)
            if mask.any():
                waterdelen_select_gdf = waterdelen_select_gdf[mask]
                clip_result = True
            else:
                clip_result = False

            # construct polygon
            basin_area_poly = basin_area_poly.union(waterdelen_select_gdf.union_all())
            if clip_result:
                basin_area_poly = basin_area_poly.intersection(basin_clip_area_poly)

            # clip on basin.Area (if original area is correct)
            if clip_on_basin_area:
                basin_area_poly = basin_area_poly.intersection(basin_area[row.Index])

            geoms[row.Index] = basin_area_poly

    return geoms
